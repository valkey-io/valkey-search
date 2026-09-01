// Living Triage Board — a single GitHub Issue that behaves like an app.
//
// Every ~5 minutes an Action runs this. It:
//   1. Reads all open PRs of the target repo (GraphQL) and classifies each
//      reviewer as auto-assigned (from the auto-assign bot comment) or manual,
//      first-pass or maintainer, with their latest review status.
//   2. Reads the dashboard issue's *comments*, which are one-line commands that
//      anyone (no repo write access needed) can post: /priority, /note, /stage,
//      /update. It applies them to the board state, then DELETES each processed
//      command comment so the issue never accrues a pile of comments (that was
//      the "page gets slow" risk).
//   3. Persists canonical human state in a hidden JSON block inside the issue
//      body, so it survives every regeneration. Humans express *intent* via
//      commands; the bot owns the *record*.
//   4. Rewrites the issue body: live shields.io badges, a Mermaid chart,
//      priority-grouped tables, and a collapsible per-reviewer "your queue".
//
// Invoked from a github-script step:
//   const build = require('./.github/scripts/build-issue-dashboard.js');
//   await build({ github, context, core });
//
// Env:
//   DASHBOARD_ISSUE  required — issue number to own (in the repo the Action runs in).
//   TARGET_REPO      optional "owner/name" — read PRs from here instead of this repo.
//   POOLS_PATH       optional — path to reviewer-pools.json (default .github/reviewer-pools.json).

const fs = require('fs');

// One-time priority seed, transcribed from Allen's triage doc. Only used for a
// PR the board has never seen before; once a PR has state its stored value wins
// (including a human clearing it), so the doc is never re-read.
const SEED_PRIORITY = {
  984: 1, 985: 1, 997: 1, 1001: 1, 1083: 1, 1084: 1, 1090: 1, 1217: 1, 1263: 1,
  1287: 1, 1321: 1, 1292: 1, 1301: 1, 1302: 1, 1279: 1, 924: 1, 1068: 1, 1075: 1,
  1196: 1, 1204: 1, 1271: 1, 1273: 1, 1277: 1, 1295: 1, 1304: 1,
  1184: 2, 567: 2, 759: 2, 800: 2, 923: 2, 1023: 2, 1193: 2, 1218: 2, 1239: 2,
  1296: 2, 1103: 2, 1297: 2,
  1278: 3, 762: 3, 864: 3, 908: 3, 922: 3, 936: 3, 946: 3, 947: 3, 948: 3, 949: 3,
  967: 3, 983: 3, 1000: 3, 1021: 3, 1116: 3, 1129: 3, 1139: 3, 1225: 3, 1234: 3,
  1236: 3, 1268: 3, 1294: 3, 1035: 3, 1088: 3,
  795: 4, 859: 4, 988: 4, 1005: 4, 1113: 4, 1016: 4, 1049: 4, 1256: 4, 1300: 4,
  1095: 4, 1124: 4, 1298: 4, 894: 4,
};

const STALE_DAYS = 7;

// ── GitHub data gathering ────────────────────────────────────────────────────
async function gatherPRs(github, owner, repo) {
  const query = `
    query($owner:String!, $repo:String!, $cursor:String) {
      repository(owner:$owner, name:$repo) {
        pullRequests(states:OPEN, first:25, after:$cursor, orderBy:{field:CREATED_AT, direction:ASC}) {
          pageInfo { hasNextPage endCursor }
          nodes {
            number title url isDraft createdAt updatedAt
            mergeable
            author { login }
            labels(first:30) { nodes { name } }
            reviewRequests(first:30) { nodes { requestedReviewer { __typename ... on User { login } } } }
            reviews(first:100) { nodes { author { login } state submittedAt } }
            comments(first:100) { nodes { author { login } body } }
            commits(last:1) { nodes { commit { statusCheckRollup { state } } } }
          }
        }
      }
    }`;
  const nodes = [];
  let cursor = null;
  do {
    const data = await github.graphql(query, { owner, repo, cursor });
    const conn = data.repository.pullRequests;
    nodes.push(...conn.nodes);
    cursor = conn.pageInfo.hasNextPage ? conn.pageInfo.endCursor : null;
  } while (cursor);
  return nodes;
}

const FP_RE = /\*\*First Pass Reviewer:\s*@([A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)\*\*/;
const MT_RE = /\*\*Maintainer Reviewer:\s*@([A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)\*\*/;

function mapState(state) {
  switch (state) {
    case 'APPROVED': return 'Approved';
    case 'CHANGES_REQUESTED': return 'Changes req.';
    case 'COMMENTED': return 'Commented';
    default: return 'Pending';
  }
}
const statusEmoji = s =>
  s === 'Approved' ? '✅' : s === 'Changes req.' ? '🔴' : s === 'Commented' ? '💬' : '⏳';

// AI / automated reviewers to exclude — they aren't people who owe a review.
// GitHub Apps post as "<name>[bot]", but some (e.g. greptile-apps) show up as a
// plain login, so also match known vendor stems as substrings.
const BOT_STEMS = ['coderabbit', 'greptile', 'sonarcloud', 'codecov', 'dependabot', 'github-actions'];
const isBot = login => {
  if (!login) return true;
  const l = String(login).toLowerCase();
  return /\[bot\]$/.test(l) || BOT_STEMS.some(s => l.includes(s));
};

// Turn a raw GraphQL PR node into the shape the renderer consumes.
function shape(node, firstPassPool, maintainerPool) {
  const author = node.author && node.author.login ? node.author.login : '(ghost)';

  let autoFP = null, autoMT = null;
  for (const c of (node.comments.nodes || [])) {
    const body = c.body || '';
    const fp = body.match(FP_RE);
    const mt = body.match(MT_RE);
    if (fp) autoFP = fp[1];
    if (mt) autoMT = mt[1];
  }

  const requested = (node.reviewRequests.nodes || [])
    .map(r => r.requestedReviewer && r.requestedReviewer.login)
    .filter(l => l && !isBot(l));

  const latest = {};
  for (const r of (node.reviews.nodes || [])) {
    const login = r.author && r.author.login;
    if (!login || isBot(login)) continue;
    if (!latest[login] || (r.submittedAt || '') >= (latest[login].submittedAt || '')) latest[login] = r;
  }

  const universe = new Set([...requested, ...Object.keys(latest), autoFP, autoMT]
    .filter(Boolean).filter(l => l !== author));

  const firstPass = [], maintainers = [], other = [];
  for (const login of universe) {
    const via = (login === autoFP || login === autoMT) ? 'auto' : 'manual';
    const status = latest[login] ? mapState(latest[login].state) : 'Pending';
    const entry = { login, via, status };
    if (login === autoFP || (via === 'manual' && firstPassPool.has(login))) firstPass.push(entry);
    else if (login === autoMT || (via === 'manual' && maintainerPool.has(login))) maintainers.push(entry);
    else other.push(entry);
  }

  const hasAuto = !!(autoFP || autoMT);
  const hasManual = [...universe].some(l => l !== autoFP && l !== autoMT);
  let via = '—';
  if (hasAuto && hasManual) via = 'Mixed';
  else if (hasAuto) via = 'Auto';
  else if (hasManual) via = 'Manual';

  const daysIdle = Math.floor((Date.now() - new Date(node.updatedAt).getTime()) / 86400000);

  const rollup = node.commits && node.commits.nodes && node.commits.nodes[0]
    && node.commits.nodes[0].commit && node.commits.nodes[0].commit.statusCheckRollup;
  const ci = rollup ? rollup.state : null;      // SUCCESS / FAILURE / ERROR / PENDING / EXPECTED / null
  const mergeable = node.mergeable || 'UNKNOWN'; // MERGEABLE / CONFLICTING / UNKNOWN

  return {
    number: node.number, title: node.title, url: node.url, isDraft: node.isDraft,
    author, firstPass, maintainers, other, via,
    reviewerCount: universe.size, daysIdle, stale: daysIdle >= STALE_DAYS,
    ci, mergeable,
  };
}

// Compact per-PR health: CI checks + merge conflicts. "—" when GitHub hasn't
// reported yet (e.g. mergeable UNKNOWN right after a push).
function ciCell(pr) {
  const parts = [];
  if (pr.ci === 'SUCCESS') parts.push('✅');
  else if (pr.ci === 'FAILURE' || pr.ci === 'ERROR') parts.push('❌');
  else if (pr.ci === 'PENDING' || pr.ci === 'EXPECTED') parts.push('🟡');
  if (pr.mergeable === 'CONFLICTING') parts.push('⚠️');
  return parts.join(' ') || '—';
}

// ── State: hidden JSON block in the issue body ───────────────────────────────
const STATE_OPEN = '<!--BOARD_STATE';
const STATE_CLOSE = 'BOARD_STATE-->';

function loadState(body) {
  const empty = { notes: {}, priority: {}, stage: {}, reviewed: {}, claims: {}, log: [] };
  if (!body) return empty;
  const i = body.indexOf(STATE_OPEN);
  const j = body.indexOf(STATE_CLOSE);
  if (i === -1 || j === -1 || j < i) return empty;
  try {
    const json = body.slice(i + STATE_OPEN.length, j).trim();
    const p = JSON.parse(json);
    // reviewed: { prNum: { login: true } }   claims: { prNum: [login, …] }
    // Strip any bare "@login" left in historical log lines (older runs wrote
    // them); a re-rendered "@login" would keep pinging that person every run.
    const log = (p.log || []).map(l => String(l).replace(/@([A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)/g, '$1'));
    return {
      notes: p.notes || {}, priority: p.priority || {}, stage: p.stage || {},
      reviewed: p.reviewed || {}, claims: p.claims || {}, log,
    };
  } catch (e) {
    return empty;
  }
}

// Manual stage override aliases → canonical stage key (see STAGES below).
const STAGE_ALIAS = {
  review: 'review', ready: 'review',
  changes: 'changes', 'changes-requested': 'changes',
  maintainer: 'maintainer', 'maintainer-review': 'maintainer', maint: 'maintainer',
  approved: 'approved', approve: 'approved',
};

// Parse one comment body for commands. Returns a list of {cmd, pr, arg, author}.
// Supported (one per line):
//   General (per-PR):   /priority 1234 P2 · /note 1234 text · /stage 1234 maintainer
//                       /needs-review 1234 · /update
//   Personal (per-you): /claim 1234 · /unclaim 1234 · /reviewed 1234 · /unreviewed 1234
const CMD_RE = /^\s*\/(priority|note|stage|claim|unclaim|reviewed|unreviewed|needs-review)\s+#?(\d+)\b\s*(.*)$/i;
const NOARG_RE = /^\s*\/(update|refresh)\b/i;

function parseCommands(commentBody, author) {
  const out = [];
  for (const line of String(commentBody || '').split('\n')) {
    const m = line.match(CMD_RE);
    if (m) { out.push({ cmd: m[1].toLowerCase(), pr: Number(m[2]), arg: m[3].trim(), author }); continue; }
    if (NOARG_RE.test(line)) out.push({ cmd: 'update', pr: null, arg: '', author });
  }
  return out;
}

// True if a comment was *trying* to be a command, so we can nudge the author
// when it fails to parse (e.g. "/priority" with no PR number).
const CMD_WORD_RE = /^\s*\/(priority|note|stage|claim|unclaim|reviewed|unreviewed|needs-review|update|refresh)\b/im;
function looksLikeCommand(body) { return CMD_WORD_RE.test(String(body || '')); }
const HINT_MARKER = '<!--HINT-->';

// Render a person WITHOUT pinging them. A bare "@login" in the issue body (or a
// hint comment) fires a GitHub notification every single run — spammy for a page
// that rewrites itself every ~5 min. A profile link shows the same name, stays
// clickable, and never notifies.
const mention = login => `[${login}](https://github.com/${login})`;

// ctx (optional) = { prByNum } — needed by /needs-review to find a PR's maintainers.
function applyCommand(state, c, now, ctx) {
  const n = c.pr;
  let msg = null;
  switch (c.cmd) {
    case 'priority': {
      const p = (c.arg.match(/[1-4]/) || [])[0];
      if (p) { state.priority[n] = Number(p); msg = `${mention(c.author)} set #${n} → P${p}`; }
      else { state.priority[n] = null; msg = `${mention(c.author)} cleared priority on #${n}`; }
      break;
    }
    case 'note':
      if (c.arg) { state.notes[n] = c.arg; msg = `${mention(c.author)} noted #${n}: ${c.arg}`; }
      else { delete state.notes[n]; msg = `${mention(c.author)} cleared note on #${n}`; }
      break;
    case 'stage': {
      const a = c.arg.toLowerCase().trim();
      if (!a || a === 'auto' || a === 'clear') { delete state.stage[n]; msg = `${mention(c.author)} reset #${n} stage to auto`; }
      else if (STAGE_ALIAS[a]) { state.stage[n] = STAGE_ALIAS[a]; msg = `${mention(c.author)} set #${n} → ${STAGES[STAGE_ALIAS[a]].label}`; }
      // unknown stage word → no change, no log (avoids noise).
      break;
    }
    case 'claim': {
      const arr = state.claims[n] || (state.claims[n] = []);
      if (!arr.includes(c.author)) arr.push(c.author);
      msg = `${mention(c.author)} claimed #${n} 🙋`; break;
    }
    case 'unclaim': {
      state.claims[n] = (state.claims[n] || []).filter(l => l !== c.author);
      if (!state.claims[n].length) delete state.claims[n];
      msg = `${mention(c.author)} unclaimed #${n}`; break;
    }
    case 'reviewed': {
      (state.reviewed[n] || (state.reviewed[n] = {}))[c.author] = true;
      msg = `${mention(c.author)} marked #${n} reviewed ✅`; break;
    }
    case 'unreviewed': {
      if (state.reviewed[n]) { delete state.reviewed[n][c.author]; if (!Object.keys(state.reviewed[n]).length) delete state.reviewed[n]; }
      msg = `${mention(c.author)} un-marked their review of #${n}`; break;
    }
    case 'needs-review': {
      // Author re-requests review: clear the Reviewed flag for the PR's
      // maintainers only; first-pass / other reviewers are left as-is.
      const pr = ctx && ctx.prByNum && ctx.prByNum[n];
      const maints = pr ? pr.maintainers.map(e => e.login) : [];
      if (state.reviewed[n]) {
        for (const login of maints) delete state.reviewed[n][login];
        if (!Object.keys(state.reviewed[n]).length) delete state.reviewed[n];
      }
      msg = `${mention(c.author)} requested re-review on #${n} — maintainer review reset 🔁`; break;
    }
    case 'update':
      msg = `${mention(c.author)} refreshed the board 🔄`; break; // rebuild happens regardless
  }
  if (msg) {
    state.log.unshift(`\`${now}\` — ${msg}`);
    state.log = state.log.slice(0, 12);
  }
  return msg;
}

// ── Rendering ────────────────────────────────────────────────────────────────
const esc = s => String(s == null ? '' : s).replace(/\|/g, '\\|').replace(/\r?\n/g, ' ');
const prLink = pr => `[#${pr.number}](${pr.url})`;
const badge = (label, msg, color) => {
  const enc = t => encodeURIComponent(String(t)).replace(/-/g, '--').replace(/_/g, '__').replace(/%20/g, '_');
  return `![${label}](https://img.shields.io/badge/${enc(label)}-${enc(msg)}-${color})`;
};

function reviewerCell(list) {
  if (!list.length) return '—';
  return list.map(r => `${mention(r.login)} ${r.via === 'auto' ? '🤖' : '✋'} ${statusEmoji(r.status)}`).join('<br>');
}

function priorityOf(state, n) {
  if (state.priority[n] != null) return state.priority[n];
  return null;
}
// Sort/group bucket: unset priority sorts with P4.
const bucket = p => (p == null || p === 4) ? 4 : p;

// ── PR lifecycle stage ───────────────────────────────────────────────────────
// The single "where is this PR" signal, derived from GitHub reviews. A human
// can override it with /stage; the override wins until reset to auto.
const STAGES = {
  draft:      { label: 'Draft',                emoji: '📝' },
  review:     { label: 'Ready to review',      emoji: '🆕' },
  changes:    { label: 'Changes requested',    emoji: '🔴' },
  maintainer: { label: 'Ready for maintainer', emoji: '🧑‍⚖️' },
  approved:   { label: 'Approved',             emoji: '✅' },
};
function autoStage(pr) {
  if (pr.isDraft) return 'draft';
  const all = [...pr.firstPass, ...pr.maintainers];
  if (all.some(e => e.status === 'Changes req.')) return 'changes';
  if (pr.maintainers.some(e => e.status === 'Approved')) return 'approved';
  if (pr.firstPass.some(e => e.status === 'Approved')) return 'maintainer';
  return 'review';
}
function stageKey(pr, state) {
  const ov = state.stage && state.stage[pr.number];
  return (ov && STAGES[ov]) ? ov : autoStage(pr);
}
function stageCell(pr, state) {
  const k = stageKey(pr, state);
  const overridden = !!(state.stage && state.stage[pr.number] && STAGES[state.stage[pr.number]]);
  return `${STAGES[k].emoji} ${STAGES[k].label}${overridden ? ' ✎' : ''}`;
}

function renderBody(prs, state, pools, now, targetLabel) {
  const L = [];
  const total = prs.length;
  const byPri = { 1: 0, 2: 0, 3: 0, 4: 0, none: 0 };
  let needsFP = 0, stale = 0, approved = 0, readyMaint = 0;
  for (const pr of prs) {
    const p = priorityOf(state, pr.number);
    byPri[p == null ? 'none' : p]++;
    if (!pr.firstPass.length && !pr.isDraft) needsFP++;
    if (pr.stale) stale++;
    const sk = stageKey(pr, state);
    if (sk === 'approved') approved++;
    else if (sk === 'maintainer') readyMaint++;
  }

  // Build login → their PRs once, keyed by PR so assignment (🤖 auto / ✋ manual)
  // and a personal 🙋 /claim collapse into a single row per PR. Claims come from
  // state, so they persist across every auto-regeneration.
  const prByNum = Object.fromEntries(prs.map(p => [p.number, p]));
  const reviewerPRs = new Map(); // login -> Map(prNum -> {pr, auto, manual, claim, status})
  const touch = (login, pr) => {
    if (!reviewerPRs.has(login)) reviewerPRs.set(login, new Map());
    const m = reviewerPRs.get(login);
    if (!m.has(pr.number)) m.set(pr.number, { pr, auto: false, manual: false, claim: false, status: 'Pending' });
    return m.get(pr.number);
  };
  for (const pr of prs) {
    for (const e of [...pr.firstPass, ...pr.maintainers, ...pr.other]) {
      const row = touch(e.login, pr);
      if (e.via === 'auto') row.auto = true; else row.manual = true;
      row.status = e.status; // this person's latest GitHub review state on the PR
    }
  }
  for (const [n, logins] of Object.entries(state.claims || {})) {
    const pr = prByNum[n];
    if (!pr) continue;
    for (const login of logins) touch(login, pr).claim = true;
  }
  const reviewerOrder = [...pools.firstPass, ...pools.maintainers,
    ...[...reviewerPRs.keys()].filter(l => !pools.firstPass.includes(l) && !pools.maintainers.includes(l)).sort()];

  L.push(`# 🗂️ Reviewer Triage Board`);
  L.push('');
  L.push([
    badge('open PRs', total, 'blue'),
    badge('P1', byPri[1], 'red'),
    badge('P2', byPri[2], 'orange'),
    badge('P3', byPri[3], 'yellow'),
    badge('needs first-pass', needsFP, needsFP ? 'critical' : 'green'),
    badge('ready for maintainer', readyMaint, 'blueviolet'),
    badge('approved', approved, 'brightgreen'),
    badge(`stale >${STALE_DAYS}d`, stale, stale ? 'lightgrey' : 'green'),
  ].join(' '));
  L.push('');
  L.push(`_Auto-updated every ~5 min from **${targetLabel}** open PRs. Last run: **${now}**._`);
  L.push('');

  // Mermaid: PRs by priority (compact labels).
  L.push('```mermaid');
  L.push('pie showData title PRs by priority');
  L.push(`    "P1" : ${byPri[1]}`);
  L.push(`    "P2" : ${byPri[2]}`);
  L.push(`    "P3" : ${byPri[3]}`);
  L.push(`    "P4/none" : ${byPri[4] + byPri.none}`);
  L.push('```');
  L.push('');

  // How to edit (the whole point: no write access needed) — always visible.
  L.push('## ✍️ How to update this board');
  L.push('');
  L.push('Anyone can edit — **no repo write access needed**. Add a **comment** with one or more commands; the bot applies it, then deletes your comment so this page stays fast.');
  L.push('');
  L.push('**General commands** — act on a PR (the priority tables below):');
  L.push('');
  L.push('```');
  L.push('/priority 1234 P2      set priority (P1–P4)');
  L.push('/note 1234 some text   set a note (empty clears it)');
  L.push('/stage 1234 maintainer override stage: review | changes | maintainer | approved | auto');
  L.push('/needs-review 1234     re-request maintainer review (clears maintainers’ Reviewed)');
  L.push('/update                refresh the board from the latest PR data now');
  L.push('```');
  L.push('');
  L.push('**Personal commands** — act on you (your row in *Your queue*):');
  L.push('');
  L.push('```');
  L.push('/claim 1234        add the PR to your queue / put your name on it');
  L.push('/unclaim 1234      remove your claim');
  L.push('/reviewed 1234     mark reviewed (only if you did NOT approve on GitHub)');
  L.push('/unreviewed 1234   un-mark it');
  L.push('```');
  L.push('');
  L.push('> **Approving a PR on GitHub already marks it Reviewed ✅ for you — you do not need `/reviewed`.** '
    + 'And if you later withdraw or change that approval, it clears itself again. The board picks this up on '
    + 'the next ~5-min refresh, or immediately if anyone runs `/update`. `/reviewed` / `/unreviewed` are only for '
    + 'a review you did *without* a formal GitHub approval.');
  L.push('');

  // Column legend — one collapsible per column, each state on its own line so
  // there's no run-on sentence to parse.
  L.push('## ℹ️ Legend');
  L.push('');
  L.push('_Expand a column to see what each symbol means. AI reviewers (CodeRabbit, Greptile, …) are excluded from every count._');
  L.push('');

  const legend = (title, lines) => {
    L.push(`<details><summary><b>${title}</b></summary>`);
    L.push('');
    for (const line of lines) L.push(`- ${line}`);
    L.push('');
    L.push('</details>');
  };

  legend('Stage <sub>(priority tables)</sub>', [
    'Auto-computed from the PR\'s reviews; merged/closed PRs leave the board on their own.',
    '📝 **Draft** — not ready for review yet.',
    '🆕 **Ready to review** — open, no decisive review yet.',
    '🔴 **Changes requested** — a reviewer asked for changes.',
    '🧑‍⚖️ **Ready for maintainer** — a first-pass reviewer approved; needs a maintainer.',
    '✅ **Approved** — a maintainer approved.',
    '✎ — a human set the stage by hand with `/stage` (overrides the auto value).',
  ]);
  legend('First-pass / Maintainer / Other <sub>(priority tables)</sub>', [
    'Who is assigned, split by pool. **Other** = anyone assigned who isn\'t in the first-pass or maintainer pool.',
    'How they were added: 🤖 **auto** (auto-assign bot) · ✋ **manual** (requested by hand).',
    'Their latest GitHub review state, shown after the how-symbol:',
    '✅ approved · 🔴 changes requested · 💬 commented · ⏳ pending.',
  ]);
  legend('How <sub>(Your queue)</sub>', [
    '🤖 auto-assigned to you.',
    '✋ requested from you by hand.',
    '🙋 you `/claim`ed it (persists across updates).',
  ]);
  legend('Reviewed <sub>(Your queue)</sub>', [
    '✅ you approved the PR on GitHub **or** ran `/reviewed <#>` — either is enough.',
    '☐ not yet — this is what counts toward **awaiting you**.',
    'Approving on GitHub sets ✅ automatically (no `/reviewed` needed); withdrawing or changing that approval clears it again. This syncs on the next ~5-min refresh, or right away with `/update`.',
    '`/reviewed` / `/unreviewed` are manual overrides for a review done *without* a GitHub approval.',
    '`/needs-review <#>` clears the manual `/reviewed` flag for the PR\'s maintainers (a standing GitHub approval still shows ✅ until it\'s dismissed).',
  ]);
  legend('Checks <sub>(CI + merge health)</sub>', [
    '✅ passing.',
    '❌ failing.',
    '🟡 running.',
    '⚠️ merge conflict.',
    '— not reported yet.',
  ]);
  L.push('');

  // Your queue: expand your name to see your PRs, split by how you were added.
  // (In-issue heading anchors are unreliable, so this uses inline collapsibles
  // instead of jump links — expand right here.)
  L.push('## 🔎 Your queue');
  L.push('');
  L.push('_Expand your name to see your PRs. 🤖 = auto-assigned to you · ✋ = requested by hand · 🙋 = you `/claim`ed it. **Reviewed** ✅ = you approved it on GitHub **or** ran `/reviewed`. **Awaiting you** = the rest._');
  L.push('');
  for (const login of reviewerOrder) {
    const m = reviewerPRs.get(login);
    if (!m || !m.size) continue;
    const items = [...m.values()];
    // Reviewed if you approved on GitHub OR marked it by hand — approving in
    // GitHub is enough, no separate /reviewed needed.
    const reviewedBy = it => it.status === 'Approved' || !!(state.reviewed[it.pr.number] && state.reviewed[it.pr.number][login]);
    const awaiting = items.filter(it => !reviewedBy(it)).length;
    const autoN = items.filter(it => it.auto).length;
    const manualN = items.filter(it => it.manual).length;
    const claimN = items.filter(it => it.claim).length;
    items.sort((a, b) => bucket(priorityOf(state, a.pr.number)) - bucket(priorityOf(state, b.pr.number)) || a.pr.number - b.pr.number);
    const bits = [`${awaiting} awaiting you`, `🤖 ${autoN} auto`, `✋ ${manualN} manual`];
    if (claimN) bits.push(`🙋 ${claimN} claimed`);
    L.push(`<details><summary><b>${mention(login)}</b> — ${bits.join(' · ')}</summary>`);
    L.push('');
    L.push(`[🔗 Open my review requests on GitHub →](https://github.com/${targetLabel}/pulls?q=${encodeURIComponent(`is:open is:pr review-requested:${login}`)})`);
    L.push('');
    L.push('| PR | Title | How | Priority | Checks | Reviewed |');
    L.push('|----|-------|:--:|:--------:|:------:|:--------:|');
    for (const it of items) {
      const p = priorityOf(state, it.pr.number);
      const how = [it.auto ? '🤖' : '', it.manual ? '✋' : '', it.claim ? '🙋' : ''].filter(Boolean).join('') || '—';
      L.push(`| ${prLink(it.pr)} | ${esc(it.pr.title)} | ${how} | ${p == null ? '—' : 'P' + p} | ${ciCell(it.pr)} | ${reviewedBy(it) ? '✅' : '☐'} |`);
    }
    L.push('');
    L.push('</details>');
  }
  L.push('');

  // Priority-grouped tables (overall triage view) — each collapsible. P1 is
  // open by default (blockers up front); the rest collapse to keep it short.
  const groups = [[1, 'P1 — launch blocker'], [2, 'P2'], [3, 'P3'], [4, 'P4 / unset']];
  for (const [g, title] of groups) {
    const rows = prs.filter(pr => bucket(priorityOf(state, pr.number)) === g)
      .sort((a, b) => a.number - b.number);
    if (!rows.length) continue;
    L.push(`<details${g === 1 ? ' open' : ''}><summary><b>${title}</b> · ${rows.length}</summary>`);
    L.push('');
    L.push('| PR | Title | Stage | Checks | Author | First-pass | Maintainer | Other | Notes |');
    L.push('|----|-------|:-----:|:------:|--------|-----------|-----------|-------|-------|');
    for (const pr of rows) {
      const n = pr.number;
      const note = state.notes[n] ? esc(state.notes[n]) : '—';
      const title2 = esc(pr.title) + (pr.stale ? ` ⏳${pr.daysIdle}d` : '');
      L.push(`| ${prLink(pr)} | ${title2} | ${stageCell(pr, state)} | ${ciCell(pr)} | ${mention(pr.author)} | ${reviewerCell(pr.firstPass)} | ${reviewerCell(pr.maintainers)} | ${reviewerCell(pr.other)} | ${note} |`);
    }
    L.push('');
    L.push('</details>');
    L.push('');
  }

  // Recent activity log.
  if (state.log && state.log.length) {
    L.push('<details><summary>🕓 <b>Recent activity</b></summary>');
    L.push('');
    for (const line of state.log) L.push(`- ${line}`);
    L.push('');
    L.push('</details>');
    L.push('');
  }

  L.push(`<sub>Priority: P1 launch blocker · P2–P3 nice to have · P4 likely not. Pools: ${pools.firstPass.length} first-pass, ${pools.maintainers.length} maintainers. Seeded once from the triage doc, maintained here via commands.</sub>`);
  L.push('');
  L.push(`${STATE_OPEN} ${JSON.stringify(state)} ${STATE_CLOSE}`);
  return L.join('\n');
}

// ── Orchestration ────────────────────────────────────────────────────────────
module.exports = async ({ github, context, core }) => {
  const { owner: hostOwner, repo: hostRepo } = context.repo; // where the issue lives + Action runs
  let readOwner = hostOwner, readRepo = hostRepo;
  if (process.env.TARGET_REPO && process.env.TARGET_REPO.includes('/')) {
    [readOwner, readRepo] = process.env.TARGET_REPO.split('/');
  }
  const issue_number = Number(process.env.DASHBOARD_ISSUE);
  if (!issue_number) throw new Error('DASHBOARD_ISSUE env var is required (the issue number to own).');
  const POOLS_PATH = process.env.POOLS_PATH || '.github/reviewer-pools.json';
  const now = new Date().toISOString().replace('T', ' ').slice(0, 16) + ' UTC';

  let pools = { firstPass: [], maintainers: [] };
  try {
    const p = JSON.parse(fs.readFileSync(POOLS_PATH, 'utf8'));
    pools.firstPass = (p.firstPass || []).filter(Boolean);
    pools.maintainers = (p.maintainers || []).filter(Boolean);
  } catch (e) { core.warning(`pools: ${e.message}`); }
  const firstPassPool = new Set(pools.firstPass);
  const maintainerPool = new Set(pools.maintainers);

  // 1. PR data.
  const nodes = await gatherPRs(github, readOwner, readRepo);
  const prs = nodes.map(n => shape(n, firstPassPool, maintainerPool));
  core.info(`Gathered ${prs.length} open PRs from ${readOwner}/${readRepo}.`);

  // 2. Load prior state from the issue body.
  const issue = await github.rest.issues.get({ owner: hostOwner, repo: hostRepo, issue_number });
  const state = loadState(issue.data.body || '');

  // Seed priority for PRs the board has never recorded.
  for (const pr of prs) {
    if (!(pr.number in state.priority) && SEED_PRIORITY[pr.number] != null) {
      state.priority[pr.number] = SEED_PRIORITY[pr.number];
    }
  }

  // 3. Process command comments, then delete them so the issue stays fast.
  //
  // Deleting leaves a "X deleted a comment" timeline event, but GitHub auto-
  // folds runs of those into "N hidden items", so the comment list itself
  // stays truly empty. We skip our own log comments and only clean up our own
  // transient hint replies.
  const ctx = { prByNum: Object.fromEntries(prs.map(p => [p.number, p])) };
  const comments = await github.paginate(github.rest.issues.listComments,
    { owner: hostOwner, repo: hostRepo, issue_number, per_page: 100 });
  const del = async id => {
    try { await github.rest.issues.deleteComment({ owner: hostOwner, repo: hostRepo, comment_id: id }); return true; }
    catch (e) { core.warning(`Could not delete comment ${id}: ${e.message}`); return false; }
  };
  let processed = 0;
  for (const c of comments) {
    const author = c.user && c.user.login;
    const body = c.body || '';
    // Clean up our own prior hint replies (transient nudges — never state).
    if (isBot(author)) {
      if (body.includes(HINT_MARKER)) await del(c.id);
      continue; // never touch our own log comments
    }
    const cmds = parseCommands(body, author);
    if (cmds.length) {
      for (const cmd of cmds) applyCommand(state, cmd, now, ctx);
      if (await del(c.id)) processed++;
    } else if (looksLikeCommand(body)) {
      // Tried to command but it didn't parse — most often a missing PR number.
      // Nudge, then remove both the attempt and (later) the hint.
      try {
        await github.rest.issues.createComment({ owner: hostOwner, repo: hostRepo, issue_number,
          body: `${HINT_MARKER}\n${mention(author)} most commands need a PR number, e.g. \`/priority 1234 P2\`. `
            + `General: \`/priority <#> P1-P4\` · \`/note <#> text\` · \`/stage <#> …\` · \`/needs-review <#>\` · \`/update\`. `
            + `Personal: \`/claim <#>\` · \`/unclaim <#>\` · \`/reviewed <#>\` · \`/unreviewed <#>\`. `
            + `(This hint auto-deletes on the next run.)` });
      } catch (e) { core.warning(`Could not post hint: ${e.message}`); }
      await del(c.id);
    }
  }
  if (processed) core.info(`Applied and cleared ${processed} command comment(s).`);

  // Prune state for PRs no longer open (merged/closed drop off the board).
  const openNums = new Set(prs.map(p => p.number));
  for (const key of ['notes', 'priority', 'stage', 'reviewed', 'claims']) {
    for (const n of Object.keys(state[key])) {
      if (!openNums.has(Number(n))) delete state[key][n];
    }
  }

  // 4. Rewrite the issue body.
  const targetLabel = `${readOwner}/${readRepo}`;
  const body = renderBody(prs, state, pools, now, targetLabel);
  if ((issue.data.body || '') !== body) {
    await github.rest.issues.update({ owner: hostOwner, repo: hostRepo, issue_number, body });
    core.info('Board updated.');
  } else {
    core.info('No change.');
  }
};

// Exposed for offline testing.
module.exports._internal = {
  shape, loadState, parseCommands, looksLikeCommand, applyCommand, renderBody,
  autoStage, stageKey, stageCell, SEED_PRIORITY,
};
