# Skills

Reusable AI-assisted workflows for valkey-search development.

## Usage

Skills in this directory can be invoked by AI coding assistants (Claude Code, GitHub Copilot, etc.) to perform common development tasks. Each skill is a directory containing a `SKILL.md` file.

## Adding a New Skill

Create a directory containing `SKILL.md` with:

1. YAML frontmatter containing `name` and `description`
2. A descriptive title as an H1 heading
3. A brief description of what the skill does
4. A clear procedure for following the workflow

## Available Skills

| Skill | Description |
|-------|-------------|
| [stale-prs](stale-prs/SKILL.md) | Find open PRs that haven't been updated in 30+ days |
| [run-integration-tests](run-integration-tests/SKILL.md) | Run and diagnose the C++ and Python integration harnesses, including standalone and cluster modes |

## TODO

- How to unit test
- Build + load module on server
