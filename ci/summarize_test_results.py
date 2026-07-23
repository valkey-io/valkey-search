#!/usr/bin/env python3
# generated with Claude Code
"""Render CTest's JUnit XML output into the GitHub Actions job summary.

CTest's own JUnit writer (`ctest --output-junit`, wired up in run_unit_tests()
in build.sh) only puts a generic "Failed" in <failure message="...">; the
actual diagnostic output (stdout/stderr captured by ctest) lives in each
testcase's <system-out>. This script surfaces that text directly in the
workflow run summary so a failure's cause is visible without downloading the
test-results artifact or scrolling the raw docker/ctest log.
"""
import os
import sys
import xml.etree.ElementTree as ET

MAX_OUTPUT_CHARS = 20000


def iter_testsuites(root):
    if root.tag == "testsuites":
        return list(root.findall("testsuite"))
    if root.tag == "testsuite":
        return [root]
    return []


def truncate(text, limit=MAX_OUTPUT_CHARS):
    text = text.strip("\n")
    if len(text) <= limit:
        return text
    return f"... [truncated, showing last {limit} of {len(text)} chars] ...\n" + text[-limit:]


def main(argv):
    if len(argv) < 2:
        print("usage: summarize_test_results.py <junit.xml> [more.xml ...]", file=sys.stderr)
        return 1

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    out = open(summary_path, "a") if summary_path else sys.stdout

    total = passed = failed = skipped = 0
    failures = []  # (suite_name, test_name, message, output)

    for path in argv[1:]:
        if not os.path.isfile(path):
            print(f"::warning::No test results found at {path} (build likely failed before tests ran)", file=sys.stderr)
            continue
        try:
            root = ET.parse(path).getroot()
        except ET.ParseError as e:
            print(f"::warning::Could not parse {path}: {e}", file=sys.stderr)
            continue

        for suite in iter_testsuites(root):
            suite_name = suite.get("name", os.path.basename(path))
            for case in suite.findall("testcase"):
                total += 1
                name = case.get("name", "?")
                failure = case.find("failure")
                error = case.find("error")
                skip = case.find("skipped")
                if skip is not None:
                    skipped += 1
                elif failure is not None or error is not None:
                    failed += 1
                    node = failure if failure is not None else error
                    message = node.get("message", "")
                    sysout = case.find("system-out")
                    syserr = case.find("system-err")
                    output_parts = []
                    if sysout is not None and sysout.text and sysout.text.strip():
                        output_parts.append(sysout.text)
                    if syserr is not None and syserr.text and syserr.text.strip():
                        output_parts.append(syserr.text)
                    output = "\n".join(output_parts)
                    failures.append((suite_name, name, message, output))
                else:
                    passed += 1

    if total == 0:
        print("### Test Results\n\n_No test results found — the build likely failed before tests ran._", file=out)
        return 0

    status_emoji = "✅" if failed == 0 else "❌"
    print(f"### {status_emoji} Test Results: {passed}/{total} passed"
          + (f", {failed} failed" if failed else "")
          + (f", {skipped} skipped" if skipped else ""), file=out)

    if failures:
        print(file=out)
        print("| Suite | Test | Message |", file=out)
        print("|---|---|---|", file=out)
        for suite_name, name, message, _ in failures:
            msg = (message or "").replace("|", "\\|").replace("\n", " ")
            print(f"| {suite_name} | `{name}` | {msg} |", file=out)

        for suite_name, name, message, output in failures:
            print(file=out)
            print(f"<details><summary>❌ {suite_name} / <code>{name}</code></summary>", file=out)
            print(file=out)
            if output.strip():
                print("```", file=out)
                print(truncate(output), file=out)
                print("```", file=out)
            else:
                print("_No captured output._", file=out)
            print("</details>", file=out)

    if summary_path:
        out.close()

    # Test pass/fail already gates the job via the separate "Check Test
    # Results" step; this step is purely a reporting step and should not
    # itself fail the build.
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
