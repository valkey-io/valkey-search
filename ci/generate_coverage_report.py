#!/usr/bin/env python3
"""Generates code coverage reports using gcov JSON data."""

import argparse
import glob
import gzip
import json
import os
import subprocess
import sys
import tempfile


def find_gcno_files(build_dir, subdirs):
    gcno_files = []
    for subdir in subdirs:
        target_dir = os.path.join(build_dir, subdir)
        if os.path.exists(target_dir):
            gcno_files.extend(glob.glob(f"{target_dir}/**/*.gcno", recursive=True))
    return sorted(gcno_files)


def normalize_rel_path(path, root_dir, container_prefixes):
    if path.startswith(root_dir):
        return os.path.relpath(path, root_dir)
    for prefix in container_prefixes:
        if path.startswith(prefix):
            return os.path.relpath(path, prefix)
    if path.startswith("/"):
        return path.lstrip("/")
    return os.path.relpath(path, root_dir)


def generate_gcov_json(build_dir, root_dir, subdirs, include_prefixes, exclude_prefixes, container_prefixes):
    gcno_files = find_gcno_files(build_dir, subdirs)
    if not gcno_files:
        return {}

    temp_dir = tempfile.mkdtemp(prefix="cov_")
    line_counts_by_file = {}

    try:
        for gcno in gcno_files:
            base_name = os.path.basename(gcno)
            if base_name.endswith(".cc.gcno"):
                src_base = base_name[:-8] + ".cc"
            elif base_name.endswith(".c.gcno"):
                src_base = base_name[:-7] + ".c"
            else:
                continue

            for stale_json in glob.glob(f"{temp_dir}/*.gcov.json.gz"):
                os.remove(stale_json)

            cmd = ["gcov", "-j", "-o", gcno, src_base]
            proc = subprocess.run(
                cmd,
                cwd=temp_dir,
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                print(
                    f"Error: gcov failed (exit {proc.returncode}) for {gcno}: {proc.stderr.strip()}",
                    file=sys.stderr,
                )
                sys.exit(1)

            json_files = glob.glob(f"{temp_dir}/*.gcov.json.gz")
            if not json_files:
                print(
                    f"Error: gcov produced no JSON output for {gcno}",
                    file=sys.stderr,
                )
                sys.exit(1)

            for json_file in json_files:
                try:
                    with gzip.open(json_file, "rt", encoding="utf-8") as f:
                        data = json.load(f)
                except (OSError, json.JSONDecodeError) as exc:
                    print(
                        f"Error: failed to parse gcov JSON {json_file} for {gcno}: {exc}",
                        file=sys.stderr,
                    )
                    sys.exit(1)
                finally:
                    try:
                        os.remove(json_file)
                    except OSError:
                        pass

                for file_info in data.get("files", []):
                    path = file_info.get("file", "")
                    rel_path = normalize_rel_path(path, root_dir, container_prefixes)

                    if not any(rel_path.startswith(p) for p in include_prefixes):
                        continue
                    if any(rel_path.startswith(p) for p in exclude_prefixes):
                        continue

                    file_lines = line_counts_by_file.setdefault(rel_path, {})
                    for line in file_info.get("lines", []):
                        line_num = line.get("line_number")
                        if line_num is None:
                            continue
                        count = line.get("count", 0)
                        file_lines[line_num] = file_lines.get(line_num, 0) + count
    finally:
        for f in glob.glob(f"{temp_dir}/*"):
            try:
                os.remove(f)
            except OSError:
                pass
        try:
            os.rmdir(temp_dir)
        except OSError:
            pass

    file_coverage = {}
    for rel_path, lines_map in line_counts_by_file.items():
        total_lines = len(lines_map)
        covered_lines = sum(1 for count in lines_map.values() if count > 0)
        file_coverage[rel_path] = {
            "total": total_lines,
            "covered": covered_lines,
        }

    return file_coverage


def print_summary_table(file_coverage, title_scope, changed_files=None):
    print("=" * 80)
    print(f" Code Coverage Report ({title_scope})")
    print("=" * 80)
    print(f"{'Source File':<52} {'Covered':<10} {'Total':<10} {'Coverage':<8}")
    print("-" * 80)

    total_covered = 0
    total_lines = 0

    sorted_files = sorted(file_coverage.keys())
    if changed_files is not None:
        sorted_files = [f for f in sorted_files if f in changed_files]

    if not sorted_files:
        print("No source files to report.")
        print("=" * 80)
        return 100.0

    for file_path in sorted_files:
        stats = file_coverage[file_path]
        cov = stats["covered"]
        tot = stats["total"]
        pct = (cov / tot * 100.0) if tot > 0 else 100.0
        total_covered += cov
        total_lines += tot
        print(f"{file_path:<52} {cov:<10} {tot:<10} {pct:>6.1f}%")

    print("-" * 80)
    total_pct = (total_covered / total_lines * 100.0) if total_lines > 0 else 100.0
    print(f"{'TOTAL':<52} {total_covered:<10} {total_lines:<10} {total_pct:>6.1f}%")
    print("=" * 80)
    return total_pct


def generate_markdown_report(file_coverage, output_md, total_pct, patchset=None):
    header = "### 📊 Code Coverage Summary"
    if patchset:
        header += f" (Patch Set {patchset})"

    md_lines = [
        header,
        "",
        f"**Overall Coverage:** **`{total_pct:.1f}%`**",
        "",
        "| Source File | Line Coverage | Executed / Total |",
        "| :--- | :---: | :---: |",
    ]

    for file_path, stats in sorted(file_coverage.items()):
        cov = stats["covered"]
        tot = stats["total"]
        pct = (cov / tot * 100.0) if tot > 0 else 100.0
        md_lines.append(f"| `{file_path}` | {pct:.1f}% | {cov} / {tot} |")

    md_lines.append(f"| **TOTAL** | **{total_pct:.1f}%** | **{sum(s['covered'] for s in file_coverage.values())} / {sum(s['total'] for s in file_coverage.values())}** |")
    md_lines.append("")
    md_lines.append("*Detailed HTML coverage report is preserved in Presubmit build artifacts.*")

    os.makedirs(os.path.dirname(os.path.abspath(output_md)), exist_ok=True)
    with open(output_md, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines) + "\n")


def generate_html_report(file_coverage, output_html, total_pct):
    html_lines = [
        "<!DOCTYPE html>",
        "<html><head><meta charset='utf-8'><title>Code Coverage</title>",
        "<style>",
        "body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 2rem; background: #f8f9fa; color: #212529; }",
        "h1 { color: #1a73e8; }",
        "table { border-collapse: collapse; width: 100%; max-width: 900px; background: #fff; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border-radius: 6px; overflow: hidden; }",
        "th, td { text-align: left; padding: 10px 16px; border-bottom: 1px solid #dee2e6; }",
        "th { background: #e9ecef; font-weight: 600; }",
        "tr:last-child { font-weight: bold; background: #f1f3f5; }",
        ".good { color: #2e7d32; font-weight: bold; }",
        ".medium { color: #f57c00; font-weight: bold; }",
        ".low { color: #c62828; font-weight: bold; }",
        "</style></head><body>",
        f"<h1>Code Coverage: {total_pct:.1f}%</h1>",
        "<table><thead><tr><th>File</th><th>Covered</th><th>Total</th><th>Coverage</th></tr></thead><tbody>",
    ]

    for file_path, stats in sorted(file_coverage.items()):
        cov = stats["covered"]
        tot = stats["total"]
        pct = (cov / tot * 100.0) if tot > 0 else 100.0
        cls = "good" if pct >= 80 else ("medium" if pct >= 60 else "low")
        html_lines.append(f"<tr><td>{file_path}</td><td>{cov}</td><td>{tot}</td><td class='{cls}'>{pct:.1f}%</td></tr>")

    html_lines.append(f"<tr><td>TOTAL</td><td>-</td><td>-</td><td>{total_pct:.1f}%</td></tr>")
    html_lines.append("</tbody></table></body></html>")

    os.makedirs(os.path.dirname(os.path.abspath(output_html)), exist_ok=True)
    with open(output_html, "w", encoding="utf-8") as f:
        f.write("\n".join(html_lines))


def main():
    parser = argparse.ArgumentParser(description="Generate code coverage report")
    parser.add_argument("--build-dir", default=".build-release-coverage", help="Path to coverage build directory")
    parser.add_argument("--root-dir", default=".", help="Root of repository")
    parser.add_argument("--html", help="Path to write HTML report")
    parser.add_argument("--markdown", help="Path to write Markdown report")
    args = parser.parse_args()

    root_dir = os.path.abspath(args.root_dir)
    build_dir = os.path.abspath(os.path.join(root_dir, args.build_dir))

    coverage = generate_gcov_json(
        build_dir=build_dir,
        root_dir=root_dir,
        subdirs=("src", "vmsdk"),
        include_prefixes=("src/", "vmsdk/src/"),
        exclude_prefixes=("vmsdk/src/valkey_module_api",),
        container_prefixes=("/workspaces/valkey-search",),
    )
    if not coverage:
        print("No coverage data found in", build_dir)
        sys.exit(1)

    total_pct = print_summary_table(coverage, "Source Files in src/ and vmsdk/src/")

    if args.markdown:
        patchset = os.getenv("KOKORO_GERRIT_PATCHSET_NUMBER") or os.getenv("GERRIT_PATCHSET_NUMBER")
        generate_markdown_report(coverage, os.path.abspath(args.markdown), total_pct, patchset)
        print(f"Markdown coverage report written to: {args.markdown}")

    if args.html:
        generate_html_report(coverage, os.path.abspath(args.html), total_pct)
        print(f"HTML coverage report written to: {args.html}")


if __name__ == "__main__":
    main()
