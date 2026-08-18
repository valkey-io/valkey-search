import csv
import sys

def calc_benefit(main_val, cur_val, higher_is_better=True):
    try:
        m = float(main_val)
        c = float(cur_val)
        if m == 0: return 0
        if higher_is_better:
            return (c - m) / m * 100
        else:
            return (m - c) / m * 100
    except:
        return None

data = []
with open('/tmp/final_report_run4.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        if len(row) > 15 and row[0] in ['main', 'current_tcache']:
            data.append(row)

grouped = {}
for row in data:
    branch = row[0]
    setup = row[1]
    threads = int(row[2])
    key = (setup, threads)
    if key not in grouped:
        grouped[key] = {}
    grouped[key][branch] = row

lines = []
lines.append("# FINAL Allocator Benchmark Report (TCache k=256)")
lines.append("\nThis report compares the `optimized_rax` branch (with Lock-Free TCache allocator optimizations) against the original baseline `main` branch (using `jemalloc`).")
lines.append("\n## Results Summary")
lines.append("```text")
lines.append("==============================================================================================================")
lines.append("CALCULATED PERCENTAGE BENEFITS (current_tcache vs main)")
lines.append("==============================================================================================================")
lines.append(f"{'Setup':<28} | {'Threads':<7} | {'Ingest Throughput':<17} | {'Mutation Rate':<13} | {'Search QPS':<10} | {'Search RAM Savings':<18} | {'RSS Savings':<12}")
lines.append("-" * 122)

for setup, threads in sorted(grouped.keys()):
    if 'main' not in grouped[(setup, threads)] or 'current_tcache' not in grouped[(setup, threads)]:
        continue
    
    m_row = grouped[(setup, threads)]['main']
    c_row = grouped[(setup, threads)]['current_tcache']
    
    ing_b = calc_benefit(m_row[3], c_row[3], True)
    mut_b = calc_benefit(m_row[9], c_row[9], True)
    qps_b = calc_benefit(m_row[15], c_row[15], True)
    ram_b = calc_benefit(m_row[12], c_row[12], False)
    rss_b = calc_benefit(m_row[14], c_row[14], False)
    
    def fmt(val):
        if val is None: return "N/A"
        return f"{'+' if val >= 0 else ''}{val:.2f}%"

    lines.append(f"{setup:<28} | {str(threads):<7} | {fmt(ing_b):<17} | {fmt(mut_b):<13} | {fmt(qps_b):<10} | {fmt(ram_b):<18} | {fmt(rss_b):<12}")

lines.append("==============================================================================================================")
lines.append("```")

with open("/usr/local/google/home/yairg/.gemini/jetski/brain/8817a01d-ca7a-49a4-8ac2-f6fbb90ffcfb/final_tcache_report.md", "w") as f:
    f.write("\n".join(lines))
