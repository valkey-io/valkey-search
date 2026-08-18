#!/bin/bash
LAST_LINE=0
while true; do
  if [ ! -f /tmp/opt_bench.log ]; then
    sleep 1
    continue
  fi
  # Check if a new summary table has been printed
  NEW_TABLE=$(tail -n +$LAST_LINE /tmp/opt_bench.log | grep -n "BENCHMARK RESULTS SUMMARY" | head -n 1 | cut -d: -f1)
  if [ ! -z "$NEW_TABLE" ]; then
    # We found a new table! Let's print the table lines.
    # The table is 7 lines long, starting 2 lines below the SUMMARY line.
    START_LINE=$((LAST_LINE + NEW_TABLE + 1))
    END_LINE=$((START_LINE + 5))
    echo "============================="
    sed -n "${START_LINE},${END_LINE}p" /tmp/opt_bench.log
    echo "============================="
    LAST_LINE=$END_LINE
  fi
  sleep 1
done
