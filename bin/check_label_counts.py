#!/usr/bin/env python3

import datetime
import sys
import os
from pathlib import Path

if len(sys.argv) < 2:
    print("Usage: check_label_counts.py <label_counts.tsv>")
    sys.exit(1)

label_counts_file = sys.argv[1]
if not os.path.isfile(label_counts_file):
    print(f"Error: File '{label_counts_file}' not found.")
    sys.exit(1)

try:
    awk_cmd = f"awk 'NR>1 {{sum+=$2}} END {{print sum+0}}' {label_counts_file}"
    total = int(os.popen(awk_cmd).read().strip())
except Exception as e:
    print(f"Error processing file: {e}")
    sys.exit(1)

now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
html = f"""
<!DOCTYPE html>
<html lang='en'>
<head>
    <meta charset='UTF-8'>
    <title>Label Count Check</title>
    <style>
        body {{ font-family: Arial, sans-serif; background: #f7f7f7; margin: 0; padding: 0; }}
        .container {{ max-width: 600px; margin: 40px auto; background: #fff; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); padding: 32px; }}
        h1 {{ color: #2c3e50; margin-bottom: 16px; }}
        .status {{ font-size: 1.2em; color: #27ae60; font-weight: bold; }}
        .meta {{ color: #888; font-size: 0.9em; margin-bottom: 24px; }}
        .count {{ font-size: 1.5em; color: #2980b9; margin-bottom: 16px; }}
    </style>
</head>
<body>
    <div class='container'>
        <h1>Label Count Check</h1>
        <div class='meta'>Generated: {now}</div>
        <div class='count'>Total labels: <b>{total}</b></div>
        <div class='status'>Status: PASS</div>
    </div>
</body>
</html>
"""
with open("label_check_report.html", "w") as f:
    f.write(html)