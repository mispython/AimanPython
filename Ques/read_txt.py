import re

file_path = input("Enter full file path: ").strip()

pattern = re.compile(r"\b\d{2}/\d{2}/26\b")

found = 0
max_rows = 100

with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
    for line_num, line in enumerate(f, start=1):
        if pattern.search(line):
            print(f"Line {line_num}: {line.rstrip()}")
            found += 1

            if found >= max_rows:
                print(f"\nStopped after {max_rows} matches.")
                break

print(f"\nMatches displayed: {found}")

================

import sys

def read_first_n_lines(file_path, n):
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for i in range(n):
            line = f.readline()
            if not line:
                break
            print(line.rstrip())

if __name__ == "__main__":
    file_path = input("Enter full file path: ").strip()
    n = int(input("How many lines you want to read: ").strip())

    read_first_n_lines(file_path, n)
