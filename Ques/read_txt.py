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
