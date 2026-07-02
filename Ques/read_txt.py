import sys

def read_first_n_lines(file_path, n=10):
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            for i in range(n):
                line = f.readline()
                if not line:
                    break
                print(line.rstrip())
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python read_txt.py <file_path> [num_lines]")
        sys.exit(1)

    file_path = sys.argv[1]
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 10

    read_first_n_lines(file_path, n)
