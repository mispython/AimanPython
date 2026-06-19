import os


def get_readable_size(path):
    try:
        b = os.path.getsize(path)
        # Loop through size units
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if b < 1024.0:
                return f"{b:.2f} {unit}"
            b /= 1024.0
    except FileNotFoundError:
        return "File not found"


# Usage
print(get_readable_size("/tmp/large_payload.tmp"))
# Output example: "14.25 MB"
