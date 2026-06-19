import os
import tempfile


def get_total_folder_size(folder_path):
    total_size = 0

    # os.walk scans all nested files and subdirectories
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                # Add file size to the total sum
                total_size += os.path.getsize(file_path)
            except (FileNotFoundError, PermissionError):
                # Skips locked files or files deleted by the OS during the run
                continue

    return total_size


def format_size(bytes_size):
    # Converts raw bytes into a human-readable format
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} TB"


# Target the server's default temp folder
temp_dir = tempfile.gettempdir()
raw_bytes = get_total_folder_size(temp_dir)
readable_size = format_size(raw_bytes)

print(f"Target Folder: {temp_dir}")
print(f"Total Size: {readable_size} ({raw_bytes:,} bytes)")
