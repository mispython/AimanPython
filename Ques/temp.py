import os
import tempfile


def get_folder_metrics(folder_path):
    total_size = 0
    all_files = []

    # Recursively scan the directory
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                # Retrieve file size
                file_size = os.path.getsize(file_path)
                total_size += file_size

                # Store the file path and its size for sorting
                all_files.append((file_path, file_size))
            except (FileNotFoundError, PermissionError):
                # Safely ignore system-locked or short-lived temp files
                continue

    # Sort all found files by size in descending order
    top_10_files = sorted(all_files, key=lambda x: x[1], reverse=True)[:10]

    return total_size, top_10_files


def format_size(bytes_size):
    # Formats raw bytes into a human-readable string
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} TB"


# Target the server's default temp directory
temp_dir = tempfile.gettempdir()
print(f"Scanning target folder: {temp_dir}...\n")

total_bytes, largest_files = get_folder_metrics(temp_dir)

# Print Summary
print("=" * 50)
print(f"TOTAL FOLDER SIZE: {format_size(total_bytes)}")
print("=" * 50)

# Print Breakdown
print("\nTOP 10 LARGEST FILES:")
if not largest_files:
    print("No files found in the temporary directory.")
else:
    for idx, (path, size) in enumerate(largest_files, 1):
        print(f"{idx}. {format_size(size)} -> {path}")
