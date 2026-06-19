import os
import tempfile
import psutil


def get_active_temp_processes(folder_path):
    """
    Scans running programs and returns a dictionary mapping
    open temporary file paths to their parent program name and PID.
    """
    active_map = {}
    for proc in psutil.process_iter(["pid", "name"]):
        try:
            # Check all open files for this running process
            for open_file in proc.open_files():
                if open_file.path.startswith(folder_path):
                    # Map the file path to its source program details
                    active_map[open_file.path] = {
                        "name": proc.info["name"],
                        "pid": proc.info["pid"],
                    }
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            # Ignore short-lived processes or files needing higher root privileges
            continue
    return active_map


def get_folder_metrics(folder_path):
    total_size = 0
    all_files = []

    # Get the dictionary of active temp files and their source programs
    active_files = get_active_temp_processes(folder_path)

    # Recursively scan the directory
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                # Retrieve file size
                file_size = os.path.getsize(file_path)
                total_size += file_size

                # Check if this file has a running program attached to it
                process_info = active_files.get(file_path, None)

                # Store the file path, size, and source program info
                all_files.append((file_path, file_size, process_info))
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
print("=" * 60)
print(f"TOTAL FOLDER SIZE: {format_size(total_bytes)}")
print("=" * 60)

# Print Breakdown
print("\nTOP 10 LARGEST FILES:")
if not largest_files:
    print("No files found in the temporary directory.")
else:
    for idx, (path, size, source) in enumerate(largest_files, 1):
        # Format the source process label if detected
        if source:
            source_label = (
                f"[ACTIVE -> Program: {source['name']} | PID: {source['pid']}]"
            )
        else:
            source_label = "[INACTIVE/CLOSED]"

        print(f"{idx}. {format_size(size)} -> {path}")
        print(f"   Status: {source_label}\n")
