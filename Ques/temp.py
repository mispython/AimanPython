import os
import tempfile
import psutil


def get_active_temp_processes(folder_path):
    """
    Scans every running program on the server and returns a dictionary 
    mapping all active temp file paths to their parent program name, PID, and size.
    """
    active_map = {}
    for proc in psutil.process_iter(["pid", "name"]):
        try:
            # Check all open files for this running process
            for open_file in proc.open_files():
                if open_file.path.startswith(folder_path):
                    file_path = open_file.path
                    try:
                        file_size = os.path.getsize(file_path)
                    except (FileNotFoundError, PermissionError):
                        file_size = 0  # Fallback if file vanishes instantly
                    
                    # Track all running programs using temp files
                    active_map[file_path] = {
                        "name": proc.info["name"],
                        "pid": proc.info["pid"],
                        "size": file_size
                    }
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            # Ignore short-lived processes or system processes requiring higher privileges
            continue
    return active_map


def get_folder_metrics(folder_path):
    total_size = 0
    all_files = []

    # 1. Fetch ALL temporary files currently linked to running programs
    active_files = get_active_temp_processes(folder_path)

    # 2. Recursively scan the entire directory structure for sizes
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                file_size = os.path.getsize(file_path)
                total_size += file_size

                # Check if this file is actively open by a running program
                process_info = active_files.get(file_path, None)

                # Store file data for our top 10 breakdown
                all_files.append((file_path, file_size, process_info))
            except (FileNotFoundError, PermissionError):
                continue

    # Sort all found files by size in descending order for the top 10 block
    top_10_files = sorted(all_files, key=lambda x: x[1], reverse=True)[:10]

    return total_size, top_10_files, active_files


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

total_bytes, largest_files, all_active_files = get_folder_metrics(temp_dir)

# ==========================================
# SECTION 1: GLOBAL SERVER SUMMARY
# ==========================================
print("=" * 65)
print(f"TOTAL TEMP FOLDER SIZE: {format_size(total_bytes)}")
print(f"TOTAL ACTIVE TEMP FILES IN USE: {len(all_active_files)}")
print("=" * 65)

# ==========================================
# SECTION 2: TOP 10 LARGEST FILES ON DISK
# ==========================================
print("\n[SECTION A] TOP 10 LARGEST TEMP FILES:")
if not largest_files:
    print("No files found in the temporary directory.")
else:
    for idx, (path, size, source) in enumerate(largest_files, 1):
        status = f"[ACTIVE -> Process: {source['name']} (PID: {source['pid']})]" if source else "[INACTIVE/IDLE]"
        print(f" {idx}. {format_size(size)} -> {path}")
        print(f"    Status: {status}")

# ==========================================
# SECTION 3: ALL RUNNING PROGRAM TEMP FILES
# ==========================================
print("\n" + "=" * 65)
print("[SECTION B] ALL ACTIVE TEMPORARY FILES BY RUNNING PROGRAMS:")
print("=" * 65)
if not all_active_files:
    print("No running programs are currently utilizing temp files.")
else:
    # Group or iterate through every single discovered running file dependency
    for idx, (file_path, info) in enumerate(all_active_files.items(), 1):
        print(f" Active File #{idx}:")
        print(f"  └─ Program Source : {info['name']} (PID: {info['pid']})")
        print(f"  └─ File Location   : {file_path}")
        print(f"  └─ Current Size    : {format_size(info['size'])}\n")
