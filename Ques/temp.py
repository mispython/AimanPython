import os
import tempfile
import psutil


def get_active_temp_processes(folder_path):
    """
    Scans every running program on the server and maps active temp file paths
    to their precise creation source (Binary path and exact execution command).
    """
    active_map = {}
    for proc in psutil.process_iter(["pid", "name", "exe", "cmdline"]):
        try:
            # Check every open file handle currently registered to this process
            for open_file in proc.open_files():
                if open_file.path.startswith(folder_path):
                    file_path = open_file.path
                    try:
                        file_size = os.path.getsize(file_path)
                    except (FileNotFoundError, PermissionError):
                        file_size = 0  # Fallback if the file vanishes quickly
                    
                    # Reconstruction of execution string for debugging
                    raw_cmd = proc.info.get("cmdline")
                    execution_command = " ".join(raw_cmd) if raw_cmd else "Unknown"

                    active_map[file_path] = {
                        "name": proc.info["name"],
                        "pid": proc.info["pid"],
                        "binary_path": proc.info.get("exe") or "Hidden/System Binary",
                        "triggered_by": execution_command,
                        "size": file_size
                    }
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    return active_map


def get_folder_metrics(folder_path):
    total_size = 0
    file_count = 0
    folder_count = 0
    all_files = []

    # 1. Fetch ALL temporary files mapped directly to active runtime sources
    active_files = get_active_temp_processes(folder_path)

    # 2. Disk scan loop for raw sizing and item counts
    for root, dirs, files in os.walk(folder_path):
        # Count subdirectories found in the current root
        folder_count += len(dirs)
        
        for file in files:
            file_count += 1
            file_path = os.path.join(root, file)
            try:
                file_size = os.path.getsize(file_path)
                total_size += file_size

                # Check if this disk asset is tied to a living process string
                process_info = active_files.get(file_path, None)
                all_files.append((file_path, file_size, process_info))
            except (FileNotFoundError, PermissionError):
                continue

    # Sort files globally by size descending for the disk metrics block
    top_10_files = sorted(all_files, key=lambda x: x, reverse=True)[:10]

    return total_size, file_count, folder_count, top_10_files, active_files


def format_size(bytes_size):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} TB"


# Target the server's default temp directory
temp_dir = tempfile.gettempdir()
print(f"Scanning target folder: {temp_dir}...\n")

total_bytes, total_files, total_folders, largest_files, all_active_files = get_folder_metrics(temp_dir)

# ==========================================================
# SECTION 1: GLOBAL SERVER SUMMARY (Sizing & Item Count)
# ==========================================================
print("=" * 75)
print(f"TARGET TEMP DIRECTORY        : {temp_dir}")
print(f"TOTAL TEMP FOLDER SIZE       : {format_size(total_bytes)}")
print(f"TOTAL FILES DETECTED         : {total_files:,}")
print(f"TOTAL SUBFOLDERS DETECTED    : {total_folders:,}")
print(f"TOTAL ACTIVE FILES IN USE    : {len(all_active_files)}")
print("=" * 75)

# ==========================================================
# SECTION 2: TOP 10 LARGEST FILES ON DISK
# ==========================================================
print("\n[SECTION A] TOP 10 LARGEST TEMP FILES:")
if not largest_files:
    print(" No files found in the temporary directory.")
else:
    for idx, (path, size, source) in enumerate(largest_files, 1):
        status = f"[ACTIVE -> Program: {source['name']} (PID: {source['pid']})]" if source else "[INACTIVE/IDLE]"
        print(f" {idx}. {format_size(size)} -> {path}")
        print(f"    Status: {status}")

# ==========================================================
# SECTION 3: COMPREHENSIVE ACTIVE PROGRAM AND ORIGIN DEEP DIVE
# ==========================================================
print("\n" + "=" * 75)
print("[SECTION B] ALL ACTIVE TEMPORARY FILES BY RUNNING PROGRAMS & ORIGINS:")
print("=" * 75)
if not all_active_files:
    print(" No running programs are currently utilizing temp files.")
else:
    for idx, (file_path, info) in enumerate(all_active_files.items(), 1):
        print(f" Active File #{idx}:")
        print(f"  ├─ Temp File Path : {file_path}")
        print(f"  ├─ File Size      : {format_size(info['size'])}")
        print(f"  ├─ Program Name   : {info['name']} (PID: {info['pid']})")
        print(f"  ├─ Binary Origin  : {info['binary_path']}")
        print(f"  └─ Launch Trigger : {info['triggered_by']}\n")
