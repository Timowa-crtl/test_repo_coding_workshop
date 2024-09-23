import os
import concurrent.futures
from datetime import datetime

# Version 1.0: This script searches for fastq.gz files in the script-directory. It then asks the user for confirmation before deletion.
# Version 1.1: Added requirement of a very explicit confirmation string to prevent accidental deletion in a wrong directory


def find_fastq_gz_files_in_dir(directory):
    """Find all .fastq.gz files in the given directory."""
    fastq_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".fastq.gz"):
                fastq_files.append(os.path.join(root, file))
    return fastq_files


def find_fastq_gz_files(directory):
    """Find all .fastq.gz files in the given directory and its subdirectories using multithreading."""
    print(f"Searching for .fastq.gz-files in '{os.path.basename(directory)}':")
    fastq_files = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(find_fastq_gz_files_in_dir, os.path.join(directory, d)) for d in os.listdir(directory) if os.path.isdir(os.path.join(directory, d))]
        for future in concurrent.futures.as_completed(futures):
            fastq_files.extend(future.result())

    # Additionally check the root directory itself
    fastq_files.extend(find_fastq_gz_files_in_dir(directory))

    # remove dublicates and sort
    fastq_files = list(set(fastq_files))
    fastq_files = sorted(fastq_files)

    return fastq_files


def sizeof(file_path):
    """Return the size of the file at file_path in gigabytes."""
    return os.path.getsize(file_path) / (1024 * 1024 * 1024)


def log_deletions(files, log_file, nr_files, volume_files):
    """Log the deleted files along with the current time and date to a specified log file."""
    with open(log_file, "a") as f:
        f.write(f"\nDeletion Log - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"A total of {nr_files} files and {volume_files:.2f} GB were deleted:\n")
        for file in files:
            f.write(f"{file}\n")


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_folder = os.path.basename(script_dir)
    fastq_files = find_fastq_gz_files(script_dir)

    if not fastq_files:
        print("No .fastq.gz files found in the script directory or its subdirectories.")
        return

    print("Files found:")
    for file in fastq_files:
        print(os.path.basename(file))
    print(f"A total of {len(fastq_files)} were found.")
    if "_" in script_folder:
        folder_string = script_folder.split("_")[0]
    else:
        folder_string = script_folder

    confirmation_string = f"delete {len(fastq_files)} fastqs in {folder_string}"
    allowed_answers = ["n", "no", confirmation_string.lower()]
    answer = "_"
    while answer not in allowed_answers:
        answer = input(f"\nAre you sure you want to delete all these files? Confirm by typing '{confirmation_string}': ").lower()

    if answer == confirmation_string.lower():
        deleted_fastq_files = []
        deleted_data = 0
        for file in fastq_files:
            try:
                filepath = file
                filesize = sizeof(file)
                os.remove(file)
                print(".", end="", flush=True)
                deleted_fastq_files.append(filepath)
                deleted_data += filesize
            except OSError as e:
                print(f"Error deleting {os.path.basename(file)}: {e}")
        print("\nDeletion complete.")
        print(f"A total of {deleted_data:.2f} GB was deleted.")
        print(f"A total of {len(deleted_fastq_files)}/{len(fastq_files)} files was deleted.")

        # Log deletions
        current_date = datetime.now().strftime("%y%m%d")
        log_file = os.path.join(script_dir, f"{current_date}_deleted_files.txt")
        log_deletions(deleted_fastq_files, log_file, len(deleted_fastq_files), deleted_data)

    else:
        print("Deletion cancelled.")


if __name__ == "__main__":
    main()
