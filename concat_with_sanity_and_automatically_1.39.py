import os
import filecmp
import shutil
import csv
import datetime

# Version 1.39:This script concatenates FASTQ files from the current sequencing run with corresponding low-read files from previous runs.
# It compares the FASTQ files, skips identical ones, and concatenates non-identical pairs.
# The script handles file operations, logs the process, calculates file sizes, and manages errors.
# It generates both local and global CSV log files with details of each concatenation operation.


if __name__ == "__main__":

    # Warning and data advisory
    print("")
    print("WARNING: Concatenating large files can be a very memory-intensive task. Please ensure ample storage is available for this process. If there is not, press Ctrl+C now")
    print("")

    # Set directory paths. Sequence1 is the fastq file from the current run, Sequence2 is the low-read from the previous run.
    fastq = "fastq"  # here are the fastq files for the current run
    low_reads_for_concat = (
        "/media/share/novaseq01/Output/sequencing_data_for_upload/low_reads_for_concat/"  # folder in which fastq files from ALL low-reads are kept until resequencing and concatenation
    )
    old_fastqs_sequence2 = "/media/share/novaseq01/Output/sequencing_data_for_upload/low_reads_for_concat/already_concatenated_low_reads"  # destination for sequence1 fastqs. Original fastqs from current run are moved here after concatenation.
    old_fastqs_sequence1 = "old_fastqs_not_needed"  # destination for sequence2 fastqs. low-read fastqs are moved here after resequencing and concatenation
    temp_concat_output_dir = "temp_output"  # temporary folder to store concatenated fastqs before copying them to fastq

    # Create the output directories if it doesn't exist
    os.makedirs(temp_concat_output_dir, exist_ok=True)
    os.makedirs(old_fastqs_sequence1, exist_ok=True)
    os.makedirs(old_fastqs_sequence2, exist_ok=True)

    # Get the list of files in sequence_1 directory
    file_inventory = os.listdir(fastq)

    # Initialize counters for scan
    skipped_files = 0
    concatenated_files = 0

    # Loop through files in fastq directory and check for matching files in low_reads_for_concat directory. Ask user to confirm if he wants to concatenate

    print(f"The following files will be concatenated:")

    for file in file_inventory:
        if file in os.listdir(low_reads_for_concat):
            # Compare files
            input_file_1 = os.path.join(fastq, file)
            input_file_2 = os.path.join(low_reads_for_concat, file)
            if filecmp.cmp(input_file_1, input_file_2):
                print("Error: Skipping %s: Files are identical" % file)
                skipped_files += 1
            else:
                print(file)
                concatenated_files += 1

        else:
            continue

    print(f"Number of files that will be concatenated: {concatenated_files}")
    print(f"Number of files that gave an Error as both files are identical: {skipped_files}")
    print()

    # do_it = input("Do you want to concatenate these files? (Y/n) ")

    do_it = "Y"

    if do_it == "Y":
        print(f"OK, {concatenated_files} files will be concatenated.")
    elif do_it == "n":
        print("OK, no files were concatenated.")
        exit()  # This will terminate the script
    else:
        do_it = input("Asking again. Do you want to concatenate these files? (Y/n) ")
        if do_it == "Y":
            print(f"OK, {concatenated_files} files will be concatenated.")
        elif do_it != "Y":
            print("OK, no files were concatenated.")
            exit()  # This will terminate the script

    # Variables for logging
    skipped_files = 0
    concatenated_files = 0
    log_entries = []
    current_date = datetime.datetime.now()  # Get the current date and time
    date = current_date.strftime("%Y-%m-%d")  # Format the date as a string (e.g., "YYYY-MM-DD")

    # Loop through files in fastq directory and check for matching files in low_reads_for_concat directory
    for file in os.listdir(fastq):
        if file in os.listdir(low_reads_for_concat):
            # Compare files
            input_file_1 = os.path.join(fastq, file)
            input_file_2 = os.path.join(low_reads_for_concat, file)
            if filecmp.cmp(input_file_1, input_file_2):
                print("Skipping %s: Files are identical" % file)
                skipped_files += 1
            else:
                # Concatenate files
                print("Processing %s... " % file)
                output_file = os.path.join(temp_concat_output_dir, file)
                shutil.copyfile(input_file_1, output_file)
                with open(output_file, "ab") as outfile:
                    with open(input_file_2, "rb") as infile:
                        shutil.copyfileobj(infile, outfile)
                concatenated_files += 1

                # Get the sizes of the files in GB with 4 digits after the decimal separator
                size_sequence_1 = os.path.getsize(input_file_1) / (1024 * 1024 * 1024)
                size_sequence_2 = os.path.getsize(input_file_2) / (1024 * 1024 * 1024)
                size_concatenated = os.path.getsize(output_file) / (1024 * 1024 * 1024)

                # Log information for this iteration
                log_entry = {
                    "date": date,
                    "filename": file,
                    "size_sequence_1": size_sequence_1,
                    "size_sequence_2": size_sequence_2,
                    "size_concatenated": size_concatenated,
                    "errors": "",
                }

                try:
                    if size_sequence_1 == size_concatenated or size_sequence_2 == size_concatenated:
                        log_entry["errors"] = "Warning: Size mismatch"
                    else:
                        # Move and replace files
                        shutil.move(input_file_1, os.path.join(old_fastqs_sequence1, file))
                        shutil.move(output_file, os.path.join(fastq, file))
                        shutil.move(input_file_2, os.path.join(old_fastqs_sequence2, file))
                except Exception as e:
                    log_entry["errors"] = str(e)
                    if os.path.exists(input_file_1):
                        os.remove(input_file_1)
                    if os.path.exists(output_file):
                        os.remove(output_file)
                    if os.path.exists(input_file_2):
                        os.remove(input_file_2)

                log_entries.append(log_entry)
        else:
            continue

    # Write log entries to the global CSV logfile
    global_log_file_path = "/media/share/novaseq01/Output/sequencing_data_for_upload/low_reads_for_concat/log_data/global_concat_log.csv"

    with open(global_log_file_path, "a", newline="") as csvfile:  # Use 'a' for append mode
        fieldnames = ["date", "filename", "size_sequence_1", "size_sequence_2", "size_concatenated", "errors"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Check if the file is empty and write the header if needed
        if csvfile.tell() == 0:
            writer.writeheader()

        writer.writerows(log_entries)

    # Add log entries to the local CSV logfile
    log_file_path = "concat_log.csv"
    with open(log_file_path, "a", newline="") as csvfile:
        fieldnames = ["date", "filename", "size_sequence_1", "size_sequence_2", "size_concatenated", "errors"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(log_entries)

    print(f"Processing complete. Skipped files: {skipped_files}, Concatenated files: {concatenated_files}")
    print(f"Data was logged in {log_file_path}.")
    print()
    print()
    print()
    print("I'm on a rollercoaster that only goes up! - JG")
