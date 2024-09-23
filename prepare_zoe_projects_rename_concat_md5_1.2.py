import csv
import datetime
import filecmp
import hashlib
import os
import pandas as pd
import re
import shutil
import sys

# 1.0 this script reads 'input_file' and 'exported.csv' to perform renaming, MD5 calculation, concatenation, and sorting of Zoe fastq-files within '{project_id}_gcloud' directories for uploading to Google Cloud.
# 1.1 added additional support for new sample information export format from 'FS_7_3_Sample_Information'


def calculate_md5(file_path, md5_file_path):
    try:
        md5_hash = hashlib.md5()
        with open(file_path, "rb") as file:
            for chunk in iter(lambda: file.read(4096), b""):
                md5_hash.update(chunk)
        md5_checksum = md5_hash.hexdigest()

        # Get the basename of the file_path
        filename = os.path.basename(file_path)

        # Write the MD5 checksum along with the file name to the .md5 file
        with open(md5_file_path, "w") as md5_file:
            md5_file.write(f"{md5_checksum}  {filename}\n")
    except Exception as e:
        print(f"Error calculating MD5 checksum for {file_path}: {e}")


def get_zoe_project_ids(input_file):
    zoe_project_ids = set()
    with open(input_file, "r") as csvfile:
        reader = csv.DictReader(csvfile)

        # Ensure project_id and customer columns exist
        fieldnames = reader.fieldnames
        project_id_field = next((field for field in fieldnames if field.lower() == "project_id"), None)
        customer_field = next((field for field in fieldnames if field.lower() == "customer"), None)

        if project_id_field is None:
            raise ValueError("Column 'project_ID' not found in project_info.csv")
        if customer_field is None:
            raise ValueError("Column 'customer' not found in project_info.csv")

        # Process rows
        for row in reader:
            customer = row.get(customer_field)
            if customer and customer.lower() == "zoe":
                project_id = row.get(project_id_field)
                if project_id:
                    zoe_project_ids.add(project_id)

    return zoe_project_ids


def get_number_of_files(working_directory, folders):
    # Initialize the total number of files
    total_number_of_files = 0

    # Iterate over the folders in the working directory
    for folder in os.listdir(working_directory):
        folder_path = os.path.join(working_directory, folder)

        # Check if it's a directory and its name is in project_ids_with_md5
        if os.path.isdir(folder_path) and os.path.basename(folder_path) in folders:
            # Change the current working directory to the folder
            os.chdir(folder_path)

            # Count the .gz files in the current folder
            gz_files = [file for file in os.listdir() if file.endswith(".gz")]
            num_files = len(gz_files)

            # Update the total number of files
            total_number_of_files += num_files

    return total_number_of_files


def find_exported_csv(project_ID, search_directory):
    for file in os.listdir(search_directory):
        if file.startswith("exported_csv_ZOE Sample Information Form") and project_ID in file and file.endswith(".csv"):
            return os.path.join(search_directory, file)
        elif (
            file.lower().startswith("exported_csv")
            and ("SampleInformation").lower() in file.lower()
            and "zoe" in file.lower()
            and project_ID.lower() in file.lower()
            and file.lower().endswith(".csv")
        ):
            return os.path.join(search_directory, file)
    return None


def get_id_map(exported_csv_path):
    id_map = {}
    with open(exported_csv_path, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            zymo_id = row["ZymoID"]
            sample_id = row["SampleID"]

            # skip empty rows or 0-values
            if zymo_id == None or zymo_id == "" or sample_id == 0 or sample_id == "0":
                continue

            else:
                # Check if changes need to be made to ZymoID
                if "." in zymo_id or " " in zymo_id or "\t" in zymo_id:
                    cleaned_zymo_id = zymo_id.replace(".", "-").replace(" ", "").replace("\t", "")
                    # print(f"ZymoID {zymo_id} modified to {cleaned_zymo_id}")
                else:
                    cleaned_zymo_id = zymo_id

                # Check if changes need to be made to sample_id
                if "." in sample_id or " " in sample_id or "\t" in sample_id:
                    cleaned_sample_id = sample_id.replace(".", "-").replace(" ", "").replace("\t", "")
                    # print(f"SampleID {sample_id} modified to {cleaned_sample_id}")
                else:
                    cleaned_sample_id = sample_id

                # Ensure cleaned_sample_id contains "-" and "STOOL"
                if "-" in cleaned_sample_id and "STOOL" in cleaned_sample_id:
                    # Ensure zymo_id contains "zr" and "_"
                    if "zr" in cleaned_zymo_id and "_" in cleaned_zymo_id:
                        id_map[cleaned_zymo_id] = cleaned_sample_id
                    else:
                        print(f"Invalid ZymoID {cleaned_zymo_id}: does not contain 'zr' or '_'")
                else:
                    print(f"Invalid SampleID {cleaned_sample_id}: does not contain '-' or 'STOOL'")

    return id_map


def rename_and_get_md5(file_list, id_map):
    for filename in file_list:
        if filename.endswith("_R1.fastq.gz") or filename.endswith("_R2.fastq.gz"):
            zymo_id = "_".join(filename.split("_")[:2])
            # print(f"zymo_id: {zymo_id}")
            sample_id = filename.split("_")[0]
            # print(f"sample_id: {sample_id}")

            if zymo_id in id_map:
                sample_id = id_map[zymo_id]
                new_filename = filename.replace(zymo_id, sample_id)
                os.rename(filename, new_filename)

                # Calculate MD5 checksum for the file
                md5_filename = new_filename.replace(".fastq.gz", ".md5")
                calculate_md5(new_filename, md5_filename)

            elif sample_id in id_map.values():
                print("File already renamed")

                # Calculate MD5 checksum for the file
                md5_filename = filename.replace(".fastq.gz", ".md5")
                calculate_md5(filename, md5_filename)

            else:
                print("ERROR! Name not found in exported.csv")


def sort_processed_files(file_list, id_map):
    for filename in file_list:
        if filename.endswith("_R1.fastq.gz") or filename.endswith("_R2.fastq.gz") or filename.endswith(".md5"):

            sample_id = filename.split("_")[0]
            # print(f"sample_id: {sample_id}")

            if sample_id in id_map.values():
                if "Reseq" in filename:
                    reseq_tag = extract_reseq_tag(filename)
                    # print(f"reseq_tag: {reseq_tag}")
                    folder_name = sample_id + "_" + reseq_tag

                else:
                    folder_name = sample_id

                if not os.path.exists(folder_name):
                    os.makedirs(folder_name)

                # Move the file to the folder corresponding to the sample_id
                shutil.move(filename, os.path.join(folder_name, filename))

            else:
                print("ERROR! File not correctly renamed!")


def extract_reseq_tag(filename):
    parts = filename.split("_")
    reseq_tag = None

    for part in parts:
        if "Reseq" in part:
            reseq_tag = part
            break

    return reseq_tag


def concatenate_zoe_fastqs(folder_path_to_concatenate, low_reads_folder, working_directory):

    def get_zymo_id(filename):
        zymo_id = "_".join(filename.split("_")[:2])
        return zymo_id

    def get_project_id(filename):
        project_id = filename.split("_")[0]
        return project_id

    def get_reseq_filename(filename, reseq_tag):
        # Define the pattern to match the filename
        pattern = r"(\w+)_\d+_R(\d+)\.fastq\.gz"

        # Extract parts of the filename using regex groups
        match = re.match(pattern, filename)
        if match:
            zymo_id = get_zymo_id(filename)
            reseq_tag = reseq_tag
            read_number = match.group(2)

            # Construct the new filename
            reseq_filename = f"{zymo_id}_{reseq_tag}_R{read_number}.fastq.gz"

        else:
            print(f"No match found for '{filename}'")

        return reseq_filename

    def rename_file(old_filename, new_filename):
        try:
            os.rename(old_filename, new_filename)
            # print(f"File '{old_filename}' renamed to '{new_filename}'")
        except Exception as e:
            print(f"Error renaming file: {e}")

    def get_reseq_tag(filename, global_log_file_path):
        # Initialize reseq_nr to 1
        reseq_nr = 1

        # Check if the global log file exists
        if os.path.exists(global_log_file_path):
            try:
                # Open the global log file for reading
                with open(global_log_file_path, "r", newline="") as csvfile:
                    # Read the CSV file
                    reader = csv.DictReader(csvfile)

                    # Count the occurrences of the filename in the "filename" column
                    for row in reader:
                        if row["filename"] == filename:
                            reseq_nr += 1
            except FileNotFoundError:
                print(f"Global log file '{global_log_file_path}' not found.")
        else:
            print(f"Global log file '{global_log_file_path}' does not exist.")

        # Construct and return the resequencing tag
        reseq_tag = f"Reseq{reseq_nr}"
        return reseq_tag

    # Set directory paths. Sequence1 is the fastq file from the current run, Sequence2 is the low-read from the previous run.
    old_fastqs_sequence2 = os.path.join(low_reads_folder, "already_concatenated_low_reads")
    old_fastqs_sequence1 = os.path.join(
        working_directory, "zoe_gcloud_old_fastqs_not_needed"
    )  # destination for sequence1 fastqs. low-read fastqs are moved here after resequencing and concatenation
    temp_concat_output_dir = os.path.join(
        working_directory, "zoe_gcloud_temp_output"
    )  # temporary folder to store concatenated fastqs before copying them to fastq

    # Create the output directories if it doesn't exist
    os.makedirs(temp_concat_output_dir, exist_ok=True)
    os.makedirs(old_fastqs_sequence1, exist_ok=True)
    os.makedirs(old_fastqs_sequence2, exist_ok=True)

    # Get the list of files in sequence_1 directory
    file_inventory = os.listdir(folder_path_to_concatenate)

    # Initialize counters for scan
    skipped_files = 0
    concatenated_files = 0

    # Loop through files in fastq directory and check for matching files in low_reads_for_concat directory. Ask user to confirm if he wants to concatenate
    # print(f"The following files will be concatenated:")

    for file in file_inventory:
        if file in os.listdir(low_reads_folder):
            # Compare files
            input_file_1 = os.path.join(folder_path_to_concatenate, file)
            input_file_2 = os.path.join(low_reads_folder, file)
            if filecmp.cmp(input_file_1, input_file_2):
                print("Error: Skipping %s: Files are identical" % file)
                skipped_files += 1
            else:
                # print(file)
                concatenated_files += 1

        else:
            continue

    print(f"{project_ID}: Number of files that will be concatenated: {concatenated_files}")
    # print(f"Number of files that gave an Error as both files are identical: {skipped_files}")
    # print()

    # Variables for logging
    skipped_files = 0
    concatenated_files = 0
    log_entries = []
    current_date = datetime.datetime.now()  # Get the current date and time
    date = current_date.strftime("%Y-%m-%d")  # Format the date as a string (e.g., "YYYY-MM-DD")

    # Define the directory for the global log file
    global_log_directory = os.path.join(low_reads_folder, "log_data")
    # Check if the directory exists, if not, create it
    if not os.path.exists(global_log_directory):
        os.makedirs(global_log_directory)

    # Define the file path for the global log file
    global_log_file_path = os.path.join(global_log_directory, "global_concat_log.csv")

    # Check if the global log file exists, if not, create it
    if not os.path.exists(global_log_file_path):
        # Write the header to the global log file
        with open(global_log_file_path, "w", newline="") as csvfile:
            fieldnames = ["date", "filename", "size_sequence_1", "size_sequence_2", "size_concatenated", "errors"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

    # Loop through files in fastq directory and check for matching files in low_reads_for_concat directory
    for file in os.listdir(folder_path_to_concatenate):
        if file in os.listdir(low_reads_folder):
            # Compare files
            input_file_1 = os.path.join(folder_path_to_concatenate, file)
            input_file_2 = os.path.join(low_reads_folder, file)
            if filecmp.cmp(input_file_1, input_file_2):
                print("Skipping %s: Files are identical" % file)
                skipped_files += 1
            else:
                # Concatenate files
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
                        shutil.move(output_file, os.path.join(folder_path_to_concatenate, file))
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

                # Handle renaming of Zoe reseq files in fastq
                # print(f"renaming concated file: {file}")
                old_filename = file
                reseq_tag = get_reseq_tag(old_filename, global_log_file_path)
                reseq_filename = get_reseq_filename(old_filename, reseq_tag)
                if reseq_filename:
                    old_filename = os.path.join(folder_path_to_concatenate, file)
                    new_filename = os.path.join(folder_path_to_concatenate, reseq_filename)
                    rename_file(old_filename, new_filename)

        else:
            continue

    print(f"{project_ID}: concatenation completed.")

    # Append log entries to the global CSV logfile
    with open(global_log_file_path, "a", newline="") as csvfile:  # Use 'a' for append mode
        fieldnames = ["date", "filename", "size_sequence_1", "size_sequence_2", "size_concatenated", "errors"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writerows(log_entries)

    # Add log entries to the local CSV logfile
    log_file_path = os.path.join(working_directory, "zoe_gcloud_concat_log.csv")
    with open(log_file_path, "a", newline="") as csvfile:
        fieldnames = ["date", "filename", "size_sequence_1", "size_sequence_2", "size_concatenated", "errors"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(log_entries)


def rename_zoe_fastqs(folder_path_to_rename, project_ID):
    # List all files in the folder
    files = os.listdir(folder_path_to_rename)

    # Initiate counters
    files_to_be_renamed = 0
    files_already_renamed = 0
    files_renamed = 0
    files_that_could_not_be_renamed = 0

    # Iterate through the files and check if they start with any of the strings in projects_to_be_renamed
    for filename in files:
        old_filename = filename
        new_filename = re.sub(r"_S\d+_L00\d(.*)_001", r"\1", filename)

        # For all files that are not controls ("Extra"). Rename them if the filename starts with one of the project IDs.
        if filename.startswith(project_ID):
            files_to_be_renamed += 1

            # check for already renamed filenames
            if old_filename == new_filename:
                files_already_renamed += 1

            else:
                try:
                    # Rename rawdata
                    os.system("rename -n 's/_S\d+_L00\d(.*)_001/$1/g' {}".format(filename))
                    os.system("rename 's/_S\d+_L00\d(.*)_001/$1/g' {}".format(filename))

                    files_renamed += 1
                except:
                    files_that_could_not_be_renamed += 1

    # check numbers
    if files_to_be_renamed == files_renamed + files_already_renamed:
        success = True
    else:
        success = False

    return success


if __name__ == "__main__":

    # define folder that contains previous low-reads for concatenation
    low_reads_for_concat = "/media/share/novaseq01/Output/sequencing_data_for_upload/zoe_gcloud_low_reads_for_concat"

    # get working directory
    script_directory = os.getcwd()

    # Set exports directory
    exported_csv_directory = os.path.join(script_directory, "sample_information_exports")

    # extract zoe_project_ids from input_file
    input_file = "project_info.csv"
    zoe_project_ids = get_zoe_project_ids(input_file)
    print("ZOE Project IDs:", zoe_project_ids)
    print()
    # create list of zoe gcloud foldernames
    zoe_gcloud_foldernames = []
    for project_id in zoe_project_ids:
        gcloud_folder_name = f"{project_id}_gcloud"
        zoe_gcloud_foldernames.append(gcloud_folder_name)

    # iterate over the folders in the working directory to count total_number_of_files
    total_number_of_files = get_number_of_files(script_directory, zoe_gcloud_foldernames)
    # calculate the total number of samples
    total_number_of_samples = int(total_number_of_files / 2)
    # print the total number of samples
    print("Total number of zoe samples that will be prepared:", total_number_of_samples)
    print()

    # iterate over folders in the working directory and prepare zoe fastq files
    for folder in os.listdir(script_directory):
        folder_path = os.path.join(script_directory, folder)
        if os.path.isdir(folder_path):
            folder_name = os.path.basename(folder_path)
            if folder_name in zoe_gcloud_foldernames:
                os.chdir(folder_path)
                print(f"Running script in folder {folder_name}:")
                project_ID = folder_name.replace("_gcloud", "")

                # rename fastq files in folder_path
                success = rename_zoe_fastqs(folder_path, project_ID)
                if success == True:
                    print(f"{project_ID}: renaming completed.")
                else:
                    print(f"{project_ID}: FATAL ERROR: Some files could not be renamed! Terminating Script.")
                    sys.exit(1)  # Terminate script with exit code 1 for failure

                # concatenate fastqs in folder_path
                concatenate_zoe_fastqs(folder_path, low_reads_for_concat, script_directory)

                # find exported.csv to rename and sort fastq-files
                exported_csv_path = find_exported_csv(project_ID, exported_csv_directory)
                if exported_csv_path:
                    # get ZOE-ID - Zymo-ID map
                    id_map = get_id_map(exported_csv_path)

                    # rename files using id_map and get .md5
                    file_list = os.listdir(".")
                    rename_and_get_md5(file_list, id_map)

                    # sort .fastq.gz files and .md5 files according to zoe-id
                    file_list = os.listdir(".")
                    sort_processed_files(file_list, id_map)
                    print(f"{project_ID}: md5-generation, renaming to STOOL-ID and sorting completed.")
                else:
                    print(f"{project_ID}: No exported.csv found! Can't rename and sort fastq files.")

    print("\n\nI'm on a rollercoaster that only goes up! - JG")
