import csv
import os
import sys
import getpass
import subprocess

# 1.20 Copies low_reads specified by input-file from uploadfolder to 1 destination
# 2.00 Adding an additional location for zoe low-reads were zoe low-reads are copied to additionally


def get_zoe_project_ids(input_file):
    zoe_projects = {}
    with open(input_file, "r") as csvfile:
        reader = csv.DictReader(csvfile)

        # Ensure the expected columns exist
        expected_columns = ["project_ID", "#samples", "customer"]
        for column in expected_columns:
            if column not in reader.fieldnames:
                raise ValueError(f"Column '{column}' not found")

        # Process rows
        for row in reader:
            # Get the values of the required columns
            project_id = row.get("project_ID")
            customer = row.get("customer")
            samples = row.get("#samples")

            # Check if both values are not empty and customer is "zoe"
            if project_id and customer and customer.lower() == "zoe":
                zoe_projects[project_id] = samples

    return zoe_projects


def get_project_id(filename):
    project_id = filename.split("_")[0]
    return project_id


if __name__ == "__main__":

    # define paths
    project_info = "project_info.csv"
    low_reads_info = "low_reads_info.csv"
    destination_aws_workflow = "/media/share/novaseq01/Output/sequencing_data_for_upload/low_reads_for_concat/"
    destination_gcloud_workflow = "/media/share/novaseq01/Output/sequencing_data_for_upload/zoe_gcloud_low_reads_for_concat/"

    # Create directories if they don't exist
    for directory in [destination_aws_workflow, destination_gcloud_workflow]:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Directory {directory} created.")

    # extract zoe_project_ids
    zoe_project_ids = get_zoe_project_ids(project_info)

    # read the CSV file and extract filepaths
    files_to_copy = []
    with open(low_reads_info, "r") as csvfile:
        csvreader = csv.DictReader(csvfile)
        for row in csvreader:
            r1_path = row["localpath_R1"]
            r2_path = row["localpath_R2"]
            files_to_copy.append(r1_path)
            files_to_copy.append(r2_path)

    # copy the files
    copied_count = 0  # Initialize the counter
    for source_file in files_to_copy:
        try:
            file_name = os.path.basename(source_file)
            destination_path_aws = os.path.join(destination_aws_workflow, file_name)

            # open the source file in binary read mode and the destination file in binary write mode
            with open(source_file, "rb") as src, open(destination_path_aws, "wb") as dest:
                while True:
                    chunk = src.read(1024)  # Copy in 1KB chunks
                    if not chunk:
                        break
                    dest.write(chunk)
            print(f"File '{file_name}' copied successfully.")

            # copy zoe files a second time to destination_gcloud_workflow
            project_id = get_project_id(file_name)
            if project_id in zoe_project_ids:
                destination_path_gcloud = os.path.join(destination_gcloud_workflow, file_name)
                # open the source file in binary read mode and the destination file in binary write mode
                with open(source_file, "rb") as src, open(destination_path_gcloud, "wb") as dest:
                    while True:
                        chunk = src.read(1024)  # copy in 1KB chunks
                        if not chunk:
                            break
                        dest.write(chunk)
                print(f"\tZoe file '{file_name}' copied additionally.")

            copied_count += 1  # increment the counter
        except IOError as e:
            print(f"Error copying file '{source_file}':", e)

    # print the total number of files copied
    print(f"Total files copied: {copied_count}")
