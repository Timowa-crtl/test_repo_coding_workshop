import re
import os
import argparse
import hashlib
import time
import csv

# Version 1.41: Script calculates md5 checksums for ZOE Projects and writes them into '{project}_SampleInformationForm_{run_date}.csv' in '{project}_metadata' folder


def calculate_md5(sample):
    try:
        line = []
        project = sample.split("_")[0]

        start_time = time.time()

        # Calculate the MD5 hash sums for R1 and R2 files
        md5R1 = hashlib.md5()
        with open("%s_R1.fastq.gz" % sample, "rb") as file:
            for chunk in iter(lambda: file.read(BUFFER_SIZE), b""):
                md5R1.update(chunk)
        md5sumR1 = md5R1.hexdigest()

        md5R2 = hashlib.md5()
        with open("%s_R2.fastq.gz" % sample, "rb") as file:
            for chunk in iter(lambda: file.read(BUFFER_SIZE), b""):
                md5R2.update(chunk)
        md5sumR2 = md5R2.hexdigest()

        elapsed_time = time.time() - start_time

        # Append the sample information to the line
        line.append(sample)  # ZymoID
        line.append("")  # Empty string for SampleID
        line.append("s3://zymo-zoe/fastq/{}/{}/{}_R1.fastq.gz".format(project, run_date, sample))
        line.append("s3://zymo-zoe/fastq/{}/{}/{}_R2.fastq.gz".format(project, run_date, sample))
        line.append(md5sumR1)
        line.append(md5sumR2)

        # print(f"{sample} took {elapsed_time:.2f} seconds to calculate")

        return line
    except Exception as e:
        print(f"Error processing {sample}: {str(e)}. Moving on to the next sample.")


if __name__ == "__main__":
    # Read the project_info.csv file and extract project IDs
    input_file = "project_info.csv"
    print(f"Input file: {input_file}")
    working_directory = os.getcwd()

    project_ids_with_md5 = set()
    with open(input_file, "r") as csvfile:
        reader = csv.DictReader(csvfile)

        # Ensure project_id and md5? columns exist
        fieldnames = reader.fieldnames
        project_id_field = next((field for field in fieldnames if field.lower() == "project_id"), None)
        md5_field = next((field for field in fieldnames if field.lower() == "md5?"), None)

        if project_id_field is None:
            raise ValueError("Column 'project_ID' not found in project_info.csv")
        if md5_field is None:
            raise ValueError("Column 'md5?' not found in project_info.csv")

        # Initialize run_date and process rows
        run_date = None
        for row in reader:
            if run_date is None:
                run_date = row["run_date"]

            project_id = row[project_id_field]
            md5_check = row[md5_field]

            if md5_check.lower() == "yes":
                project_ids_with_md5.add(project_id)

    print(f"Run_date: {run_date}")
    print()

    # Iterate over the folders in the working directory to count total_number_of_files
    # Initialize the total number of files
    total_number_of_files = 0

    # Iterate over the folders in the working directory
    for folder in os.listdir(working_directory):
        folder_path = os.path.join(working_directory, folder)

        # Check if it's a directory and its name is in project_ids_with_md5
        if os.path.isdir(folder_path) and os.path.basename(folder_path) in project_ids_with_md5:
            # Change the current working directory to the folder
            os.chdir(folder_path)

            # Count the .gz files in the current folder
            gz_files = [file for file in os.listdir() if file.endswith(".gz")]
            num_files = len(gz_files)

            # Update the total number of files
            total_number_of_files += num_files

    # Reset the working directory to the original one if necessary
    os.chdir(working_directory)

    # calculate the total number of samples
    total_number_of_samples = int(total_number_of_files / 2)

    # Print the total number of samples
    print("Total number of samples that will be hashed:", total_number_of_samples)
    print()

    # Initialize the total number of hashed samples
    already_hashed_samples = 0

    # Iterate over the folders in the working directory and run the script in matching folders
    for folder in os.listdir(working_directory):
        folder_path = os.path.join(working_directory, folder)
        if os.path.isdir(folder_path):
            folder_name = os.path.basename(folder_path)
            if folder_name in project_ids_with_md5:
                os.chdir(folder_path)
                print(f"Running script in folder {folder_name}")

                # Command Line Arguments
                parser = argparse.ArgumentParser(description="This script uses python3. Make sure you have the correct version installed.")

                args = parser.parse_args()

                files = os.listdir(".")

                samples = []
                for file in files:
                    if file.endswith("gz"):
                        sample = file.split("_R")[0]
                        samples.append(sample)
                samples = sorted(set(samples), key=lambda x: int(re.findall(r"\d+$", x)[0]))  # Sort the samples based on the numerical value
                # print(samples)
                print("Give me some time. I will now do my calculations.")

                start_time_all = time.time()

                csv_rows = []  # Store the rows for CSV output

                BUFFER_SIZE = 32768  # Adjust the buffer size according to your needs

                results = []
                for sample in samples:
                    result = calculate_md5(sample)
                    if result is not None:
                        results.append(result)
                        already_hashed_samples += 1
                        print(f"{already_hashed_samples}/{total_number_of_samples} total samples have been processed")

                csv_rows.extend(results)  # Append the results to the csv_rows list

                # Extract the project name from the first sample
                project = "not_found"
                project = samples[0].split("_")[0]

                # Write the rows to the CSV file
                output_directory = os.path.abspath("..")  # Get the absolute path of the parent directory
                folder_name = f"{project}_metadata"
                output_directory = os.path.join(output_directory, folder_name)

                # Create the folder if it doesn't exist
                if not os.path.exists(output_directory):
                    os.makedirs(output_directory)

                output_filename = os.path.join(output_directory, f"{project}_SampleInformationForm_{run_date}.csv")

                with open(output_filename, "w", newline="") as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(["ZymoID", "SampleID", "Read1 Path", "Read2 Path", "Read1 md5", "Read2 md5"])  # Write the header row
                    writer.writerows(csv_rows)  # Write the data rows

                # Calculate the number of samples
                num_samples = len(samples)

                elapsed_time_all = time.time() - start_time_all
                elapsed_time_minutes = elapsed_time_all / 60

                print(f"Done. It took me {elapsed_time_minutes:.2f} minutes to calculate {num_samples} samples.")
                print()
                print("You should now have the output CSV file.")
                print(f"File name: {output_filename}")
                print()

                # Exit the folder to continue iterating over other folders
                os.chdir(working_directory)
