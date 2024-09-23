import re
import os
import csv

# 1.64: Added exception for Alba Health. Outputfiles use S3 URLs and are copied to {project_ID}_metadata. This can easily be implemented for other customers.
# 1.65: Using Https Links for Alba Health again.
# 1.66: Skipping ZRC Projects due to naming issues (letters within sample id nr)
# 1.67: Skipping ZOE Projects, too. Creating Alba files directly in project_ID_metadata to not have it twice.
# 1.68: Adding Error messages if no matching folder is found for customers that require rawdatalinks
# 1.69: Adding Error handling for sorting samples-list. Removed dead code.


def generate_rows_https(sample):
    try:
        line = []
        project = sample.split("_")[0]

        # Append the sample information to the line
        line.append(sample)  # ZymoID
        line.append("")  # Empty string for SampleID
        line.append("https://{}.s3.{}.amazonaws.com/{}{}_R1.fastq.gz".format(bucket, bucket_region, object_path, sample))
        line.append("https://{}.s3.{}.amazonaws.com/{}{}_R2.fastq.gz".format(bucket, bucket_region, object_path, sample))

        return line
    except Exception as e:
        print(f"Error processing {sample}: {str(e)}. Moving on to the next sample.")


def generate_rows_s3_url(sample):
    try:
        line = []
        project = sample.split("_")[0]

        # Append the sample information to the line
        line.append(sample)  # ZymoID
        line.append("")  # Empty string for SampleID
        line.append("s3://{}/{}{}_R1.fastq.gz".format(bucket, object_path, sample))
        line.append("s3://{}/{}{}_R2.fastq.gz".format(bucket, object_path, sample))

        return line
    except Exception as e:
        print(f"Error processing {sample}: {str(e)}. Moving on to the next sample.")


if __name__ == "__main__":
    # Read the project_info.csv file and extract project IDs
    input_file = "project_output_info.csv"
    working_directory = os.getcwd()

    project_data = []
    with open(input_file, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)

        # Access the first row (index 0) directly
        first_row = rows[0]
        run_date = first_row.get("run_date", "run_date not defined")

        for row in rows:
            bucket_region = "eu-central-1"  # set standard bucket-region
            customer = row["customer"]
            project_id = row["project_ID"]

            try:
                uploadpath_rawdatalinks = row["uploadpath_rawdatalinks"]
            except:
                print(f"uploadpath_rawdatalinks not found.")

            if customer == "Alba Health":
                # Split the uploadpath_rawdatalinks-string using "/" as the delimiter
                parts = uploadpath_rawdatalinks.split("/")
                # Define the variables
                bucket = parts[0]
                object_path = "/".join(parts[1:])  # Join the remaining parts to create the object_path
                bucket_region = "eu-north-1"

                if not project_id.startswith("Extra"):
                    project_data.append(
                        {"customer": customer, "project_ID": project_id, "bucket": bucket, "object_path": object_path, "bucket_region": bucket_region}
                    )

            # exclude projects from customers that dont need rawdatalinks
            elif (
                customer.startswith("ZRC")
                or customer.startswith("Extra")
                or customer.startswith("microbiomics_upload_complete")
                or customer.startswith("epigenetics_upload_complete")
                or customer == "ZOE"
            ):
                pass

            else:
                # Split the uploadpath_rawdatalinks-string using "/" as the delimiter
                parts = uploadpath_rawdatalinks.split("/")
                # Define the variables
                bucket = parts[0]
                object_path = "/".join(parts[1:])  # Join the remaining parts to create the object_path
                bucket_region = "eu-central-1"

                if not project_id.startswith("Extra"):
                    project_data.append(
                        {"customer": customer, "project_ID": project_id, "bucket": bucket, "object_path": object_path, "bucket_region": bucket_region}
                    )

    # initialize dict for files that have been created
    created_files_dict = {}

    # Iterate over the folders in the working directory and run the script in matching folders
    # Iterate over project_data first
    for data in project_data:
        project_ID = data["project_ID"]
        customer = data["customer"]
        bucket = data["bucket"]
        object_path = data["object_path"]
        upload_path_fastq = f"{bucket}/{object_path}"
        bucket_region = data["bucket_region"]

        # Check if the project_ID folder exists in the working directory
        folder_path = os.path.join(working_directory, project_ID)
        if os.path.isdir(folder_path):
            os.chdir(folder_path)  # Change to the project_ID folder

            # Add customer and project_ID to created_files_dict
            if customer not in created_files_dict:
                created_files_dict[customer] = [project_ID]
            else:
                created_files_dict[customer].append(project_ID)

            files = os.listdir(".")

            samples = []
            for file in files:
                if file.endswith("gz"):
                    sample = file.split("_R")[0]
                    samples.append(sample)

            # Sort the samples based on the numerical value
            try:
                samples = sorted(set(samples), key=lambda x: int(re.findall(r"\d+$", x)[0]))
            except ValueError:
                # Handle the case where sorting fails due to non-numeric characters in the sample names
                print(f"Error sorting samples for project {project_ID}: Sample names contain non-numeric characters.")
                continue  # Move to the next project_ID
            except Exception as e:
                # Catch-all for other unexpected errors
                print(f"Unexpected error processing project {project_ID}: {e}")
                continue  # Move to the next project_ID

            csv_rows = []  # Store the rows for CSV output

            # Extract the project name from the first sample
            project = "not_found"
            project = samples[0].split("_")[0]
            if project != project_ID:
                print(
                    f"{customer}({project_ID}): ERROR!!! Project ID of sample filename ({project}) does not match folder name ({project_ID})! Did you get something mixed up?!\n"
                )
            results = []

            # Handle special requirements for customer Alba Health
            if customer == "Alba Health":

                for sample in samples:
                    result = generate_rows_https(sample)
                    if result is not None:
                        results.append(result)

                # Append the results to the csv_rows list
                csv_rows.extend(results)

                # define output directory (is different for Alba)
                parent_directory = os.path.abspath("..")  # Get the absolute path of the parent directory
                folder_name = f"{project_ID}_metadata"
                output_directory = os.path.join(parent_directory, folder_name)

                # Create the folder if it doesn't exist
                if not os.path.exists(output_directory):
                    os.makedirs(output_directory)

                output_filename = os.path.join(output_directory, f"{project_ID}_Rawdatalinks_{run_date}.csv")

                with open(output_filename, "w", newline="") as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(["sample_id", "customer_label", "Read1 Download", "Read2 Download"])  # Write the header row
                    writer.writerows(csv_rows)  # Write the data rows

                # Exit the folder to continue iterating over other folders
                os.chdir(working_directory)

            # Handle normal customers
            else:
                for sample in samples:
                    result = generate_rows_https(sample)
                    if result is not None:
                        results.append(result)

                csv_rows.extend(results)  # Append the results to the csv_rows list

                # Write the rows to the CSV file
                parent_directory = os.path.abspath("..")  # Get the absolute path of the parent directory
                folder_name = "Rawdatalinks"
                output_directory = os.path.join(parent_directory, folder_name)

                # Create the folder if it doesn't exist
                if not os.path.exists(output_directory):
                    os.makedirs(output_directory)

                output_filename = os.path.join(output_directory, f"{project_ID}_Rawdatalinks_{run_date}.csv")

                with open(output_filename, "w", newline="") as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(["sample_id", "customer_label", "Read1 Download", "Read2 Download"])  # Write the header row
                    writer.writerows(csv_rows)  # Write the data rows

                # Exit the folder to continue iterating over other folders
                os.chdir(working_directory)
        else:
            print(f"No matching folder found for {customer}: {project_ID}")

    # print results
    if not created_files_dict:
        print("No rawdatalinks had to be created.")
    else:
        for customer, project_ids in created_files_dict.items():
            print()
            print(f"For customer '{customer}' files were created for the following project IDs:")
            for project_id in project_ids:
                print(f"  - {project_id}")
