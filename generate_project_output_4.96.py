import csv
from customer_settings import get_upload_priority, generate_random_string, get_expected_objects
import os
import random
import string
import sys

# 4.46  added collumn for bucket_region
# 4.49  added exception for Alba Health: expected_objects += 2 because customer needs fastqs for 1 ExtraN for each project
# 4.50  deleted dead code in generate_project_output_info() and standardized metadata entries for ZRE_Standard, Zotal and Ventra
# 4.51  changed localpath_metadata to {project_ID}_rawdatalinks for ZRE_Standard, Zotal and Ventra
# 4.52  changed uploadpath_metadata_2 to for Alba Health
# 4.53  fixxed wrong localpath_metadata for Alba Health and ZOE
# 4.90  ZOE will now be uploaded to Google Cloud using upload_zoe_projects_to_gcloud.py.
#       Changed generate_project_output.py so that AWS-Upload for ZOE projects only is done as a backup
# 4.91  not uploading ZOE metadatafile anymore
# 4.98

# CONSTANTS
INPUT_FILE = "project_info.csv"
OUTPUT_FILE = "project_output_info.csv"

HEADER_INPUT_FILE = [
    "#",
    "project_ID",
    "#samples",
    "customer",
    "run_date",
    "sequencing_ID",
    "renaming?",
    "metagenomics_BI?",
    "metatranscriptomics_BI?",
    "epigenetics_BI?",
    "total_rna_BI?",
]

HEADER_OUTPUT_FILE = [
    "upload_priority",
    "project_ID",
    "#samples",
    "customer",
    "run_date",
    "sequencing_ID",
    "renaming?",
    "metagenomics_BI?",
    "metatranscriptomics_BI?",
    "epigenetics_BI?",
    "total_rna_BI?",
    "random_string",
    "local_path",
    "local_path_metadata",
    "expected_objects",
    "upload path fastq 1",
    "upload path fastq 2",
    "upload path metadata 1",
    "upload path metadata 2",
    "upload_command_fastq_1",
    "upload_command_fastq_2",
    "upload_command_metadata_1",
    "upload_command_metadata_2",
    "uploadpath_rawdatalinks",
    "bucket_region",
]


def validate_header(input_file, header_input_file):
    with open(input_file, "r") as file:
        reader = csv.DictReader(file)
        # Extract the actual header from the file
        actual_header = reader.fieldnames

        # Check if the headers match
        if actual_header != header_input_file:
            missing_columns = set(header_input_file) - set(actual_header)
            extra_columns = set(actual_header) - set(header_input_file)

            if missing_columns:
                print(f"Missing columns: {missing_columns}")
            if extra_columns:
                print(f"Extra columns: {extra_columns}")
            sys.exit(f"Terminating script due to invalid file '{input_file}'")
        else:
            print("Input file validation passed.")


def generate_project_output_info(input_file, output_file, header_output_file):

    with open(input_file, "r") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    if len(rows) > 0:
        first_row = rows[0]
        sequencing_id_global = first_row.get("sequencing_ID", "sequencing_ID not defined")
        run_date_global = first_row.get("run_date", "run_date not defined")
        print(f"sequencing_ID: {sequencing_id_global}")
        print(f"run_date: {run_date_global}")

        # adding additional mock-project to handle upload_complete file
        rows.append(
            {
                "#": "",
                "project_ID": "microbiomics_upload_complete",
                "#samples": 1,
                "customer": "microbiomics_upload_complete",
                "run_date": run_date_global,
                "sequencing_ID": sequencing_id_global,
                "renaming?": "no",
                "metagenomics_BI?": "no",
                "metatranscriptomics_BI?": "no",
                "epigenetics_BI?": "no",
                "total_rna_BI?": "no",
            }
        )
        # adding additional mock-project to handle upload_complete file
        rows.append(
            {
                "#": "",
                "project_ID": "epigenetics_upload_complete",
                "#samples": 1,
                "customer": "epigenetics_upload_complete",
                "run_date": run_date_global,
                "sequencing_ID": sequencing_id_global,
                "renaming?": "no",
                "metagenomics_BI?": "no",
                "metatranscriptomics_BI?": "no",
                "epigenetics_BI?": "no",
                "total_rna_BI?": "no",
            }
        )

    output_rows = []
    row_counter = 0
    for row in rows:
        # check if all run_dates and sequencing_ids are identical
        row_counter += 1
        run_date = row["run_date"]
        sequencing_id = row["sequencing_ID"]
        if row["sequencing_ID"] != sequencing_id_global or row["run_date"] != run_date_global:
            print(f"Error detected on row {row_counter}! Not all sequencing_ids and/or run_dates are identical.")
            user_input = input("Do you want to continue anyway? (yes/no): ").lower()
            if user_input != "yes":
                print("Exiting script. No output was generated due to faulty project_info.csv")
                return

        # default outputs
        project_id = row["project_ID"]
        samples = row["#samples"]
        customer = row["customer"]
        run_date = run_date_global
        sequencing_id = sequencing_id_global
        renaming = row["renaming?"]
        metagenomics_BI = row["metagenomics_BI?"]
        metatranscriptomics_BI = row["metatranscriptomics_BI?"]
        epigenetics_BI = row["epigenetics_BI?"]
        total_rna_BI = row["total_rna_BI?"]

        expected_objects = get_expected_objects(customer, samples)
        upload_priority = get_upload_priority(customer)
        random_string = generate_random_string(16)
        local_path = f"/media/share/novaseq01/Output/sequencing_data_for_upload/{sequencing_id}/{project_id}"
        local_path_metadata = f"{local_path}_rawdatalinks"  # default is "_rawdatalinks"  because Alba Health and ZOE are the exception with "_metadata"
        upload_path_fastq1 = "empty"  # main upload, prioritized
        upload_path_fastq2 = "empty"  # backup upload
        upload_path_metadata1 = "empty"  # main upload, prioritized
        upload_path_metadata2 = "empty"  # backup upload
        upload_command_fastq1 = "empty"  # main upload, prioritized
        upload_command_fastq2 = "empty"  # backup upload
        upload_command_metadata1 = "empty"  # main upload, prioritized
        upload_command_metadata2 = "empty"  # backup upload
        uploadpath_rawdatalinks = "empty"  # this path is used for generation of rawdatalinks.csv, used for sharing links with customer
        bucket_region = "eu-central-1"  # aws-region of the bucket

        # changes based on name of customer
        if customer in ["ExtraN", "ExtraNZRE"]:
            upload_path_fastq2 = f"epiquest-zre/{project_id}/rawdata/{run_date}/{random_string}/"
            upload_command_fastq2 = f"time aws s3 cp {local_path} s3://{upload_path_fastq2} --recursive"
            uploadpath_rawdatalinks = upload_path_fastq2

        elif customer in ["ExtraP", "ExtraPZRE"]:
            upload_path_fastq1 = f"zymo-zre-sequencing/{sequencing_id}/"
            upload_path_fastq2 = f"epiquest-zre/{project_id}/rawdata/{run_date}/{random_string}/"
            upload_command_fastq1 = f"time aws s3 cp {local_path} s3://{upload_path_fastq1} --recursive"
            upload_command_fastq2 = f"time aws s3 cp {local_path} s3://{upload_path_fastq2} --recursive"
            uploadpath_rawdatalinks = upload_path_fastq2

        elif customer == "ZOE":
            local_path_metadata = f"{local_path}_metadata"
            # upload_path_fastq1 = f"zymo-zoe/fastq/{project_id}/{run_date}/"                                                       # is now uploaded to gcloud using upload_zoe_projects_to_gcloud.py
            upload_path_fastq2 = f"epiquest-zre/zoe_projects/{project_id}/rawdata/{run_date}/{random_string}/"  # data is uploaded to epiquest-zre for backup
            # upload_path_metadata1 = "zymo-zoe/metadata/"                                                                          # is now uploaded to gcloud using upload_zoe_projects_to_gcloud.py
            # upload_path_metadata2 = f"epiquest-zre/zoe_projects/{project_id}/metadata/{run_date}/{random_string}/"                # not needed due to gcloud upload
            upload_command_fastq1 = f"time aws s3 cp {local_path} s3://zymo-zoe/fastq/{project_id}/{run_date}/ --recursive"
            upload_command_fastq2 = f"time aws s3 cp {local_path} s3://{upload_path_fastq2} --recursive"
            upload_command_metadata1 = f"time aws s3 cp {local_path_metadata} s3://zymo-zoe/metadata/ --recursive"
            upload_command_metadata2 = f"time aws s3 cp {local_path_metadata} s3://epiquest-zre/zoe_projects/{project_id}/rawdata/{run_date}/{random_string}/ --recursive"
            uploadpath_rawdatalinks = upload_path_fastq2

        elif customer in ["ZRE_default_no_BI", "ZRE_Standard_no_BI", "Zotal"]:
            local_path_metadata = f"{local_path}_rawdatalinks"
            upload_path_fastq2 = f"epiquest-zre/{project_id}/rawdata/{run_date}/{random_string}/"
            upload_path_metadata2 = f"epiquest-zre/{project_id}/metadata/{run_date}/{random_string}/"
            upload_command_fastq2 = f"time aws s3 cp {local_path} s3://{upload_path_fastq2} --recursive --acl public-read-write"
            upload_command_metadata2 = f"time aws s3 cp {local_path_metadata} s3://{upload_path_metadata2} --recursive --acl public-read-write"
            uploadpath_rawdatalinks = upload_path_fastq2

        elif customer in ["ZRE_Standard_with_BI", "ZRE_default_with_BI", "ZRE_default_metagenomics_BI", "ZRE_default_metatranscriptomics_BI", "ZRE_default_total_rna_BI", "Ventra"]:
            local_path_metadata = f"{local_path}_rawdatalinks"
            upload_path_fastq1 = f"zymo-zre-sequencing/{sequencing_id}/"
            upload_path_fastq2 = f"epiquest-zre/{project_id}/rawdata/{run_date}/{random_string}/"
            upload_path_metadata2 = f"epiquest-zre/{project_id}/metadata/{run_date}/{random_string}/"
            upload_command_fastq1 = f"time aws s3 cp {local_path} s3://{upload_path_fastq1} --recursive"
            upload_command_fastq2 = f"time aws s3 cp {local_path} s3://{upload_path_fastq2} --recursive --acl public-read-write"
            upload_command_metadata2 = f"time aws s3 cp {local_path_metadata} s3://{upload_path_metadata2} --recursive --acl public-read-write"
            uploadpath_rawdatalinks = upload_path_fastq2

        elif customer in ["ZRE_default_epigenetics_BI"]:
            local_path_metadata = f"{local_path}_rawdatalinks"
            upload_path_fastq1 = "empty"
            upload_path_fastq2 = f"epiquest-zre/{project_id}/rawdata/{run_date}/{random_string}/"
            upload_path_metadata2 = f"epiquest-zre/{project_id}/metadata/{run_date}/{random_string}/"
            upload_command_fastq1 = "empty"
            upload_command_fastq2 = f"time aws s3 cp {local_path} s3://{upload_path_fastq2} --recursive --acl public-read-write"
            upload_command_metadata2 = f"time aws s3 cp {local_path_metadata} s3://{upload_path_metadata2} --recursive --acl public-read-write"
            uploadpath_rawdatalinks = upload_path_fastq2

        elif customer in ["ZRC", "ZRC_default", "ZRC_Microbiomics", "ZRC_Epigenetics"]:
            upload_path_fastq1 = f"zymo-zre-sequencing/{sequencing_id}/"
            upload_path_fastq2 = "empty"
            upload_command_fastq1 = f"time aws s3 cp {local_path} s3://{upload_path_fastq1} --recursive"
            upload_command_fastq2 = "empty"
            uploadpath_rawdatalinks = upload_path_fastq1
            bucket_region = "us-east-1"

        elif customer == "Alba Health":
            local_path_metadata = f"{local_path}_metadata"
            upload_path_fastq1 = f"s3-external-zymo/rawdata/{run_date}_{project_id}/"
            upload_path_fastq2 = "empty"
            upload_command_fastq1 = f"time aws s3 cp {local_path} s3://s3-external-zymo/rawdata/{run_date}_{project_id}/ --recursive"
            upload_command_fastq2 = "empty"
            upload_path_metadata1 = f"s3-external-zymo/metadata/{run_date}_{project_id}/"
            upload_path_metadata2 = f"epiquest-zre/{project_id}/metadata/{run_date}/{random_string}/"
            upload_command_metadata1 = f"time aws s3 cp {local_path_metadata} s3://{upload_path_metadata1} --recursive"
            upload_command_metadata2 = f"time aws s3 cp {local_path_metadata} s3://{upload_path_metadata2} --recursive --acl public-read-write"
            uploadpath_rawdatalinks = upload_path_fastq1
            bucket_region = "eu-north-1"

        elif customer == "microbiomics_upload_complete":
            project_id = "microbiomics_upload_complete"
            samples = 1
            run_date = run_date_global
            sequencing_id = sequencing_id_global
            renaming = "no"
            metagenomics_BI = "no"
            metatranscriptomics_BI = "no"
            epigenetics_BI = "no"
            total_rna_BI = "no"
            local_path = f"/media/share/novaseq01/Output/sequencing_data_for_upload/{sequencing_id_global}/upload_complete_files/microbiomics_upload_complete/"
            upload_path_fastq1 = f"zymo-zre-sequencing/{sequencing_id_global}/"
            upload_priority = get_upload_priority(customer)
            random_string = generate_random_string(16)
            upload_command_fastq1 = f"time aws s3 cp {local_path} s3://{upload_path_fastq1} --recursive"
            bucket_region = "us-east-1"

        elif customer == "epigenetics_upload_complete":
            project_id = "epigenetics_upload_complete"
            samples = 1
            run_date = run_date_global
            sequencing_id = sequencing_id_global
            renaming = "no"
            metagenomics_BI = "no"
            metatranscriptomics_BI = "no"
            epigenetics_BI = "no"
            total_rna_BI = "no"
            local_path = f"/media/share/novaseq01/Output/sequencing_data_for_upload/{sequencing_id_global}/upload_complete_files/epigenetics_upload_complete/"
            upload_path_fastq1 = f"zymo-zre-sequencing/{sequencing_id_global}/"
            upload_priority = get_upload_priority(customer)
            random_string = generate_random_string(16)
            upload_command_fastq1 = f"time aws s3 cp {local_path} s3://{upload_path_fastq1} --recursive"
            bucket_region = "us-east-1"

        else:
            print(f"WARNING: Customer '{customer}' for project-ID '{project_id}' is not defined. Data is not uploaded. Check 'customer_info' in 'Upload_Data_Info.xlsm' for available customers.")

        output_row = {
            "upload_priority": upload_priority,
            "project_ID": project_id,
            "#samples": samples,
            "customer": customer,
            "run_date": run_date,
            "sequencing_ID": sequencing_id,
            "renaming?": renaming,
            "metagenomics_BI?": metagenomics_BI,
            "metatranscriptomics_BI?": metatranscriptomics_BI,
            "epigenetics_BI?": epigenetics_BI,
            "total_rna_BI?": total_rna_BI,
            "random_string": random_string,
            "local_path": local_path,
            "local_path_metadata": local_path_metadata,
            "expected_objects": expected_objects,
            "upload path fastq 1": upload_path_fastq1,
            "upload path fastq 2": upload_path_fastq2,
            "upload path metadata 1": upload_path_metadata1,
            "upload path metadata 2": upload_path_metadata2,
            "upload_command_fastq_1": upload_command_fastq1,
            "upload_command_fastq_2": upload_command_fastq2,
            "upload_command_metadata_1": upload_command_metadata1,
            "upload_command_metadata_2": upload_command_metadata2,
            "uploadpath_rawdatalinks": uploadpath_rawdatalinks,
            "bucket_region": bucket_region,
        }

        output_rows.append(output_row)

    with open(output_file, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=header_output_file)
        writer.writeheader()
        writer.writerows(output_rows)

    if os.path.exists(output_file):
        print(f"\n'{output_file}' generated successfully.")
    else:
        print(f"\nERROR: '{output_file}' does not exist.")


if __name__ == "__main__":

    validate_header(INPUT_FILE, HEADER_INPUT_FILE)
    generate_project_output_info(INPUT_FILE, OUTPUT_FILE, HEADER_OUTPUT_FILE)
