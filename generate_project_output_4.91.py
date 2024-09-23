import csv
import random
import string

# 4.46  added collumn for bucket_region
# 4.49  added exception for Alba Health: expected_objects += 2 because customer needs fastqs for 1 ExtraN for each project
# 4.50  deleted dead code in generate_project_output_info() and standardized metadata entries for ZRE_Standard, Zotal and Ventra
# 4.51  changed localpath_metadata to {project_ID}_rawdatalinks for ZRE_Standard, Zotal and Ventra
# 4.52  changed uploadpath_metadata_2 to for Alba Health
# 4.53  fixxed wrong localpath_metadata for Alba Health and ZOE
# 4.90  ZOE will now be uploaded to Google Cloud using upload_zoe_projects_to_gcloud.py.
#       Changed generate_project_output.py so that AWS-Upload for ZOE projects only is done as a backup
# 4.91  Not uploading ZOE metadatafile anymore


def generate_random_string(length):
    letters = string.ascii_uppercase
    return "".join(random.choice(letters) for _ in range(length))


def get_expected_objects(customer, samples):
    if customer == "epigenetics_upload_complete" or customer == "microbiomics_upload_complete":
        expected_objects = int(samples)
    elif customer == "Alba Health":
        expected_objects = int(samples) * 2 + 2
    else:
        expected_objects = int(samples) * 2
    return expected_objects


def get_upload_priority(customer):
    customer_priority_map = {
        "ExtraP": 1,
        "ExtraPZRE": 1,
        "Alba Health": 1,
        "ZRC_Microbiomics": 2,
        "ZRC": 4,
        "ZRE_Standard_with_BI": 5,
        "Ventra": 6,
        "Zotal": 8,
        "microbiomics_upload_complete": 9,  # uploading microbiomics_upload_complete.txt for Jiawei
        "ZRC_Jeffrey": 11,
        "ZRC_Epigenetics": 11,
        "epigenetics_upload_complete": 12,  # uploading epigenetics_upload_complete.txt for Jiawei
        "ZRE_Standard_no_BI": 13,
        "ExtraN": 14,
        "ExtraNZRE": 14,
        "ZOE": 15,
    }

    # Check if the customer is in the map, and return the corresponding priority
    if customer in customer_priority_map:
        upload_priority = customer_priority_map[customer]
    else:
        # Handle the case where the customer is not in the map
        print(f"Customer {customer} has not been assinged an upload priority, yet. Upload priority was set to default value = 5 (high priority)")
        upload_priority = 5

    return upload_priority


def generate_project_output_info(input_file, output_file):

    with open(input_file, "r") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    # Check if there are any rows in the CSV
    if len(rows) > 0:
        # Access the first row (index 0) directly
        first_row = rows[0]
        sequencing_id_global = first_row.get("sequencing_ID", "sequencing_ID not defined")
        run_date_global = first_row.get("run_date", "run_date not defined")
        # print()
        print(f"sequencing_ID: {sequencing_id_global}")
        print(f"run_date: {run_date_global}")
        rows.append(
            {
                "#": "empty",
                "project_ID": "microbiomics_upload_complete",
                "#samples": 1,
                "customer": "microbiomics_upload_complete",
                "run_date": run_date_global,
                "sequencing_ID": sequencing_id_global,
                "renaming?": "no",
                "concat?": "no",
                "BI?": "no",
                "analysis_file?": "no",
                "md5?": "no",
            }
        )
        rows.append(
            {
                "#": "empty",
                "project_ID": "epigenetics_upload_complete",
                "#samples": 1,
                "customer": "epigenetics_upload_complete",
                "run_date": run_date_global,
                "sequencing_ID": sequencing_id_global,
                "renaming?": "no",
                "concat?": "no",
                "BI?": "no",
                "analysis_file?": "no",
                "md5?": "no",
            }
        )

    output_rows = []
    row_counter = 1
    for row in rows:
        # check if all run_dates and sequencing_ids are identical
        row_counter += 1
        run_date = row["run_date"]
        sequencing_id = row["sequencing_ID"]
        # print(f"Row {row_counter}: {run_date} , {sequencing_id} ")
        if row["sequencing_ID"] != sequencing_id_global or row["run_date"] != run_date_global:
            print(f"Error detected on row {row_counter}! Not all sequencing_ids and/or run_dates are identical.")
            user_input = input("Do you want to continue anyway? (yes/no): ").lower()
            if user_input != "yes":
                print("Exiting script. No output was generated due to faulty project_info.csv")
                return  # Exit the function if the user does not want to continue

        project_id = row["project_ID"]
        customer = row["customer"]
        samples = row["#samples"]
        run_date = run_date_global
        sequencing_id = sequencing_id_global
        renaming = row["renaming?"]
        concat = row["concat?"]
        bi = row["BI?"]
        analysis_file = row["analysis_file?"]
        md5 = row["md5?"]

        # default outputs
        expected_objects = get_expected_objects(customer, samples)
        upload_priority = get_upload_priority(customer)
        random_string = generate_random_string(16)
        local_path = f"/media/share/novaseq01/Output/sequencing_data_for_upload/{sequencing_id}/{project_id}"
        local_path_metadata = f"{local_path}_rawdatalinks"  # changed default to "_rawdatalinks"  because Alba Health and ZOE are the exception with "_metadata"
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
        if customer == "ExtraN":
            upload_path_fastq1 = f"epiquest-zre/{project_id}/rawdata/{run_date}/{random_string}/"
            upload_command_fastq1 = f"time aws s3 cp {local_path} s3://epiquest-zre/{project_id}/rawdata/{run_date}/{random_string}/ --recursive"
            uploadpath_rawdatalinks = upload_path_fastq1

        elif customer == "ExtraNZRE":
            upload_path_fastq1 = f"epiquest-zre/{project_id}/rawdata/{run_date}/{random_string}/"
            upload_command_fastq1 = f"time aws s3 cp {local_path} s3://epiquest-zre/{project_id}/rawdata/{run_date}/{random_string}/ --recursive"
            uploadpath_rawdatalinks = upload_path_fastq1

        elif customer == "ExtraP":
            upload_path_fastq1 = f"zymo-zre-sequencing/{sequencing_id}/"
            upload_path_fastq2 = f"epiquest-zre/{project_id}/rawdata/{run_date}/{random_string}/"
            upload_command_fastq1 = f"time aws s3 cp {local_path} s3://zymo-zre-sequencing/{sequencing_id}/ --recursive"
            upload_command_fastq2 = f"time aws s3 cp {local_path} s3://epiquest-zre/{project_id}/rawdata/{run_date}/{random_string}/ --recursive"
            uploadpath_rawdatalinks = upload_path_fastq2

        elif customer == "microbiomics_upload_complete":
            project_id = "microbiomics_upload_complete"
            customer = "microbiomics_upload_complete"
            samples = 1
            run_date = run_date_global
            sequencing_id = sequencing_id_global
            renaming = "no"
            concat = "no"
            bi = "no"
            analysis_file = "no"
            md5 = "no"
            local_path = f"/media/share/novaseq01/Output/sequencing_data_for_upload/{sequencing_id_global}/upload_complete_files/microbiomics_upload_complete/"
            upload_path_fastq1 = f"zymo-zre-sequencing/{sequencing_id_global}/"
            upload_priority = get_upload_priority(customer)
            random_string = generate_random_string(16)
            upload_command_fastq1 = f"time aws s3 cp {local_path} s3://zymo-zre-sequencing/{sequencing_id_global}/ --recursive"  # main upload, prioritized
            bucket_region = "us-east-1"

        elif customer == "epigenetics_upload_complete":
            project_id = "epigenetics_upload_complete"
            customer = "epigenetics_upload_complete"
            samples = 1
            run_date = run_date_global
            sequencing_id = sequencing_id_global
            renaming = "no"
            concat = "no"
            bi = "no"
            analysis_file = "no"
            md5 = "no"
            local_path = f"/media/share/novaseq01/Output/sequencing_data_for_upload/{sequencing_id_global}/upload_complete_files/epigenetics_upload_complete/"
            upload_path_fastq1 = f"zymo-zre-sequencing/{sequencing_id_global}/"
            upload_priority = get_upload_priority(customer)
            random_string = generate_random_string(16)
            upload_command_fastq1 = f"time aws s3 cp {local_path} s3://zymo-zre-sequencing/{sequencing_id_global}/ --recursive"  # main upload, prioritized
            bucket_region = "us-east-1"

        elif customer == "ExtraPZRE":
            upload_path_fastq1 = f"zymo-zre-sequencing/{sequencing_id}/"
            upload_path_fastq2 = f"epiquest-zre/{project_id}/rawdata/{run_date}/{random_string}/"
            upload_command_fastq1 = f"time aws s3 cp {local_path} s3://zymo-zre-sequencing/{sequencing_id}/ --recursive"
            upload_command_fastq2 = f"time aws s3 cp {local_path} s3://epiquest-zre/{project_id}/rawdata/{run_date}/{random_string}/ --recursive"
            uploadpath_rawdatalinks = upload_path_fastq2

        elif customer == "ZOE":
            local_path_metadata = f"{local_path}_metadata"
            # upload_path_fastq1 = f"zymo-zoe/fastq/{project_id}/{run_date}/"                                                       # is now uploaded to gcloud using upload_zoe_projects_to_gcloud.py
            upload_path_fastq2 = f"epiquest-zre/zoe_projects/{project_id}/rawdata/{run_date}/{random_string}/"  # data is uploaded to epiquest-zre for backup
            # upload_path_metadata1 = "zymo-zoe/metadata/"                                                                          # is now uploaded to gcloud using upload_zoe_projects_to_gcloud.py
            # upload_path_metadata2 = f"epiquest-zre/zoe_projects/{project_id}/metadata/{run_date}/{random_string}/"                # not needed due to gcloud upload
            upload_command_fastq1 = f"time aws s3 cp {local_path} s3://zymo-zoe/fastq/{project_id}/{run_date}/ --recursive"
            upload_command_fastq2 = f"time aws s3 cp {local_path} s3://epiquest-zre/zoe_projects/{project_id}/rawdata/{run_date}/{random_string}/ --recursive"
            upload_command_metadata1 = f"time aws s3 cp {local_path_metadata} s3://zymo-zoe/metadata/ --recursive"
            upload_command_metadata2 = f"time aws s3 cp {local_path_metadata} s3://epiquest-zre/zoe_projects/{project_id}/rawdata/{run_date}/{random_string}/ --recursive"
            uploadpath_rawdatalinks = upload_path_fastq2

        elif customer == "ZRE_Standard_no_BI":
            local_path_metadata = f"{local_path}_rawdatalinks"
            upload_path_fastq1 = f"epiquest-zre/{project_id}/rawdata/{run_date}/{random_string}/"
            upload_path_metadata2 = f"epiquest-zre/{project_id}/metadata/{run_date}/{random_string}/"
            upload_command_fastq1 = f"time aws s3 cp {local_path} s3://epiquest-zre/{project_id}/rawdata/{run_date}/{random_string}/ --recursive --acl public-read-write"
            upload_command_metadata2 = f"time aws s3 cp {local_path_metadata} s3://epiquest-zre/{project_id}/metadata/{run_date}/{random_string}/ --recursive --acl public-read-write"
            uploadpath_rawdatalinks = upload_path_fastq1

        elif customer == "ZRE_Standard_with_BI":
            local_path_metadata = f"{local_path}_rawdatalinks"
            upload_path_fastq1 = f"zymo-zre-sequencing/{sequencing_id}/"
            upload_path_fastq2 = f"epiquest-zre/{project_id}/rawdata/{run_date}/{random_string}/"
            upload_path_metadata2 = f"epiquest-zre/{project_id}/metadata/{run_date}/{random_string}/"
            upload_command_fastq1 = f"time aws s3 cp {local_path} s3://zymo-zre-sequencing/{sequencing_id}/ --recursive"
            upload_command_fastq2 = f"time aws s3 cp {local_path} s3://epiquest-zre/{project_id}/rawdata/{run_date}/{random_string}/ --recursive --acl public-read-write"
            upload_command_metadata2 = f"time aws s3 cp {local_path_metadata} s3://epiquest-zre/{project_id}/metadata/{run_date}/{random_string}/ --recursive --acl public-read-write"
            uploadpath_rawdatalinks = upload_path_fastq2

        elif customer == "Ventra":
            local_path_metadata = f"{local_path}_rawdatalinks"
            upload_path_fastq1 = f"zymo-zre-sequencing/{sequencing_id}/"
            upload_path_fastq2 = f"epiquest-zre/{project_id}/rawdata/{run_date}/{random_string}/"
            upload_path_metadata2 = f"epiquest-zre/{project_id}/metadata/{run_date}/{random_string}/"
            upload_command_fastq1 = f"time aws s3 cp {local_path} s3://zymo-zre-sequencing/{sequencing_id}/ --recursive"
            upload_command_fastq2 = f"time aws s3 cp {local_path} s3://epiquest-zre/{project_id}/rawdata/{run_date}/{random_string}/ --recursive --acl public-read-write"
            upload_command_metadata2 = f"time aws s3 cp {local_path_metadata} s3://epiquest-zre/{project_id}/metadata/{run_date}/{random_string}/ --recursive --acl public-read-write"
            uploadpath_rawdatalinks = upload_path_fastq2

        elif customer == "Zotal":
            local_path_metadata = f"{local_path}_rawdatalinks"
            upload_path_fastq1 = f"epiquest-zre/{project_id}/rawdata/{run_date}/{random_string}/"
            upload_path_metadata2 = f"epiquest-zre/{project_id}/metadata/{run_date}/{random_string}/"
            upload_command_fastq1 = f"time aws s3 cp {local_path} s3://epiquest-zre/{project_id}/rawdata/{run_date}/{random_string}/ --recursive --acl public-read-write"
            upload_command_metadata2 = f"time aws s3 cp {local_path_metadata} s3://epiquest-zre/{project_id}/metadata/{run_date}/{random_string}/ --recursive --acl public-read-write"
            uploadpath_rawdatalinks = upload_path_fastq1

        elif customer == "ZRC":
            upload_path_fastq1 = f"zymo-zre-sequencing/{sequencing_id}/"
            upload_command_fastq1 = f"time aws s3 cp {local_path} s3://zymo-zre-sequencing/{sequencing_id}/ --recursive"
            uploadpath_rawdatalinks = upload_path_fastq1

        elif customer == "ZRC_Microbiomics":
            upload_path_fastq1 = f"zymo-zre-sequencing/{sequencing_id}/"
            upload_command_fastq1 = f"time aws s3 cp {local_path} s3://zymo-zre-sequencing/{sequencing_id}/ --recursive"
            uploadpath_rawdatalinks = upload_path_fastq1

        elif customer == "ZRC_Epigenetics":
            upload_path_fastq1 = f"zymo-zre-sequencing/{sequencing_id}/"
            upload_command_fastq1 = f"time aws s3 cp {local_path} s3://zymo-zre-sequencing/{sequencing_id}/ --recursive"
            uploadpath_rawdatalinks = upload_path_fastq1

        elif customer == "Alba Health":
            local_path_metadata = f"{local_path}_metadata"
            upload_path_fastq1 = f"s3-external-zymo/rawdata/{run_date}_{project_id}/"
            upload_command_fastq1 = f"time aws s3 cp {local_path} s3://s3-external-zymo/rawdata/{run_date}_{project_id}/ --recursive"
            upload_path_metadata1 = f"s3-external-zymo/metadata/{run_date}_{project_id}/"
            upload_path_metadata2 = f"epiquest-zre/{project_id}/metadata/{run_date}/{random_string}/"
            upload_command_metadata1 = f"time aws s3 cp {local_path_metadata} s3://s3-external-zymo/metadata/{run_date}_{project_id}/ --recursive"
            upload_command_metadata2 = f"time aws s3 cp {local_path_metadata} s3://epiquest-zre/{project_id}/metadata/{run_date}/{random_string}/ --recursive --acl public-read-write"
            uploadpath_rawdatalinks = upload_path_fastq1
            bucket_region = "eu-north-1"

        else:
            print(f"WARNING: Customer for {project_id} is not defined. Check customer_info in Upload_Data_Info.xlsm for available customers.")

        output_row = {
            "upload_priority": upload_priority,
            "project_ID": project_id,
            "#samples": samples,
            "customer": customer,
            "run_date": run_date,
            "sequencing_ID": sequencing_id,
            "renaming?": renaming,
            "concat?": concat,
            "BI?": bi,
            "analysis_file?": analysis_file,
            "md5?": md5,
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
        fieldnames = [
            "upload_priority",
            "project_ID",
            "#samples",
            "customer",
            "run_date",
            "sequencing_ID",
            "renaming?",
            "concat?",
            "BI?",
            "analysis_file?",
            "md5?",
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
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    print("\nProject output information generated successfully.")


if __name__ == "__main__":

    input_file = "project_info.csv"
    output_file = "project_output_info.csv"

    # Run the function
    generate_project_output_info(input_file, output_file)
