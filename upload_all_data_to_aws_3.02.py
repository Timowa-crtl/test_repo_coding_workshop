import boto3
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import os
import re

# setup for sending emails. Password for Hilde's Gmailadress is: "Zymo2023HZ". The password below is an "App Passwort" generated for the Device "Python".
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# Version 1.75 can now handle two new customers: ExtraPZRE and ExtraNZRE
# Version 2.11 should allow correct measurement of projects that only have one object to upload using workaround: "0.5 samples"
# Version 2.20 changed expected objects metadata to '1' for ZRC_Standards, Zotal, Ventra as rawdatalinks.csv are now always uploaded
# Version 2.21 improved print statements for summary, added Y/N switch for email
# Version 2.22 fixxed typo in Y/N switch: --> if want_to_send_mail.lower() == "y":
# Version 2.24 removed ZOE Uploads to zymo-zoe as those are now done via gcloud. Deactivated generation of zoe email as that text is no longer correct.
# Version 2.25 set expected objects metadata ZOE to 0 due to gcloud upload without metadatafile
# Version 3.00 increased upload speed by adding concurrent futures to upload more than 1 file at once (hope it's tread-safe), refactored upload into 2 functions only, removed unneeded functionality e.g. zoe S3 email
# Version 3.01 minor changes to upload functions for safety
# Version 3.02 now only creating boto3 client 's3_client' once to safe ressources, as boto3 clients a regared generally thread-safe (https://boto3.amazonaws.com/v1/documentation/api/1.19.0/guide/clients.html)


def send_email(recipients, subject, body, attachment_file_paths):
    port = 465
    smtp_server = "smtp.gmail.com"
    sender_email = "hildezymoresearch@gmail.com"
    apppassword = "bqjxoscbhsmkxwci"

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))  # Attach the email body as plain text

    if attachment_file_paths:
        for attachment_file_path in attachment_file_paths:
            # Attach each file with its original filename
            filename = os.path.basename(attachment_file_path)
            attachment = open(attachment_file_path, "rb")
            part = MIMEBase("application", "octet-stream")
            part.set_payload((attachment).read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename= {filename}")
            msg.attach(part)

    server = smtplib.SMTP_SSL(smtp_server, port)
    server.login(sender_email, apppassword)
    print("Login successful")

    for recipient in recipients:
        server.sendmail(sender_email, recipient, msg.as_string())
        print(f"Email was successfully sent to '{recipient}'.")

    server.quit()


def get_folder_size_in_gb(path):
    """Gets the size of a folder recursively in GB rounded to 1 decimal place."""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            total_size += os.path.getsize(file_path)
    # Convert bytes to gigabytes and round to 1 decimal place
    size_in_gb = total_size / (1024**3)  # Convert bytes to GB
    size_in_gb = round(size_in_gb, 1)  # Round to 1 decimal place
    return size_in_gb


def get_expected_objects_metadata(customer):
    """Determines the expected number of metadata objects for a given customer."""
    customer_expected_objects_metadata_map = {
        "ExtraP": 0,
        "ExtraPZRE": 0,
        "ZRC_Microbiomics": 0,
        "ZRC_Jeffrey": 0,
        "ZRC_Epigenetics": 0,
        "ZRC": 0,
        "ZRE_Standard_with_BI": 1,
        "Ventra": 1,
        "Alba Health": 1,
        "Zotal": 1,
        "ZRE_Standard_no_BI": 1,
        "ExtraN": 0,
        "ExtraNZRE": 0,
        "ZOE": 0,
        "zrc_upload_complete": 1,  # uploading zrc_upload_complete.txt for ZRC
        "microbiomics_upload_complete": 1,  # uploading microbiomics_upload_complete.txt for ZRC
        "epigenetics_upload_complete": 1,  # uploading epigenetics_upload_complete.txt for ZRC
    }

    # Check if the customer is in the map, and return the corresponding priority
    if customer in customer_expected_objects_metadata_map:
        expected_objects_metadata = customer_expected_objects_metadata_map[customer]
    else:
        # Handle the case where the customer is not in the map
        print(f"Customer {customer} has not been assinged, yet. expected_objects_metadata was set to default value = 1")
        expected_objects_metadata = 1

    return expected_objects_metadata


def extract_uploadpaths(file_path):
    """Extracts upload path information from a CSV file and organizes it into a structured format."""
    run_date = "rundate not defined"
    priority_customer_datatype_localpath_awspath_samples_objects = {}
    with open(file_path, newline="") as csvfile:
        reader = csv.DictReader(csvfile)

        # Convert the CSV reader to a list of dictionaries
        rows = list(reader)

        # Check if there are any rows in the CSV
        if len(rows) > 0:
            # Access the first row (index 0) directly
            first_row = rows[0]
            run_date = first_row.get("run_date", "rundate not defined")
            print()
            print(f"rundate: {run_date}")
            print()

        for row in rows[0:]:
            upload_priority = row["upload_priority"]
            project_ID = row["project_ID"]
            customer = row["customer"]
            localpath = row["local_path"]
            awspath_1 = row["upload path fastq 1"]
            awspath_2 = row["upload path fastq 2"]
            localpath_metadata = row["local_path_metadata"]
            awspath_metadata_1 = row["upload path metadata 1"]
            awspath_metadata_2 = row["upload path metadata 2"]
            expected_objects_metadata = get_expected_objects_metadata(customer)
            expected_objects = row["expected_objects"]
            nr_of_samples = row["#samples"]
            nr_of_samples = float(nr_of_samples)  # convert to float to be able to use 0.5
            if nr_of_samples >= 1 and nr_of_samples % 1 == 0:
                nr_of_samples = int(nr_of_samples)

            # Create a formatted key for the dictionary using the project ID and counter
            if awspath_1 != "empty":
                key = f"{project_ID}_1"
                priority_customer_datatype_localpath_awspath_samples_objects[key] = {
                    "upload_priority": upload_priority,
                    "customer": customer,
                    "datatype": "rawdata",
                    "local_path": localpath,
                    "aws_path": awspath_1,
                    "expected_samples": nr_of_samples,
                    "expected_objects": expected_objects,
                }

            if awspath_2 != "empty":
                key = f"{project_ID}_2"
                priority_customer_datatype_localpath_awspath_samples_objects[key] = {
                    "upload_priority": (int(upload_priority) + 1),
                    "customer": customer,
                    "datatype": "rawdata",
                    "local_path": localpath,
                    "aws_path": awspath_2,
                    "expected_samples": nr_of_samples,
                    "expected_objects": expected_objects,
                }

            if awspath_metadata_1 != "empty":
                key = f"{project_ID}_metadata_1"
                priority_customer_datatype_localpath_awspath_samples_objects[key] = {
                    "upload_priority": upload_priority,
                    "customer": customer,
                    "datatype": "metadata",
                    "local_path": localpath_metadata,
                    "aws_path": awspath_metadata_1,
                    "expected_samples": nr_of_samples,
                    "expected_objects": expected_objects_metadata,
                }

            if awspath_metadata_2 != "empty":
                key = f"{project_ID}_metadata_2"
                priority_customer_datatype_localpath_awspath_samples_objects[key] = {
                    "upload_priority": (int(upload_priority) + 1),
                    "customer": customer,
                    "datatype": "metadata",
                    "local_path": localpath_metadata,
                    "aws_path": awspath_metadata_2,
                    "expected_samples": nr_of_samples,
                    "expected_objects": expected_objects_metadata,
                }

    return (
        priority_customer_datatype_localpath_awspath_samples_objects,
        run_date,
    )


def upload_file(local_file_path, s3_client, bucket_name, s3_key, extra_args):
    """Uploads a file to S3 and returns success/failure."""

    # defining threshold and chunksize for multipart uploads
    config = boto3.s3.transfer.TransferConfig(multipart_threshold=100 * 1024 * 1024, multipart_chunksize=100 * 1024 * 1024)

    try:
        s3_client.upload_file(local_file_path, bucket_name, s3_key, Config=config, ExtraArgs=extra_args)
        return True, None  # Upload successful
    except Exception as e:
        return False, str(e)  # Upload failed


def upload_directory(local_path, s3_client, aws_path, storage_class=None, public=False, tag=None, max_workers=20):
    """Uploads each file in a directory to S3 using concurrent futures."""

    extra_args = {}
    if storage_class:
        extra_args["StorageClass"] = storage_class
    if public:
        extra_args["ACL"] = "public-read"
    if tag:
        extra_args["Tagging"] = tag

    bucket_name, prefix = aws_path.split("/", 1)

    files_uploaded = 0
    files_failed = 0
    futures = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for root, _, files in os.walk(local_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                s3_key = os.path.join(prefix, os.path.relpath(local_file_path, local_path))
                future = executor.submit(upload_file, local_file_path, s3_client, bucket_name, s3_key, extra_args)
                futures.append((local_file_path, future))

        for local_file_path, future in futures:
            try:
                success, error_message = future.result()
                if success:
                    files_uploaded += 1
                    print(".", end="", flush=True)
                else:
                    files_failed += 1
                    print(f"\nError uploading {local_file_path}: {error_message}")
            except Exception as e:
                files_failed += 1
                print(f"\nUnexpected error uploading {local_file_path}: {e}")

        print()

    return files_uploaded, files_failed


def main():
    # initialize email variables
    attachment_file_paths = None
    recipients = []

    # ask user if he wants to be notified when the upload is finished
    want_to_send_mail = "not defined"
    while want_to_send_mail not in ["Y", "y", "N", "n"]:
        want_to_send_mail = input("Do you want to receive an email once the upload is finished? (Y/N) ")

    if want_to_send_mail.lower() == "y":
        # default recipients
        recipients = ["services@zymoresearch.de"]

        # ask user for additional email address
        recipient_input = input(f"{recipients} will be notified once the upload is finished. If you want other mail-adress to be notified, please enter that email address now. Else, press Enter: ")

        # Use regular expression to validate the email address
        email_pattern = r"^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+$"

        # Check if the recipient_input is a valid email address
        if re.match(email_pattern, recipient_input):
            recipients.append(recipient_input)
            print(f"Email address {recipient_input} has been added to the list of recipients. Email will be sent to {recipients}. Starting the upload now.")
        else:
            print(f"Invalid email address. Email will only be sent to {recipients}.")

    # Get the starttime from system time
    start_time = datetime.now()

    # Create a single S3 client to be used throughout the script, as clients are regarded as thread-safe (https://boto3.amazonaws.com/v1/documentation/api/1.19.0/guide/clients.html)
    s3_client = boto3.client("s3")

    # initate some variables
    priority_customer_datatype_localpath_awspath_samples_objects = {}
    expected_objects_metadata = 1
    file_path = "project_output_info.csv"
    priority_customer_datatype_localpath_awspath_samples_objects, run_date = extract_uploadpaths(file_path)

    # Sort the dictionary by upload_priority
    priority_customer_datatype_localpath_awspath_samples_objects = dict(sorted(priority_customer_datatype_localpath_awspath_samples_objects.items(), key=lambda item: int(item[1]["upload_priority"])))

    print("Projects will be uploaded in the following order:")
    for key, values in priority_customer_datatype_localpath_awspath_samples_objects.items():

        print(f"{values['upload_priority']}: For customer {values['customer']}, {key} we will upload {values['expected_samples']} samples.")

    # Create a list to store the data for each key
    data_list = []
    # Create a list of print-strings
    summary = []

    # calculate how many files will have to be uploaded for each key in the dictionary to AWS S3
    total_files_to_upload = 0

    for key, data in priority_customer_datatype_localpath_awspath_samples_objects.items():
        expected_objects = data["expected_objects"]
        expected_objects = int(expected_objects)
        total_files_to_upload += expected_objects

    print(f"\nWe will have to upload a total of {total_files_to_upload} files")

    # Upload data for each key in the dictionary to AWS S3
    total_files_uploaded = 0
    total_files_failed = 0

    for key, data in priority_customer_datatype_localpath_awspath_samples_objects.items():
        start_time_key = datetime.now()

        local_path = data["local_path"]
        aws_path = data["aws_path"]
        customer = data["customer"]
        datatype = data["datatype"]
        expected_samples = data["expected_samples"]
        expected_samples = int(expected_samples)
        expected_objects = data["expected_objects"]
        expected_objects = int(expected_objects)
        added_objects = "NA"

        # Get the size of the local folder
        folder_size = get_folder_size_in_gb(local_path)

        print(f"\ncurrently uploading '{key}' for customer '{customer}'")
        print(f"folder size: {folder_size} GB")
        print(f"expected number of objects: {expected_objects}")

        # 1. Get number of objects before upload
        bucket_name, prefix = aws_path.split("/", 1)
        actual_objects_before_upload = 0

        # 1.1 Use a paginator to handle pagination of the list_objects_v2 API
        paginator = s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        for page in page_iterator:
            # 1.2 Count the number of objects in the current page
            num_objects_in_page = len(page.get("Contents", []))
            actual_objects_before_upload += num_objects_in_page

        # Upload: If aws_path startwith 'epiquest-zre' make the object public using ACL. Else, just upload. Tag ZOE data and store in DEEP_ARCHIVE
        if aws_path.startswith("epiquest-zre/zoe_projects/"):
            files_uploaded, files_failed = upload_directory(local_path, s3_client, aws_path, storage_class="DEEP_ARCHIVE", public=False, tag="zoe_project")
            total_files_uploaded += files_uploaded
            total_files_failed += files_failed
        elif aws_path.startswith("epiquest-zre/"):
            files_uploaded, files_failed = upload_directory(local_path, s3_client, aws_path, public=True)
            total_files_uploaded += files_uploaded
            total_files_failed += files_failed
        else:
            files_uploaded, files_failed = upload_directory(local_path, s3_client, aws_path)
            total_files_uploaded += files_uploaded
            total_files_failed += files_failed

        # Get finish time for key and calculate duration it took to upload in minutes
        finish_time_key = datetime.now()
        duration_key = finish_time_key - start_time_key
        duration_key_min = duration_key.total_seconds() / 60  # Convert duration to minutes

        # Format the datetime as "YYMMDD hh:mm"
        formatted_time = finish_time_key.strftime("%y%m%d %H:%M")

        print(f"Uploaded Objects: {total_files_uploaded}/{total_files_to_upload}")
        if total_files_failed > 0:
            print(f"WARNING!: {total_files_failed} files failed to upload. Please check the logs.")
        if files_uploaded != expected_objects:
            print(f"WARNING!: Uploaded files ({files_uploaded}) do not match expected ({expected_objects})!")

        # 1. Get number of objects after upload
        bucket_name, prefix = aws_path.split("/", 1)
        actual_objects_after_upload = 0

        # 1.1 Use a paginator to handle pagination of the list_objects_v2 API
        paginator = s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        for page in page_iterator:
            # 1.2 Count the number of objects in the current page
            num_objects_in_page = len(page.get("Contents", []))
            actual_objects_after_upload += num_objects_in_page

        # Calculate added_objects
        added_objects = actual_objects_after_upload - actual_objects_before_upload

        # Compare expected_objects to the number of objects in the awspath (setup)
        bucket_name, prefix = aws_path.split("/", 1)
        actual_objects = actual_objects_after_upload

        # Compare expected_objects to the number of objects in the awspath
        if actual_objects == expected_objects:
            if added_objects == 0 and datatype != "metadata":
                control = "CORRECT"
                explanation = f"Upload for {key}: No Objects added but number of objects is correct. Has the file been uploaded before?"
            else:
                control = "SUCCESS"
                explanation = f"Upload for {key}: SUCCESSFUL!"

        else:
            # Compare expected_objects to the change in number of objects before and after the upload
            if added_objects == expected_objects:
                control = "SUCCESS"
                explanation = f"Upload for {key}: SUCCESSFUL!"
            elif added_objects == 0 and added_objects == expected_objects:
                control = "SUCCESS"
                explanation = f"Upload for {key}: SUCCESSFUL!"
            elif added_objects == 0:
                control = "UPLOADED BEFORE?"
                explanation = f"Upload for {key}: No Objects added. Has the file been uploaded before?"
            else:
                control = "FAILED UPLOAD!"
                explanation = f"Upload for {key} FAILED! Check the upload_log.csv!"

        # print and log results
        string = f"{control}:\t{key}\t{customer}\t{explanation}\tUpload completed: {formatted_time}"
        print(string)
        summary.append(string)

        # Append data for each key to the data_list
        data_list.append([customer, key, aws_path, expected_objects, actual_objects, added_objects, folder_size, control, formatted_time, duration_key_min])

    # Write the data_list to the CSV file 'upload_log.csv'
    csv_file = os.path.join(os.getcwd(), "upload_log.csv")

    with open(csv_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["customer", "key", "aws_path", "expected_objects", "actual_objects", "added_objects", "folder_size[GB]", "control", "finish_time", "duration[min]"])  # Write the header
        writer.writerows(data_list)  # Write data for each key

    print(f"\n\nUpload finished!!")
    print(f"Uploaded Objects: {total_files_uploaded}/{total_files_to_upload}")
    if total_files_uploaded != total_files_to_upload:
        print(f"\nWARNING!: Total uploaded files ({total_files_uploaded}) do not match total expected ({total_files_to_upload})")

    # Check upload again to show prints at the end of script
    print()
    print("Summary of the Upload:")
    for line in summary:
        print(line)

    print()
    print(f"Log data written to upload_log.csv.")
    print()

    # Get the finishtime from system time
    finish_time = datetime.now()
    # Calculate the duration
    duration = finish_time - start_time
    # Calculate duration in hours
    duration_hours = duration.total_seconds() / 3600
    # Format start_time and finish_time as strings (hours and minutes only)
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M")
    finish_time_str = finish_time.strftime("%Y-%m-%d %H:%M")

    attachment_file_paths = [csv_file]

    # Create a formatted summary string with each line separated
    summary_lines = "\n".join(summary)

    # recipients = recipients #definded at beginning of script
    subject = f"{run_date}: Upload completed!"
    body = (
        f"Dear user,\n\n"
        f"the upload for the {run_date} sequencing-run was completed.\n\n"
        f"The process started at: {start_time_str}.\n"
        f"The process finished at: {finish_time_str}.\n"
        f"In total the upload took {duration_hours:.3f} hours.\n\n"
        f"Summary of the upload:\n{summary_lines}\n\n"
        f"The complete summary 'upload_log.csv' has been attached to this email and saved in your working directory.\n"
        f"best wishes,\n\n"
        f"Hilde\n"
    )
    send_email(recipients, subject, body, attachment_file_paths)


if __name__ == "__main__":
    main()
