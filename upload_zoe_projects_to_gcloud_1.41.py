import csv
import os
import re
import time
from datetime import datetime
from google.cloud import storage
from google.api_core.exceptions import GoogleAPIError
from google.api_core.exceptions import Forbidden
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import concurrent.futures  # Import ThreadPoolExecutor for concurrent processing

# 1.00 This Script uploads fastq files to gcloud. The Module concurrent.futures is used to allow multible files to be uploaded in parallel which increases upload speeds significantly.
# 1.01 increased TIMEOUT_DURATION_S for concurrent futures as 1/800 files was not uploaded in testupload due to timeout error: "'Connection aborted.', TimeoutError('The write operation timed out')"
# 1.02 fixed typo
# 1.10 adding retries to upload_file_to_gcloud() to make script safer for issues like short internet connection loss
# 1.30 switched to PRODUCTION
# 1.41 made input of email adresses less annoying


# constants

# switch between stage and production
# ZOE_BUCKET_CREDENTIALS_FILE_RELATIVE_PATH = "credentials/stage-credentials.json"  # stage credentials
ZOE_BUCKET_CREDENTIALS_FILE_RELATIVE_PATH = "credentials/prod-credentials.json"  # production credentials

# get working directory
working_directory = os.getcwd()

# construct absolute credentials paths
ZOE_BUCKET_CREDENTIALS_FILE_PATH = os.path.join(working_directory, ZOE_BUCKET_CREDENTIALS_FILE_RELATIVE_PATH)

# authenticate
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ZOE_BUCKET_CREDENTIALS_FILE_PATH

# UPLOAD_BUCKET_NAME = "zoe-backend-stage-zymo-results"  # stage bucket
UPLOAD_BUCKET_NAME = "zoe-backend-production-zymo-results"  # production bucket

# input & output filenames
INPUT_FILEPATH = "project_output_info.csv"
UPLOAD_LOG_NAME = "gcloud_upload_log.csv"

# recipients for notification email
DEFAULT_RECIPIENTS_UPLOAD_COMPLETED = ["services@zymoresearch.de"]

# select the number of files that are allowed to uploaded concurrently
MAX_CONCURRENT_FILES = 12

# timeout duration for concurrent threads in seconds
TIMEOUT_DURATION_S = 3600


def format_zoe_gcloud_string(string_counter, project_id, nr_samples, run_date):
    samples = "samples"
    if nr_samples == 1:
        samples = "sample"
    formatted_string = f"\t{string_counter}. " f"for {project_id} data for {nr_samples} {samples} was uploaded. " f"Data was uploaded to: gs://{UPLOAD_BUCKET_NAME}/data/{run_date}_ZRE/"
    return formatted_string


def extract_zoe_projects(file_path):
    zoe_projects = {}
    with open(file_path, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            customer = row["customer"]
            project_id = row["project_ID"]
            samples = row["#samples"]
            run_date = row["run_date"]
            gcloud_folder_name = f"{project_id}_gcloud"
            if customer == "ZOE":
                zoe_projects[project_id] = (samples, gcloud_folder_name, run_date)
    project_ids = list(zoe_projects.keys())

    return zoe_projects, project_ids


def format_upload_duration(start_time, end_time):
    # Calculate the duration of upload
    upload_duration = end_time - start_time

    # Extract hours and minutes from the timedelta object
    hours = upload_duration.seconds // 3600
    minutes = (upload_duration.seconds % 3600) // 60

    # Format the upload duration to hh:mm
    upload_duration_formatted = f"{hours:02d}:{minutes:02d}"

    return upload_duration_formatted


def get_folder_size_in_gb(path):
    """Function to get the size of a folder recursively in GB rounded to 1 decimal place"""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            total_size += os.path.getsize(file_path)
    # Convert bytes to gigabytes and round to 1 decimal place
    size_in_gb = total_size / (1024**3)  # Convert bytes to GB
    size_in_gb = round(size_in_gb, 1)  # Round to 1 decimal place
    return size_in_gb


def choose_recipients(DEFAULT_RECIPIENTS_UPLOAD_COMPLETED):
    recipients = []

    # ask user if he wants to be notified when the upload is finished
    want_to_send_mail = "not defined"
    while want_to_send_mail not in ["Y", "y", "N", "n"]:
        want_to_send_mail = input("Do you want to receive an email once the upload is finished? (Y/N) ")

    if want_to_send_mail.lower() == "y":
        # default recipients
        recipients = DEFAULT_RECIPIENTS_UPLOAD_COMPLETED

        # ask user for additional email address
        recipient_input = input(
            f"{recipients} will be notified once the upload is finished. " f"If you want other email address to be notified, " f"please enter that email address now. Else, press Enter: "
        )

        # Use regular expression to validate the email address
        email_pattern = r"^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+$"

        # Check if the recipient_input is a valid email address
        if re.match(email_pattern, recipient_input):
            recipients.append(recipient_input)
            print(f"Email address {recipient_input} has been added to the list of recipients. " f"Email will be sent to {recipients}. Starting the upload now.")
        else:
            print(f"Invalid email address. Email will only be sent to {recipients}. " f"Starting the upload now.")

    return recipients


def write_email_texts(zoe_projects, project_ids, projects_summary, run_date):
    project_ids_comma_separated = ", ".join(project_ids)
    zoe_email_path = f"{run_date}_Data available on gcloud_{project_ids_comma_separated}.txt"
    # Check if zoe_projects is not empty
    if project_ids:
        projects = "project"

        if len(project_ids) > 1:
            projects = "projects"

        # Open the file in write mode and store the output in it
        with open(zoe_email_path, "w") as f:

            # Printing Hello Customer
            f.write("Dear Daniela, dear Meaghan,\n\n")

            # Print the metadata files and sequencing-data for projects
            f.write(f"New sequencing-data for {projects} ")
            f.write(", ".join(project_ids[:-1]))  # Print all project IDs except the last one with a comma and space separator
            if len(project_ids) > 1:
                f.write(", and ")  # Add ", and " before the last project ID if there are multiple projects
            f.write(f"{project_ids[-1]} is now available on Google Cloud.\n\n")  # Print the last project ID

            # Printing formatted strings for ZOE projects
            zoe_string_counter = 1
            for project_id, tuple in zoe_projects.items():
                samples = int(tuple[0])
                formatted_str = format_zoe_gcloud_string(zoe_string_counter, project_id, samples, run_date)
                f.write(formatted_str)
                f.write("\n\n")
                zoe_string_counter += 1

            f.write("If you encounter any issues or have any questions regarding this data upload, " "please do not hesitate to reach out to us.")

    # parameters for user mail
    user_mail_subject = f"{run_date}: ZOE Google Cloud Upload completed ({project_ids_comma_separated})"
    user_mail_body = (
        f"Dear user,\n\n"
        f"the gcloud upload for the {run_date} sequencing-run was completed.\n"
        f"{projects_summary}\n"
        f"Upload duration [hh:mm]: {upload_duration_formatted}\n\n\n"
        f"best wishes,\n\n"
        f"Hilde\n"
    )
    return user_mail_subject, user_mail_body, zoe_email_path


def send_email(recipients, subject, body, attachment_file_paths):
    PORT = 465
    SMTP_SERVER = "smtp.gmail.com"
    SENDER_EMAIL = "hildezymoresearch@gmail.com"
    APPPASSWORD = "bqjxoscbhsmkxwci"

    msg = MIMEMultipart()
    msg["From"] = SENDER_EMAIL
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

    server = smtplib.SMTP_SSL(SMTP_SERVER, PORT)
    server.login(SENDER_EMAIL, APPPASSWORD)
    print("Login successful")

    for recipient in recipients:
        server.sendmail(SENDER_EMAIL, recipient, msg.as_string())
        print(f"Email was successfully sent to '{recipient}'.")

    server.quit()


def write_upload_log(path, upload_log_projects):
    with open(path, "w", newline="") as csvfile:
        fieldnames = ["project_id"] + list(upload_log_projects[next(iter(upload_log_projects))].keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for project_id, project_info in upload_log_projects.items():
            writer.writerow({"project_id": project_id, **project_info})
    return


def upload_file_to_gcloud(local_file_path, bucket_name, blob_path, TIMEOUT_DURATION_S):
    """Uploads a file to Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    # Retry upload if an Error is raised
    max_retries = 5
    for attempt in range(max_retries):
        try:
            blob.upload_from_filename(local_file_path, timeout=TIMEOUT_DURATION_S)
            print(f"\tSUCCESS!!!\tUploaded to: {blob_path}")
            return True
        except Forbidden as e:
            # Handle 403 Forbidden error
            print(f"\n\tERROR!!!\tYour request to {blob_path} was denied.")
            print(e)
            return False
        except Exception as e:
            print(f"\tERROR!!!\tCan't upload to {blob_path}")
            print(e)
            wait_duration_s = 60 * attempt + 1
            print(f"\tRetry Nr.{attempt}/{max_retries}:")
            # If not the last attempt, wait for a brief moment before retrying
            if attempt < 4:
                print(f"\tWaiting for {wait_duration_s} seconds before retrying...")
                time.sleep(wait_duration_s)  # Wait for seconds before retrying
    return False


def upload_project(project_id, samples, gcloud_folder_name, run_date):
    start_time_project = datetime.now()
    print(f"\nUploading Project_ID {project_id}:")
    bucket_dir = f"data/{run_date}_ZRE/"
    gcloud_folder_name = os.path.abspath(gcloud_folder_name)

    expected_files = int(samples) * 4
    files_uploaded_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_FILES) as executor:
        futures = []
        for root, dirs, files in os.walk(gcloud_folder_name):

            for file in files:
                local_file_path = os.path.join(root, file)
                # Extracting the filename
                # filename = os.path.basename(local_file_path)

                # Constructing the blob path
                blob_path = os.path.join(bucket_dir, os.path.relpath(local_file_path, gcloud_folder_name))
                # Queue the file upload
                futures.append(executor.submit(upload_file_to_gcloud, local_file_path, UPLOAD_BUCKET_NAME, blob_path, TIMEOUT_DURATION_S))
        # Wait for all futures to complete
        for future in concurrent.futures.as_completed(futures):
            if future.result():
                files_uploaded_count += 1

    end_time_project = datetime.now()
    duration_min = (end_time_project - start_time_project).total_seconds() / 60.0
    folder_size_gb = get_folder_size_in_gb(gcloud_folder_name)

    upload_status = "INCOMPLETE?"
    if expected_files == files_uploaded_count:
        upload_status = "SUCCESS!"

    return {
        "run_date": run_date,
        "bucket": UPLOAD_BUCKET_NAME,
        "bucket_directory": bucket_dir,
        "control": upload_status,
        "expected_files": expected_files,
        "files_uploaded_count": files_uploaded_count,
        "folder_size_GB": folder_size_gb,
        "start_time": start_time_project,
        "finish_time": end_time_project,
        "duration_min": duration_min,
    }


if __name__ == "__main__":

    # ask user if he wants to be notified when upload is complete
    recipients = choose_recipients(DEFAULT_RECIPIENTS_UPLOAD_COMPLETED)

    # Record start time to print duration later
    start_time = datetime.now()

    # get working directory
    working_directory = os.getcwd()

    # construct absolute credentials paths
    ZOE_BUCKET_CREDENTIALS_FILE_PATH = os.path.join(working_directory, ZOE_BUCKET_CREDENTIALS_FILE_RELATIVE_PATH)

    # authenticate
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ZOE_BUCKET_CREDENTIALS_FILE_PATH

    # extract zoe projects and and expected objects from file_path
    zoe_projects, project_ids = extract_zoe_projects(INPUT_FILEPATH)

    print(f"Data will be uploaded to: {UPLOAD_BUCKET_NAME}")
    print()

    # dict to log project upload parameters
    upload_log_projects = {}

    # Upload each Zoe project's local folder contents to Google Cloud
    for project_id, (samples, gcloud_folder_name, run_date) in zoe_projects.items():
        upload_log_projects[project_id] = upload_project(project_id, samples, gcloud_folder_name, run_date)

    # Record end time and print duration
    end_time = datetime.now()
    # Call the function to get the formatted upload duration
    upload_duration_formatted = format_upload_duration(start_time, end_time)
    print(f"\nUpload duration [hh:mm]: {upload_duration_formatted} ")

    # Print the count of successfully uploaded files for each project
    projects_summary = "\nNumber of files uploaded per project:\n"

    for project_id, project_info in upload_log_projects.items():
        files_uploaded_count = project_info["files_uploaded_count"]
        expected_files = project_info["expected_files"]
        upload_status = project_info["control"]
        projects_summary += f"Project_ID {project_id}: Files uploaded: {files_uploaded_count}/{expected_files} ({upload_status})\n"

    print(projects_summary)

    # write upload log
    upload_log_path = os.path.join(working_directory, UPLOAD_LOG_NAME)
    write_upload_log(upload_log_path, upload_log_projects)

    # Write zoe_email_text and sent user mail using gmail
    subject, body, zoe_email_path = write_email_texts(zoe_projects, project_ids, projects_summary, run_date)
    attachment_file_paths = [zoe_email_path, upload_log_path]
    send_email(recipients, subject, body, attachment_file_paths)
