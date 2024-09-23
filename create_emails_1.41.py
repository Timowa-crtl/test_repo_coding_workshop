import csv

# Version 1.41: This script processes a CSV file containing project information and generates email templates for different customers.
# Key functionalities:

# 1. Reads project data from a CSV file.
# 2. Extracts relevant information for specific customers.
# 3. Generates customized email templates based on customer type and project details.
# 4. Exports email templates as text files.
# The script handles various customer types (ZRE_Standard_with_BI, Ventra, ZRE_Standard_no_BI, Alba Health, ExtraPZRE)
# and creates appropriate email content, including subject lines, recipients, and body text.
# The email templates include information about uploaded data, bioinformatics reports, and raw data links.
# Usage:
# 1. Ensure the input CSV file 'project_output_info.csv' is in the same directory as the script.
# 2. Run the script to generate email template text files for each qualifying project.

# Note: Some placeholders (e.g., <<<<<<project_manager>>>>>>, <<<<<<INSERT LINK HERE>>>>>>)
# need to be replaced with actual information before sending the emails.


def extract_projects_with_emails(file_path):
    projects_with_emails = {}

    with open(file_path, newline="") as csvfile:
        reader = csv.DictReader(csvfile)

        # Convert the CSV reader to a list of dictionaries
        rows = list(reader)

        # Check if there are any rows in the CSV
        if len(rows) > 0:
            for row in rows:
                project_ID = row["project_ID"]
                customer = row["customer"]

                # Create a formatted key for the dictionary using the project ID and counter
                if customer in ["ZRE_Standard_with_BI", "Ventra", "ZRE_Standard_no_BI", "Alba Health", "ExtraPZRE"]:
                    projects_with_emails[project_ID] = {
                        "upload_priority": row["upload_priority"],
                        "#samples": row["#samples"],
                        "customer": row["customer"],
                        "run_date": row["run_date"],
                        "sequencing_ID": row["sequencing_ID"],
                        "renaming?": row["renaming?"],
                        "concat?": row["concat?"],
                        "BI?": row["BI?"],
                        "analysis_file?": row["analysis_file?"],
                        "md5?": row["md5?"],
                        "random_string": row["random_string"],
                        "local_path": row["local_path"],
                        "local_path_metadata": row["local_path_metadata"],
                        "expected_objects": row["expected_objects"],
                        "upload path fastq 1": row["upload path fastq 1"],
                        "upload path fastq 2": row["upload path fastq 2"],
                        "upload path metadata 1": row["upload path metadata 1"],
                        "upload path metadata 2": row["upload path metadata 2"],
                        "upload_command_fastq_1": row["upload_command_fastq_1"],
                        "upload_command_fastq_2": row["upload_command_fastq_2"],
                        "upload_command_metadata_1": row["upload_command_metadata_1"],
                        "upload_command_metadata_2": row["upload_command_metadata_2"],
                        "uploadpath_rawdatalinks": row["uploadpath_rawdatalinks"],
                        "bucket_region": row["bucket_region"],
                    }

    return projects_with_emails


def export_txt_file(customer, project_id, run_date, bucket_region, random_string):
    subject_template = {
        "ExtraPZRE": f"{run_date}_Bioinforeport_{customer}",
        "Alba Health": f"{run_date}_Upload completed_{customer}_{project_id}",
        "ZRE_Standard_no_BI": f"{run_date}_Rawdatalinks_{customer}_{project_id}",
        "Ventra": f"{run_date}_Bioinforeport_{customer}_{project_id}",
        "ZRE_Standard_with_BI": f"{run_date}_Bioinforeport_{customer}_{project_id}",
    }
    default_subject = f"{run_date}__Upload completed__{customer}_{project_id}"
    subject = subject_template.get(customer, default_subject)

    filename_template = {
        "ExtraPZRE": f"SEND_MAIL_WHEN_BI_IS_DONE_{subject}",
        "Alba Health": f"SEND_MAIL_AFTER_UPLOAD_{subject}",
        "ZRE_Standard_no_BI": f"SEND_MAIL_AFTER_UPLOAD_{subject}",
        "Ventra": f"SEND_MAIL_WHEN_BI_IS_DONE_{subject}",
        "ZRE_Standard_with_BI": f"SEND_MAIL_WHEN_BI_IS_DONE_{subject}",
    }

    default_filename = f"{run_date}__Upload completed__{customer}_{project_id}"
    filename = filename_template.get(customer, default_filename) + ".txt"

    to_recipients = []
    cc_recipients = []

    # recipient lists
    if customer == "ZRE_Standard_no_BI":
        to_recipients = ["services@zymoresearch.de"]
        cc_recipients = ["<<<<<<project_manager>>>>>>"]
    elif customer == "Ventra":
        to_recipients = ["services@zymoresearch.de"]
        cc_recipients = ["ptripp@zymoresearch.de", "MSchopp@zymoresearch.de"]
    elif customer == "ZRE_Standard_with_BI":
        to_recipients = ["services@zymoresearch.de"]
        cc_recipients = ["<<<<<<project_manager>>>>>>"]
    elif customer == "Alba Health":
        to_recipients = ["services@zymoresearch.de"]
        cc_recipients = ["<<<<<<project_manager>>>>>>"]
    elif customer == "ExtraPZRE":
        to_recipients = ["services@zymoresearch.de"]
        cc_recipients = [""]
    else:
        to_recipients = ["services@zymoresearch.de"]
        cc_recipients = ["<<<<<<project_manager>>>>>>"]

    email_text = ""
    if customer == "ZRE_Standard_no_BI":
        email_text = (
            f"To: {', '.join(to_recipients)}\n"
            f"Cc: {', '.join(cc_recipients)}\n\n"
            f"Betreff: {subject}\n\n"
            f"Hallo,\n\n"
            f"die Daten für {customer} Project {project_id} sind hochgeladen.\n\n"
            f"Die Rawdatalinks-Tabelle wurde unter folgendem Link hochgeladen und freigegeben: "
            f"https://epiquest-zre.s3.{bucket_region}.amazonaws.com/{project_id}/metadata/{run_date}/{random_string}/{project_id}_Rawdatalinks_{run_date}.csv\n"
            f"Tabelle bitte immer einmal öffnen und auf Vollständigkeit überprüfen. "
            f"Dann bei zumindest einem der Links testen, ob beim Öffnen im Browser "
            f"der Download gestartet wird (ohne das du bei AWS eingeloggt bist, am besten Inkognito-Modus)."
        )

    elif customer == "Ventra":
        email_text = (
            f"To: {', '.join(to_recipients)}\n"
            f"Cc: {', '.join(cc_recipients)}\n\n"
            f"Betreff: {subject}\n\n"
            f"Hallo Meike,\n\n"
            f"der Bioinforeport vom Ventraprojekt {project_id} ist da.\n\n"
            f"Der Bioinforeport wurde hier hochgeladen und freigegeben:   "
            f"[<<<<<<INSERT LINK HERE>>>>>>]\n\n"
            f"Die Rawdatalinks-Tabelle wurde unter folgendem Link hochgeladen und freigegeben: "
            f"https://epiquest-zre.s3.{bucket_region}.amazonaws.com/{project_id}/metadata/{run_date}/{random_string}/{project_id}_Rawdatalinks_{run_date}.csv\n"
            f"Tabelle bitte immer einmal öffnen und auf Vollständigkeit überprüfen. "
            f"Dann bei zumindest einem der Links testen, ob beim Öffnen im Browser "
            f"der Download gestartet wird (ohne das du bei AWS eingeloggt bist, am besten Inkognito-Modus)."
        )

    elif customer == "ZRE_Standard_with_BI":
        email_text = (
            f"To: {', '.join(to_recipients)}\n"
            f"Cc: {', '.join(cc_recipients)}\n\n"
            f"Betreff: {subject}\n\n"
            f"Hallo,\n\n"
            f"der Bioinforeport von {customer} {project_id} ist da.\n\n"
            f"Der Bioinforeport wurde hier hochgeladen und freigegeben:   "
            f"[<<<<<<INSERT LINK HERE>>>>>>]\n\n"
            f"Die Rawdatalinks-Tabelle wurde unter folgendem Link hochgeladen und freigegeben: "
            f"https://epiquest-zre.s3.{bucket_region}.amazonaws.com/{project_id}/metadata/{run_date}/{random_string}/{project_id}_Rawdatalinks_{run_date}.csv\n"
            f"Tabelle bitte immer einmal öffnen und auf Vollständigkeit überprüfen. "
            f"Dann bei zumindest einem der Links testen, ob beim Öffnen im Browser "
            f"der Download gestartet wird (ohne das du bei AWS eingeloggt bist, am besten Inkognito-Modus)."
        )

    elif customer == "ExtraPZRE":
        email_text = (
            f"To: {', '.join(to_recipients)}\n"
            f"Cc: {', '.join(cc_recipients)}\n\n"
            f"Betreff: {subject}\n\n"
            f"Liebe Sequenzierrunmanagerin von {run_date},\n\n"
            f"der Report für die Positivkontrollen wurde hier hochgeladen und kann jetzt ausgewertet werden:\n"
            f"[<<<<<<INSERT LINK HERE>>>>>>]\n\n\n"
            f"P.S.:\n"
            f"Falls der Link oben nicht funktioniert, ist er nicht freigegeben worden. "
            f"Die Dateien sollten aber alle zugänglich sein, wenn man bei AWS eingeloggt ist. "
            f"Alle Bioinforeports für die Positivkontrollen werden von ZRC zu folgendem Link hochgeladen: \n"
            f"https://s3.console.aws.amazon.com/s3/buckets/epiquest-zre?prefix=zr0000/report/CAFAHSDFZPA5DHDEKEQYGMWN63MGDTH9/&region=eu-central-1"
        )
    elif customer == "Alba Health":
        email_text = (
            f"To: {', '.join(to_recipients)}\n"
            f"Cc: {', '.join(cc_recipients)}\n\n"
            f"Betreff: {subject}\n\n"
            f"Hallo Lisa,\n\n"
            f"die Daten für {customer} Project {project_id} sind hochgeladen, inklusive einer Negativkontrolle und der Rawdatalinks-Exceltabelle.\n\n"
            f"Die Rawdatalinks-Tabelle wurde zusätzlich unter folgendem Link hochgeladen und freigegeben: "
            f"https://epiquest-zre.s3.eu-central-1.amazonaws.com/{project_id}/metadata/{run_date}/{random_string}/{project_id}_Rawdatalinks_{run_date}.csv\n\n"
            f"Google docs Liste: https://docs.google.com/spreadsheets/d/1yhT8UG5tisq2BpaCNbze2o3boPAWYrZf3whPMTVAhsk/edit#gid=0 \n"
            f"URL zu dem Bucket von Alba: https://s3.console.aws.amazon.com/s3/buckets/s3-external-zymo?region=eu-north-1&tab=objects"
        )
    with open(filename, "w") as file:
        file.write(email_text)
    print(f"Exported {filename}")


if __name__ == "__main__":
    file_path = "project_output_info.csv"
    projects_with_emails = extract_projects_with_emails(file_path)

    for project_ID, project_data in projects_with_emails.items():
        customer = project_data["customer"]
        run_date = project_data["run_date"]
        bucket_region = project_data["bucket_region"]
        random_string = project_data["random_string"]
        export_txt_file(customer, project_ID, run_date, bucket_region, random_string)
