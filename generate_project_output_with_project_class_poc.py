from customer_configurations import customers_configs
from project_class import Project
from dataclasses import dataclass
from typing import Dict, List, Callable
import csv
import os
import random
import string


# Constants
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


def main(input_file, output_file, header_output_file):
    with open(input_file, "r") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    if len(rows) > 0:
        first_row = rows[0]
        sequencing_id_global = first_row.get("sequencing_ID", "sequencing_ID not defined")
        run_date_global = first_row.get("run_date", "run_date not defined")
        print(f"sequencing_ID: {sequencing_id_global}")
        print(f"run_date: {run_date_global}")

    projects = []
    for row in rows:
        customer = row["customer"]
        customer_config = None

        for key, config in customers_configs.items():
            if (isinstance(key, tuple) and customer in key) or (customer == key):
                customer_config = config
                break

        if customer_config:
            project = Project.from_row_and_config(row, customer_config, sequencing_id_global, run_date_global)
            projects.append(project)

        else:
            print(
                f"WARNING: Customer '{customer}' for project-ID '{row['project_ID']}' is not defined. Data is not uploaded. Check 'customer_info' in 'Upload_Data_Info.xlsm' for available customers."
            )

    with open(output_file, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=header_output_file)
        writer.writeheader()
        for project in projects:
            writer.writerow(
                {
                    "upload_priority": project.upload_priority,
                    "project_ID": project.project_ID,
                    "#samples": project.samples,
                    "customer": project.customer,
                    "run_date": project.run_date,
                    "sequencing_ID": project.sequencing_ID,
                    "renaming?": project.renaming,
                    "metagenomics_BI?": project.metagenomics_BI,
                    "metatranscriptomics_BI?": project.metatranscriptomics_BI,
                    "epigenetics_BI?": project.epigenetics_BI,
                    "total_rna_BI?": project.total_rna_BI,
                    "random_string": project.random_string,
                    "local_path": project.local_path,
                    "local_path_metadata": project.local_path_metadata,
                    "expected_objects": project.expected_objects,
                    "upload path fastq 1": project.upload_path_fastq_1,
                    "upload path fastq 2": project.upload_path_fastq_2,
                    "upload path metadata 1": project.upload_path_metadata_1,
                    "upload path metadata 2": project.upload_path_metadata_2,
                    "upload_command_fastq_1": project.upload_command_fastq_1,
                    "upload_command_fastq_2": project.upload_command_fastq_2,
                    "upload_command_metadata_1": project.upload_command_metadata_1,
                    "upload_command_metadata_2": project.upload_command_metadata_2,
                    "uploadpath_rawdatalinks": project.uploadpath_rawdatalinks,
                    "bucket_region": project.bucket_region,
                }
            )

    print(f"\n'{output_file}' generated successfully.")


if __name__ == "__main__":
    main(INPUT_FILE, OUTPUT_FILE, HEADER_OUTPUT_FILE)
