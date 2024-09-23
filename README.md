# Shotgun Sequencing Data Processing Pipeline

## Overview

This pipeline automates the processing and uploading of shotgun sequencing data. It handles tasks such as renaming files, concatenating low-read samples, sorting data by project, generating metadata and analysis files, and uploading data to cloud storage.

A shot description as well as a change history of each script is found in the top comment of each script.

## Setup

1. Install python libraries listed in requirements.txt
2. Configure AWS and Google Cloud credentials.

## Usage

Refer to "YYMMDD_Instructions_&_Checklist_1.45.xlsx" for instructions on using the pipeline.

## Important Notes

- Always check script outputs and logs for errors.
- Pay attention to specific requirements for different customers and project types.

## Troubleshooting

Some additioal scripts for troubleshooting tasks can be found in /troubleshooting

## Maintenance

- Fastq-Data from upload folders may be deleted after 4 weeks. Run "delete_all_fastq_files.py" to locate and delete all fastq-files within current directory and subdirectories.