import csv

def extract_ZOE_projects(file_path):
    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            customer = row['customer']
            project_id = row['project_ID']
            samples = row['#samples']
            if customer == 'ZOE':
                ZOE_projects[project_id] = samples
                project_ids.append(project_id)
            elif customer == 'ZOE-Redo':
                ZOE_redo_projects[project_id] = samples
                project_ids.append(project_id)

    return ZOE_projects, ZOE_redo_projects, project_ids


def format_ZOE_project_string(project_id, samples, run_date):
    formatted_string = (
        f"{counter}. "
        f"for {project_id} data for {samples} samples was uploaded. "
        f"The metadata table is uploaded to s3://zymo-zoe/metadata/{project_id}_SampleInformationForm_{run_date}.csv, "
        f"and the fastq files are uploaded to s3://zymo-zoe/fastq/{project_id}/{run_date}/."
    )
    return formatted_string


def format_ZOE_redo_project_string(project_id, samples, run_date):
    formatted_string = (
        f"{counter}. "
        f"for {project_id} data for {samples} samples was uploaded (previously low-reads)."
        f"The metadata table is uploaded to s3://zymo-zoe/metadata/{project_id}_SampleInformationForm_{run_date}.csv, "
        f"and the fastq files are uploaded to s3://zymo-zoe/fastq/{project_id}/{run_date}/."
    )
    return formatted_string


if __name__ == "__main__":
    run_date = input("Please enter the rundate in the format YYMMDD: ")
    project_ids = []
    ZOE_projects = {}
    ZOE_redo_projects = {}

    file_path = 'project_info.csv'
    ZOE_projects, ZOE_redo_projects, project_ids = extract_ZOE_projects(file_path)

    print("ZOE_projects:", ZOE_projects)
    print("ZOE_redo_projects:", ZOE_redo_projects)
    print(f"Project_IDs are: {project_ids}")

    counter = 1

    project_ids_comma_seperated = ', '.join(project_ids)

    output_textfilename = (f"{run_date}_Data available on S3_{project_ids_comma_seperated}.txt")

    # Open the file in write mode and store the output in it
    with open(output_textfilename, "w") as f:

        #Printing Hello Customer
        f.write("Dear Daniela, dear Meaghan,\n\n")

        # Print the metadata files and sequencing-data for projects
        f.write("New metadata files and sequencing-data for projects ")
        f.write(", ".join(project_ids[:-1]))  # Print all project IDs except the last one with a comma and space separator
        if len(project_ids) > 1:
            f.write(", and ")  # Add ", and " before the last project ID if there are multiple projects
        f.write(f"{project_ids[-1]} are now available on s3.\n\n")  # Print the last project ID

        # Printing formatted strings for ZOE projects
        for project_id, samples in ZOE_projects.items():
            formatted_str = format_ZOE_project_string(project_id, samples, run_date)
            f.write(formatted_str)
            f.write("\n\n")
            counter += 1

        # Printing formatted strings for ZOE-Redo projects
        for project_id, samples in ZOE_redo_projects.items():
            formatted_str = format_ZOE_redo_project_string(project_id, samples, run_date)
            f.write(formatted_str)
            f.write("\n\n")
            counter += 1

        f.write("If you encounter any issues or have any questions regarding the data upload, please do not hesitate to reach out to us.")

    print()
    print(f"Please copy the text from {output_textfilename} to Outlook and contact ZOE after you uploaded the data.")