import os
import csv
import sys
import re


# Version 2.60:
# Script now knows new naming scheme for ZRE-controls. Will also work for old naming scheme.
# New naming scheme: Extra1_PZRE_. Old naming scheme: Extra1_P_. Script should work with ZRC and ZRE controls.
# It will also correctly notice if a filename has already been renamed and recognize it as such.
# For mixed projects with old and new naming scheme, no  problems should arise. Analysis file will be combined of both.
# Will recognize every control called Extra1_P_ as ZRE control. ZRC usually has e.g. Extra1_PA_ or Extra1_PYYMMDD_
# Version 2.74: Projects that cant be renamed are printed to .txt file
# Version 2.74: Fixed filepath for {run_date}_projects_not_renamed.txt
# Version 2.76: Added 'project_info.csv' format check, refactored to looping through files instead of project_ids to determine which files shall be renamed
# Version 2.78: Messages about incorrect numbers of files for projet IDs are displayed before user is asked if he wants to continue anyway
# Version 2.78: fixxed check for unknown project_id in fastq files, allowed spaces in customer value within INPUT_FILE


# CONSTANTS
INPUT_FILE = "project_info.csv"
FASTQ_FOLDER_NAME = "fastq"
ALLOWED_CONTROL_TAGS_ZRE = ["PZRE", "P", "NZRE", "N"]
RENAMING_PATTERN_RE_SUB = r"_S\d+_(.*)_001"
RENAMING_PATTERN_RE_SUB_EXTRA = r"_S\d+_L00\d(.*)_001"


def validate_input_file(input_file):
    errors = []

    try:
        with open(input_file, "r") as file:
            # Check if file is empty
            if file.read(1) == "":
                errors.append("Error: File is empty")
                return errors

            file.seek(0)  # Reset file pointer to beginning

            # Check if file is comma-separated
            dialect = csv.Sniffer().sniff(file.read(1024))
            if dialect.delimiter != ",":
                errors.append("Error: File is not comma-separated")

            file.seek(0)  # Reset file pointer to beginning

            reader = csv.reader(file)
            headers = next(reader)

            # Check for leading/trailing whitespace in headers
            for i, header in enumerate(headers):
                if header != header.strip():
                    errors.append(f"Error: Header '{header}' contains leading or trailing whitespace")

            # Check if 'project_ID' is in headers
            if "project_ID" not in headers:
                errors.append("Error: 'project_ID' column is missing from the headers")
                return errors

            project_id_index = headers.index("project_ID")
            customer_index = headers.index("customer")

            # Check each row
            for row_num, row in enumerate(reader, start=2):  # start=2 because row 1 is headers
                # Check for leading/trailing whitespace
                for i, value in enumerate(row):
                    if value != value.strip():
                        errors.append(f"Error: Row {row_num}, Column {i+1} contains leading or trailing whitespace")

                # Check for forbidden characters (except "-" and "_")
                for i, value in enumerate(row):
                    if i not in [project_id_index, customer_index]:
                        if re.search(r"[^a-zA-Z0-9\-_,]", value):
                            errors.append(f"Error: Row {row_num}, Column {i+1} contains forbidden special characters")
                    elif i == customer_index:  # allowing spaces for customer value
                        if re.search(r"[^a-zA-Z0-9\-_ ,]", value):
                            errors.append(f"Error: Row {row_num}, Column {i+1} contains forbidden special characters")
                    elif i == project_id_index:
                        if not value.isalnum():
                            errors.append(f"Error: Row {row_num}, project_ID '{value}' is not alphanumeric")

    except FileNotFoundError:
        errors.append("Error: File not found")
    except csv.Error:
        errors.append("Error: File is not a valid CSV")
    except Exception as e:
        errors.append(f"Error: An unexpected error occurred - {str(e)}")

    return errors


# function to extract project_id from filename
def get_project_id(filename):
    if filename.startswith("Extra"):
        return get_extra_project_id(filename)
    project_id = filename.split("_")[0]
    return project_id


# Function to extract project ID from the filename of controls. Works for ZRE and ZRC controls.
def get_extra_project_id(filename):
    if filename.startswith("Extra"):
        filename_split = filename.split("_")[1][:]
        extra_project_id = f"Extra{filename_split}"  # Extract project ID from the filename of controls. Works for ZRE and ZRC schemas.
        return extra_project_id


# Function to extract project ID from an already renamed filename of controls. Works for ZRE and ZRC controls.
def get_extra_project_id_from_already_renamed(filename):
    if filename.startswith("Extra"):  # Consider only controls (filename always starts with "Extra").
        # Extract "Extra" from the first part (excluding numbers at the end)
        first_part = filename.split("_")[0]
        if first_part[-1].isdigit():
            extracted_first = first_part.rstrip("0123456789")
        else:
            extracted_first = first_part

        # Extract everything until "L0" from the second part
        second_part = filename.split("_")[1]
        if "L0" in second_part:
            extracted_second = second_part.split("L0")[0]
        else:
            extracted_second = second_part

        extra_project_id = extracted_first + extracted_second
        return extra_project_id


# Function to count the files in the "fastq" folder that match the project ID.
def count_files_matching_project_id(folder_path, project_id):
    # Initialize variables
    matching_files_already_renamed = 0
    matching_files = 0

    for filename in os.listdir(folder_path):

        # Check if file was already renamed
        old_filename = filename
        new_filename = re.sub(RENAMING_PATTERN_RE_SUB, r"\1", filename)
        new_filename_extra = re.sub(RENAMING_PATTERN_RE_SUB_EXTRA, r"\1", filename)

        if old_filename in {new_filename, new_filename_extra}:

            if get_project_id(filename) == project_id:
                matching_files += 1
                matching_files_already_renamed += 1
            elif project_id == get_extra_project_id_from_already_renamed(filename):
                matching_files += 1
                matching_files_already_renamed += 1

        # For controls (filename starts with "Extra").
        elif filename.startswith("Extra"):
            # Extract project ID from the filename of controls. Works for ZRE and ZRC schemas.
            extra_project_id = get_extra_project_id(filename)
            # Does the extracted project ID of the control match one of the project IDs from ZRE or ZRC?
            if project_id == extra_project_id:
                matching_files += 1

        # For all files that are not controls ("Extra"). Count them if the filename starts with one of the project IDs.
        elif get_project_id(filename) == project_id:
            matching_files += 1

    return matching_files, matching_files_already_renamed


# Function to import project data from 'project_info.csv'
def import_input_file_project_data(input_file):

    project_data_dict = []
    run_date = "None"

    with open(input_file, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            project_data = {
                "project_ID": row["project_ID"],
                "#samples": int(row["#samples"]),
                "customer": row["customer"],
                "run_date": row["run_date"],
                "sequencing_ID": row["sequencing_ID"],
                "renaming?": row["renaming?"],
                "concat?": row["concat?"],
                "BI?": row["BI?"],
                "analysis_file?": row["analysis_file?"],
                "md5?": row["md5?"],
            }
            project_data_dict.append(project_data)

            # Set run_date to the last row's run_date
            run_date = row["run_date"]

    return project_data_dict, run_date


def read_csv_and_extract_projects(csv_file):
    all_project_ids = []
    projects_to_be_renamed = []

    with open(csv_file, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            all_project_ids.append(row["project_ID"])
            if row["renaming?"] == "yes":
                projects_to_be_renamed.append(row["project_ID"])

    return all_project_ids, projects_to_be_renamed


# Function to simulate the renaming process and identify projects with potential non-unique filenames
def identify_projects_with_issues(csv_file):
    projects_with_issues = []

    with open(csv_file, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row["renaming?"] == "yes":
                project_ID = row["project_ID"]
                files_to_rename, _ = count_files_matching_project_id(FASTQ_FOLDER_NAME, project_ID)
                unique_filenames_after_rename = set()

                for filename in os.listdir(FASTQ_FOLDER_NAME):
                    # for contols (filename starts with "Extra") the project_ID has to be calculated as they dont start with the project_ID
                    if filename.startswith("Extra"):
                        # handle ZRE Controls which contain "NZRE" or "PZRE" in filename
                        if "NZRE" in filename or "PZRE" in filename:
                            # Extract project ID from the filename of controls. Works for ZRE and ZRC schemas.
                            extra_project_id = get_extra_project_id(filename)
                            extra_project_id_already_renamed = get_extra_project_id_from_already_renamed(filename)

                            # Does the extracted project ID of the control match one of the project IDs from ZRE or ZRC?
                            if project_ID == extra_project_id or project_ID == extra_project_id_already_renamed:
                                new_filename_extra = re.sub(RENAMING_PATTERN_RE_SUB, r"\1", filename)
                                unique_filenames_after_rename.add(new_filename_extra)

                        # handle ZRC Controls which dont contain "NZRE" or "PZRE" in filename
                        else:
                            # Extract project ID from the filename of controls. Works for ZRE and ZRC schemas.
                            extra_project_id = get_extra_project_id(filename)
                            extra_project_id_already_renamed = get_extra_project_id_from_already_renamed(filename)

                            # Does the extracted project ID of the control match one of the project IDs from ZRE or ZRC?
                            if project_ID == extra_project_id or project_ID == extra_project_id_already_renamed:
                                new_filename_extra_ZRC = re.sub(RENAMING_PATTERN_RE_SUB_EXTRA, r"\1", filename)
                                unique_filenames_after_rename.add(new_filename_extra_ZRC)
                    # all other files start with their project_ID
                    elif get_project_id(filename) == project_ID:
                        new_filename = re.sub(RENAMING_PATTERN_RE_SUB_EXTRA, r"\1", filename)
                        unique_filenames_after_rename.add(new_filename)

                # Check if there would be non-unique filenames
                if len(set(unique_filenames_after_rename)) != files_to_rename:
                    # print(f"\nProject {project_ID} cant be renamed due to issues with non unique filenames:\nunique_filenames_after_rename: {len(unique_filenames_after_rename)}\nfiles_to_rename: {files_to_rename}")
                    projects_with_issues.append(project_ID)

    return projects_with_issues


# function to write .txt file if some projects could not be renamed
def log_projects_with_issues(projects_with_issues, run_date, script_directory):
    filename = f"{run_date}_projects_not_renamed.txt"
    filepath = os.path.join(script_directory, filename)
    with open(filepath, "w") as file:
        file.write(f"Projects that can't be renamed due to issues with non-unique filenames:\n")
        for project in projects_with_issues:
            file.write(" - " + project + "\n")


if __name__ == "__main__":

    # Import project data
    project_data_dict, run_date = import_input_file_project_data(INPUT_FILE)

    # get script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # validate 'project_info.csv'
    print(f"\nValidating structure and format of input file '{INPUT_FILE}'.")
    errors = validate_input_file(INPUT_FILE)
    if errors:
        for error in errors:
            print(error)

        user_input = input("Do you want to continue anyway? (yes/no): ").strip().lower()
        if user_input != "yes":
            print("Exiting script.")
            sys.exit()
        else:
            print("Continuing script even though there are errors in 'project_info.csv'...")
    else:
        print(f"No errors found in the input file '{INPUT_FILE}'.")

    # control number of fastq files
    print("\nControlling if the number of files matches the expected number of samples/files defined in 'project_info.csv':")

    # Iterate through project data and compare file counts
    no_number_of_files_errors = True

    total_matching_files_already_renamed = 0
    total_files_that_match_project_IDs = 0

    total_files_in_fastq_folder = len([f for f in os.listdir(FASTQ_FOLDER_NAME) if os.path.isfile(os.path.join(FASTQ_FOLDER_NAME, f))])

    for project_data in project_data_dict:
        project_id = project_data["project_ID"]
        expected_samples = project_data["#samples"]
        expected_files = expected_samples * 2
        nr_files_that_match_project_id, nr_matching_files_already_renamed = count_files_matching_project_id(FASTQ_FOLDER_NAME, project_id)
        total_matching_files_already_renamed += nr_matching_files_already_renamed
        total_files_that_match_project_IDs += nr_files_that_match_project_id

        if nr_files_that_match_project_id != expected_files:
            print(f"Warning!: Project {project_id}: Number of files that match project ID: {nr_files_that_match_project_id}/{expected_files}")
            no_number_of_files_errors = False

    if no_number_of_files_errors:
        print("Number of fastq-files is correct. Script will now rename files.\n")

    else:
        sum_of_expected_files = sum(project_data["#samples"] * 2 for project_data in project_data_dict)

        print()
        print(f"Total expected files: {sum_of_expected_files}")
        print(f"Total files that match project IDs: {total_files_that_match_project_IDs}")
        if nr_files_that_match_project_id != total_files_in_fastq_folder:
            print(f"Actual total number of files in '{FASTQ_FOLDER_NAME}': {total_files_in_fastq_folder}")

        user_input = input("\nERROR! The number of files does not match the expected number of samples for one or more projects. Do you want to continue anyway? (yes/no): ")

        if user_input.lower() != "yes":
            print("Script terminated as per user request.")
            sys.exit(1)
        else:
            print("Continuing with the script...")

    all_project_ids, projects_to_be_renamed = read_csv_and_extract_projects(INPUT_FILE)
    print(f"Projects to be renamed: {', '.join(projects_to_be_renamed)}\n")

    # identify projects that can not be renamed due to issues with non-unique filenames
    projects_with_issues = identify_projects_with_issues(INPUT_FILE)

    # List all files in the fastq folder
    fastq_folder_path = os.path.join(script_dir, FASTQ_FOLDER_NAME)
    files = os.listdir(fastq_folder_path)
    os.chdir(fastq_folder_path)

    # Initiate counters
    files_to_be_renamed = 0
    files_that_could_not_be_renamed = 0
    filenames_that_could_not_be_renamed = []
    files_that_were_successfully_renamed = 0

    # Iterate through the files and check if they start with any of the strings in projects_to_be_renamed
    for filename in files:
        filename_parts = filename.split("_")
        old_filename = filename
        new_filename = re.sub(RENAMING_PATTERN_RE_SUB_EXTRA, r"\1", filename)
        new_filename_extra = re.sub(RENAMING_PATTERN_RE_SUB, r"\1", filename)
        project_id = get_project_id(filename)

        # For controls filename starts with "Extra".
        if filename.startswith("Extra"):
            # Extract project ID from the filename of controls. Works for ZRE and ZRC schemas.
            extra_project_id = get_extra_project_id(filename)
            # Does the extracted project ID of the control match one of the project IDs?
            if extra_project_id.startswith(tuple(projects_to_be_renamed)) and not extra_project_id in projects_with_issues:

                # check for already renamed filenames
                if old_filename == new_filename_extra:
                    pass

                # rename ZRE controls and keep the laneinfo "L00X".
                elif filename_parts[1] in ALLOWED_CONTROL_TAGS_ZRE:
                    files_to_be_renamed += 1
                    os.system("rename -n 's/_S\\d+_(.*)_001/$1/g' {}".format(filename))
                    os.system("rename 's/_S\\d+_(.*)_001/$1/g' {}".format(filename))

                # rename ZRC controls and delete laneinfo "L00X".
                else:
                    files_to_be_renamed += 1
                    os.system("rename -n 's/_S\d+_L00\d(.*)_001/$1/g' {}".format(filename))
                    os.system("rename 's/_S\d+_L00\d(.*)_001/$1/g' {}".format(filename))
            elif not extra_project_id.startswith(tuple(all_project_ids)):
                print(f"Warning! File '{filename}' with extra project id '{extra_project_id}' did not match any project ID. Please find out if something went wrong.")

        # For all files that are not controls ("Extra"). Rename them if the filename starts with one of the project IDs.
        elif project_id in projects_to_be_renamed and project_id not in projects_with_issues:

            # check for already renamed filenames
            if old_filename == new_filename:
                pass

            else:
                # Rename rawdata
                files_to_be_renamed += 1
                os.system("rename -n 's/_S\d+_L00\d(.*)_001/$1/g' {}".format(filename))
                os.system("rename 's/_S\d+_L00\d(.*)_001/$1/g' {}".format(filename))

        elif project_id not in all_project_ids:
            print(f"Warning! File '{filename}' with project_id '{project_id}' did not match any project ID. Please find out if something went wrong.")

    print()

    # Refresh list of all files in the folder after renaming
    files = os.listdir(fastq_folder_path)

    # Check if there are still files that have to be renamed
    for filename in files:
        project_id = get_project_id(filename)
        new_filename = re.sub(RENAMING_PATTERN_RE_SUB_EXTRA, r"\1", filename)
        if filename != new_filename and project_id in projects_to_be_renamed and project_id not in projects_with_issues:
            print(f"ERROR: Filename is {filename}")
            print(f"...but Filename should be {new_filename}")
            files_that_could_not_be_renamed += 1
            filenames_that_could_not_be_renamed.append(filename)

    files_that_were_successfully_renamed = files_to_be_renamed - files_that_could_not_be_renamed

    print()
    print("Data renaming complete!")
    print(f"Files already renamed: {total_matching_files_already_renamed}")
    print(f"Files to be renamed: {files_to_be_renamed}")
    print(f"Files that were successfully renamed: {files_that_were_successfully_renamed}")
    print()
    if projects_with_issues:
        print("Projects that can't be renamed due to issues with non-unique filenames:")
        for project in projects_with_issues:
            print(" - " + project)
        log_projects_with_issues(projects_with_issues, run_date, script_dir)
        print()

    if files_that_could_not_be_renamed > 0:
        print()
        print(f"WARNING!!!! There are {files_that_could_not_be_renamed} files that should be but could not be renamed. Please find out why this is the case: ")
        for filename in filenames_that_could_not_be_renamed:
            print(f"{filename} could not be renamed due to an ERROR.")
        print()
