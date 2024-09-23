import csv
import os
import shutil
import sys


# Version 2.30 supports new naming scheme for ZRE-controls: Extra1_PZRE_. It will also work with old naming scheme Extra1_P_
# Version 2.31 reduced printing to make Errors more visible
# Version 2.32 removed restrictions on start of fastq filenames to accomadate things like "Plate". Keeping special treatment for samples that startwith "Extra"
# Version 2.40 improved printing clarity and refactored for maintainability

# CONSTANTS
INPUT_FILE = "project_info.csv"
FASTQ_FOLDER_NAME = "fastq"


def get_project_id(filename):

    # Works for ZRE and ZRC controls.
    def get_extra_project_id(filename):
        if filename.startswith("Extra"):
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

    if filename.startswith("Extra"):
        return get_extra_project_id(filename)

    project_id = filename.split("_")[0]

    return project_id


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


def check_file_counts(project_data, base_path):
    results = []
    all_correct = True

    for project in project_data:
        project_id = project["project_ID"]
        expected_files = project["#samples"] * 2
        project_folder_path = os.path.join(base_path, project_id)
        actual_files = len(os.listdir(project_folder_path))

        if actual_files != expected_files:
            results.append({"project_id": project_id, "actual_files": actual_files, "expected_files": expected_files, "status": "error"})
            all_correct = False
        else:
            results.append({"project_id": project_id, "actual_files": actual_files, "expected_files": expected_files, "status": "ok"})

    return {"all_correct": all_correct, "results": results}


if __name__ == "__main__":

    working_directory = os.getcwd()
    fastq_folder = os.path.join(working_directory, FASTQ_FOLDER_NAME)

    if not os.path.exists(fastq_folder):
        print(f"The FASTQ folder '{FASTQ_FOLDER_NAME}' does not exist in the current directory.")
        sys.exit(1)

    for filename in os.listdir(fastq_folder):
        if filename.endswith(".fastq.gz"):

            destination_folder = get_project_id(filename)
            destination_path = os.path.join(working_directory, destination_folder)

            if not os.path.exists(destination_path):
                os.makedirs(destination_path)

            source_path = os.path.join(fastq_folder, filename)

            try:
                shutil.move(source_path, destination_path)
            except shutil.Error as e:
                print(f"Error moving file: {str(e)}")

    print("Sorting process completed.")
    print()

    # control if the number of files matches the number of samples from  project_info.csv
    project_data, _ = import_input_file_project_data(INPUT_FILE)

    print("Controling if number of files matches the expected number of samples/files defined in project_info.csv:")
    file_count_results = check_file_counts(project_data, working_directory)

    if file_count_results["all_correct"]:
        print("SUCCESS! All projects have the correct number of files.")
    else:
        print("ERRORS found in file counts:")
        for result in file_count_results["results"]:
            if result["status"] == "error":
                print(f"FATAL ERROR!! For project {result['project_id']}: " f"Found {result['actual_files']} files, expected {result['expected_files']}.")
