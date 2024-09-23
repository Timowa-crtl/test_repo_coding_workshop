import os
import csv
import sys

# Version 1.50 is able to work with new naming scheme for ZRE-controls: Extra1_PZRE_. It will also work with old naming scheme Extra1_P_
# Version 1.53 Bugfix: Analyis file #num will correspond to Sample ID e.g. zr11522-3 --> #num 3
# Version 1.55 added check for unrenamed fastq-files
# Version 1.56 bugfix for "renamed referenced before assignment"
# Version 1.60 changed "zr0000.ZRE_CHECK_FILE_CONTENT_AND_RENAME.csv" name to "zr0000.ZRE.csv"


# Function to extract projectID and num from filenames
def extract_projectID_and_num(filename):
    if filename.startswith("Extra") and "P" in filename:
        parts = filename.split("_")
        projectID = parts[0]  # Extracts the first part
        num = parts[1]  # Extracts the second part
        if num.startswith("P"):
            return {"projectID": projectID, "num": num}
        else:
            return None


# Function to get projectIDs and nums of files in the "ExtraP" and/or "ExtraPZRE" folder. Function can take variable number of arguments
def list_project_IDs_and_nums(*folder_paths):
    project_IDs_and_nums = []
    for folder_path in folder_paths:
        for filename in os.listdir(folder_path):
            if os.path.isfile(
                os.path.join(folder_path, filename)
            ) and filename.startswith("Extra"):
                info = extract_projectID_and_num(filename)
                if info:
                    project_IDs_and_nums.append(info)
    return project_IDs_and_nums


# Remove duplicates from the list of project IDs and nums
def remove_duplicates(project_IDs_and_nums):
    seen = set()
    unique_project_IDs_and_nums = []
    for item in project_IDs_and_nums:
        # Convert the item to a frozenset to make it hashable
        frozen_item = frozenset(item.items())
        if frozen_item not in seen:
            seen.add(frozen_item)
            unique_project_IDs_and_nums.append(item)
    return unique_project_IDs_and_nums


# Always create the analysis file for ExtraP called "zr0000.ZRE.csv"
def create_and_save_ExtraP_csv(run_date):
    # Check if the "ExtraP" folder exists
    extraP_folder = os.path.join(os.getcwd(), "ExtraP")
    ExtraP = os.path.exists(extraP_folder)

    # Check if the "ExtraPZRE" folder exists
    extraPZRE_folder = os.path.join(os.getcwd(), "ExtraPZRE")
    ExtraPZRE = os.path.exists(extraPZRE_folder)

    if ExtraP and ExtraPZRE:
        # print("Both ExtraP and ExtraPZRE are true.")
        project_IDs_and_nums = list_project_IDs_and_nums(
            extraP_folder, extraPZRE_folder
        )
        if (
            check_if_files_have_been_renamed("ExtraP", extraP_folder) == False
            or check_if_files_have_been_renamed("ExtraPZRE", extraPZRE_folder) == False
        ):
            renamed = False
        else:
            renamed = True

    elif ExtraPZRE:
        # print("ExtraPZRE is true, but ExtraP is false.")
        project_IDs_and_nums = list_project_IDs_and_nums(extraPZRE_folder)
        if check_if_files_have_been_renamed("ExtraPZRE", extraPZRE_folder) == False:
            renamed = False
        else:
            renamed = True

    elif ExtraP:
        # print("ExtraP is true, but ExtraPZRE is false.")
        project_IDs_and_nums = list_project_IDs_and_nums(extraP_folder)
        if check_if_files_have_been_renamed("ExtraP", extraP_folder) == False:
            renamed = False
        else:
            renamed = True
    else:
        print("Error! No Positive Control found!")
        sys.exit()

    # removing duplicates
    project_IDs_and_nums = remove_duplicates(project_IDs_and_nums)

    # Sort the project_IDs_and_nums based on the numerical value in "Project_ID"
    project_IDs_and_nums.sort(
        key=lambda x: int(x["projectID"].split("Extra")[1].split("_")[0])
    )

    # Data for the CSV
    data = []
    for item in project_IDs_and_nums:
        projectID = item["projectID"]
        num = item["num"]
        data.append(
            [
                num,
                projectID,
                f"novaseq{run_date}",
                "PositiveControls",
                "illumina.pe",
                f"{projectID}.{num}",
                "",
            ]
        )

    # CSV file name
    filename = f"zr0000.ZRE.csv"
    if renamed == False:
        filename = f"zr0000.ZRE_ERROR_WARNING_FILES_NOT_RENAMED.csv"

    # Create the "Analysis file" subfolder if it doesn't exist
    analysis_folder = os.path.join(os.getcwd(), "Analysis files")
    if not os.path.exists(analysis_folder):
        os.makedirs(analysis_folder)

    # Combine the subfolder path with the filename
    filepath = os.path.join(analysis_folder, filename)

    # Write data to the CSV file
    with open(filepath, mode="w", newline="") as file:
        writer = csv.writer(file)
        # Write the header
        writer.writerow(
            [
                "#num",
                "projectID",
                "RunID",
                "GroupID",
                "SeqType",
                "UniqueLabel",
                "Subgroup1",
            ]
        )
        # Write the data rows
        writer.writerows(data)

    print(
        f"Analysis file '{filename}' created and saved successfully in the 'Analysis files' subfolder."
    )
    print()


# Function to create the individual analysis files
def create_csv(sample_numbers, num_rows, project_id, run_date, renamed):
    output_file = f"{project_id}_ADD_UNIQUE_LABELS.csv"
    if renamed == False:
        output_file = f"{project_id}_WARNING_FILES_NOT_RENAMED.csv"
    with open(os.path.join(analysis_folder, output_file), "w", newline="") as csvfile:
        fieldnames = [
            "#num",
            "projectID",
            "RunID",
            "GroupID",
            "SeqType",
            "UniqueLabel",
            "Subgroup1",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(1, num_rows + 1):
            row_data = {
                "#num": sample_numbers[i - 1],
                "projectID": f"{project_id}",
                "RunID": f"novaseqx{run_date}",
                "GroupID": "00...AllSamples",
                "SeqType": "illumina.pe",
                "UniqueLabel": "",
                "Subgroup1": "",
            }
            writer.writerow(row_data)


def extract_sample_numbers(files_in_subfolder):
    sample_numbers = set()

    for file_name in files_in_subfolder:
        # Split the file name using '_' and '.' as separators
        parts = file_name.split("_")

        # Check if the filename has the expected format
        if len(parts) >= 2:
            try:
                # Try to convert the second part to an integer
                sample_number = int(parts[1])
                sample_numbers.add(sample_number)
            except ValueError:
                print(
                    f"ERROR!! The filename '{file_name}' does not have the correct format!"
                )
                pass  # Ignore if conversion to integer fails

    return list(sample_numbers)


def check_if_files_have_been_renamed(project_id, project_subfolder):
    # Get the list of files in the project subfolder
    files_in_subfolder = os.listdir(project_subfolder)

    # Initialize a flag to track if any file is not renamed
    any_not_renamed = False

    for filename in files_in_subfolder:
        # Count the number of underscores in the filename
        num_underscores = filename.count("_")

        if num_underscores != 2:
            print(f"ERROR!: {filename} has not been renamed. Warning!")
            any_not_renamed = True

    if any_not_renamed:
        print(
            f"{project_id}: ERROR! Some files have not been renamed correctly! Check for files with non unique filenames! ERROR!"
        )
        return False  # At least one file is not renamed
    else:
        return True  # All files are renamed


if __name__ == "__main__":

    # define input_file
    input_file = "project_info.csv"

    # Read the project_info.csv file and extract project IDs that need analysis-files
    analysis_folder = os.path.join(os.getcwd(), "Analysis files")
    project_ids_with_analysis = []

    with open(input_file, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)
        first_row = rows[0]
        run_date = first_row.get("run_date", "run_date not defined")

        for row in rows:
            project_id = row["project_ID"]
            analysis_file = row["analysis_file?"]
            customer = row["customer"]
            if (
                analysis_file == "yes"
                and customer != "ExtraP"
                and customer != "ExtraPZRE"
            ):
                project_ids_with_analysis.append(project_id)

    print()
    print(f"Run_date: {run_date}")
    print()
    print(f"These projects need analysis-files: {project_ids_with_analysis}")
    print()

    # Call the function with the provided run_date to create the analysis files
    # Always create the analysis file for ExtraP
    create_and_save_ExtraP_csv(run_date)

    # Create analysis files for the specified projects
    for project_id in project_ids_with_analysis:
        project_subfolder = os.path.join(os.getcwd(), project_id)

        # Check if the project subfolder exists
        if os.path.exists(project_subfolder) and os.path.isdir(project_subfolder):

            renamed = check_if_files_have_been_renamed(project_id, project_subfolder)

            # Get the list of files in the project subfolder with the same project_ID
            files_in_subfolder = [f for f in os.listdir(project_subfolder)]

            sample_numbers = extract_sample_numbers(files_in_subfolder)
            if len(sample_numbers) < 1:
                print(f"ERROR!! Project ID {project_id} does not have any files!!")

            print(f"{project_id}: sample_numbers: {sample_numbers}")

            # Determine how many rows to create (half as many as the number of files)
            num_rows = len(files_in_subfolder) // 2

            # Call the function to create the CSV file
            create_csv(sample_numbers, num_rows, project_id, run_date, renamed)

            print(
                f"Analysis file '{project_id}_ADD_UNIQUE_LABELS.csv' with {num_rows} rows created and saved successfully in the 'Analysis files' subfolder."
            )
            print()
        else:
            print(
                f"Project subfolder '{project_subfolder}' does not exist or is not a directory."
            )
            print()
