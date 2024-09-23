import os
import pandas as pd
import re
import sys
import fileinput
from datetime import datetime

# Version 1.0 This script performs format checks on CSV files in the "Analysis Files" folder.
# It always replaces semicolons with commas. It validates sample table formats, checks for illegal characters, ensures uniqueness of sample IDs and labels, and verifies filename conventions.
# The script generates a report of any errors found and provides a summary of good and bad files.
# Version 1.5 Complete refactor to integrate a double check with exported_csv files in case they are present seamlessly.


def replace_semicolon_with_comma(file_path):
    with fileinput.FileInput(file_path, inplace=True) as f:
        for line in f:
            print(line.replace(";", ","), end="")


def is_valid_seq_type(seq):
    seq_types = ["illumina.pe"]
    return any(re.search(seq_type, seq) for seq_type in seq_types)


def contains_illegal_letters(value):
    return value and not re.match(r"^[a-zA-Z0-9\.]*$", value.strip())


def check_filename(filename):
    forbidden_symbols = ["_", "?", "!", " ", "#", "a", "b"]
    for symbol in forbidden_symbols:
        if symbol in filename:
            return f"ERROR: Filename '{filename}' contains forbidden symbol: '{symbol}'"
    return None


def get_project_id(df):
    if "projectID" not in df.columns:
        return None, "ERROR: The 'projectID' column is missing from the CSV file."

    unique_project_ids = df["projectID"].unique()

    # catch ExtraN/P which can't be double checked
    for id in unique_project_ids:
        if "extra" in id.lower():
            return "ExtraPZRE", None

    if len(unique_project_ids) != 1:
        return None, f"ERROR: Multiple project IDs found: {unique_project_ids}"
    else:
        project_id = unique_project_ids[0]

    return project_id, None


def find_exported_csv(script_dir, project_id):
    exported_csv_dir = os.path.join(script_dir, "sample_information_exports")
    if os.path.exists(exported_csv_dir):
        for file in os.listdir(exported_csv_dir):
            if file.startswith(f"exported_csv") and project_id in file and file.endswith(".csv"):
                return os.path.join(exported_csv_dir, file)
    return None


def double_check_unique_labels(analysis_df, exported_csv):
    try:
        exported_df = pd.read_csv(exported_csv)
        exported_dict = exported_df.set_index("sample_id")["sample_name"].to_dict()

        errors = []
        for _, row in analysis_df.iterrows():
            sample_id = f"{row['projectID']}_{row['#num']}"
            unique_label = row["UniqueLabel"]

            if sample_id not in exported_dict:
                errors.append(f"ERROR: sample_id '{sample_id}' not found in exported CSV.")
                continue

            exported_sample_name = exported_dict[sample_id]
            if unique_label != exported_sample_name:
                errors.append(f"ERROR: Mismatch for sample_id '{sample_id}': UniqueLabel '{unique_label}' in analysis-file does not match sample_name in exported_csv '{exported_sample_name}'.")

        return errors
    except Exception as e:
        return [f"ERROR: An error occurred while double-checking: {str(e)}"]


def check_file(analysis_file, script_dir):
    errors = []
    notes = []

    # Check filename
    filename_error = check_filename(os.path.basename(analysis_file))
    if filename_error:
        errors.append(filename_error)

    # Replace semicolons with commas
    replace_semicolon_with_comma(analysis_file)

    try:
        df = pd.read_csv(analysis_file)
    except Exception as e:
        errors.append(f"ERROR: Failed to read the CSV file: {str(e)}")
        return errors

    # Check project ID
    project_id, project_id_error = get_project_id(df)
    if project_id_error:
        errors.append(project_id_error)

    # Check sample table format
    if len(df.columns) < 5:
        errors.append("ERROR: The sample table should have at least five columns.")

    groups = {}
    for _, row in df.iterrows():
        group_id = f"{row['GroupID']}_{row['SeqType']}"
        sample_id = f"{row['projectID']}_{row['#num']}"
        label = row["UniqueLabel"]

        if not is_valid_seq_type(row["SeqType"]):
            errors.append(f"ERROR: sample '{sample_id}' has an unknown seq_type, {row['SeqType']}!")

        for value in row:
            if contains_illegal_letters(str(value)):
                errors.append(f"ERROR: '{value}' in the row of sample '{sample_id}' contains illegal letters!")

        if group_id not in groups:
            groups[group_id] = {"id": set(), "label": set()}

        if sample_id in groups[group_id]["id"]:
            errors.append(f"ERROR: Group '{group_id}', sample '{sample_id}', is not unique!")
        groups[group_id]["id"].add(sample_id)

        if label in groups[group_id]["label"]:
            errors.append(f"ERROR: Group '{group_id}', label '{label}', is not unique!")
        groups[group_id]["label"].add(label)

    # Double check unique labels
    if project_id and project_id != "ExtraPZRE":
        exported_csv = find_exported_csv(script_dir, project_id)
        if exported_csv:
            errors.extend(double_check_unique_labels(df, exported_csv))
        else:
            notes.append(f"Please note: No exported_csv file present for double_checking UniqueLabels in '{os.path.basename(analysis_file)}'. Continuing anyway...")

    return errors, notes


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_folder = os.path.join(script_dir, "Analysis Files")

    if not os.path.exists(input_folder):
        print(f"Error: The folder '{input_folder}' does not exist.")
        sys.exit(1)

    current_date = datetime.now().strftime("%y%m%d")
    report_file = os.path.join(input_folder, f"{current_date}_analysis_files_check_report.txt")

    good_files = 0
    bad_files = 0

    with open(report_file, "w", encoding="utf-8") as report:
        for filename in os.listdir(input_folder):
            if filename.endswith(".csv"):
                file_path = os.path.join(input_folder, filename)
                report.write(f"\n------------Checking {filename}-----------\n")
                print(f"\nChecking {filename}")

                errors, notes = check_file(file_path, script_dir)

                # Log results
                if notes:
                    for note in notes:
                        report.write(f"{note}\n")
                        print(note)
                if errors:
                    for error in errors:
                        report.write(f"{error}\n")
                        print(error)
                else:
                    success_message = f"SUCCESS! No errors found in '{os.path.basename(filename)}'"
                    report.write(f"{success_message}\n")
                    print(success_message)

                if errors:
                    bad_files += 1
                else:
                    good_files += 1

    # Print summary
    all_files = good_files + bad_files
    print(f"\nBad files: {bad_files}/{all_files}")
    print(f"Good files: {good_files}/{all_files}")
    if bad_files == 0:
        print("SUCCESS! All files seem to be fine!")
    else:
        print("WARNING! Some files failed the sanity check!")


if __name__ == "__main__":
    main()
