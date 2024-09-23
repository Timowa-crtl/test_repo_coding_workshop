import os
import pandas as pd
import string
import fileinput
import csv

# Version 1.3 Added ability to check Rawdatalinks in "_metadata"-Folders, too.
# Version 1.48 Added removal of trailing and leading whitespace as well as replacement of ";" with ","
# Version 1.49 Now also checking rawdatalinks.csv in .../Rawdatalinks
# Version 1.50 When rawdatalinks.csv in .../Rawdatalinks are fine, all of them are copied to {projectID_metadata} for an automated upload
# Version 1.51 Added double-check of sample_id/customer_label. Compares pair with exported_csv files if present.


def replace_semicolon_with_comma(input_folder):
    if not os.path.exists(input_folder):
        print(f"ERROR: The folder '{input_folder}' does not exist.")
        return

    for root, dirs, files in os.walk(input_folder):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                with fileinput.FileInput(file_path, inplace=True) as f:
                    for line in f:
                        print(line.replace(";", ","), end="")


def remove_whitespace(csv_file_path):
    with open(csv_file_path, "r", newline="") as f:
        reader = csv.reader(f)
        rows = [[value.strip() for value in row] for row in reader]

    with open(csv_file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)


def is_good_sample_id(sample_id, csv_file):
    return_value = True
    no_sample_id = False
    # Check if customer_label is empty
    if pd.isnull(sample_id) or pd.isna(sample_id) or sample_id.strip() == "":
        print(f"ERROR!!  Cells in SampleID column of {csv_file} are empty cells!")
        return_value = False
        no_sample_id = True

    # Define a set of allowed characters: uppercase letters, numbers, and "."
    allowed_characters = set(string.ascii_uppercase + string.ascii_lowercase + string.digits + ".")

    if no_sample_id != True:
        for char in sample_id:
            if char not in allowed_characters:
                print(f"ERROR!!  Forbidden character {char} in {csv_file} !")

        if all(char in allowed_characters and char != " " for char in sample_id) == False:
            return_value = False

        # Check for space at the beginning
        if sample_id.startswith(" "):
            print(f"ERROR!!  SampleID startwith 'space' in {csv_file} !")
            return_value = False

        return return_value


def is_good_customer_label(customer_label, csv_file):
    return_value = True
    no_customer_label = False

    # Check if customer_label is empty
    if pd.isnull(customer_label) or pd.isna(customer_label) or str(customer_label).strip() == "":
        print(f"ERROR!!  Cells in 'customer_label' column of {csv_file} are empty cells!")
        return_value = False
        no_customer_label = True

    if no_customer_label != True:
        # Define a set of allowed characters: uppercase letters, numbers, and "."
        allowed_characters = set(string.ascii_uppercase + string.ascii_lowercase + string.digits + ".")
        for char in str(customer_label):
            if char not in allowed_characters:
                print(f"ERROR!!  Forbidden character '{char}' in {csv_file} !")
        if all(char in allowed_characters and char != " " for char in str(customer_label)) == False:
            return_value = False

        # Check for space at the beginning
        if str(customer_label).startswith(" "):
            print(f"ERROR!!  Customer Label startswith 'space' in {csv_file} !")
            return_value = False

    return return_value


def move_to_projectID_metadata(script_dir, file_path):
    try:
        # Check if the file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file '{file_path}' does not exist.")

        # Extract project ID from file name
        file_name = os.path.basename(file_path)
        project_ID = file_name.split("_")[0]

        # Create the target directory path
        target_dir_path = os.path.join(script_dir, f"{project_ID}_rawdatalinks")

        # Ensure the target directory exists
        if not os.path.exists(target_dir_path):
            os.makedirs(target_dir_path, exist_ok=True)

        # Move the file
        move_to_path = os.path.join(target_dir_path, file_name)

        if file_path != move_to_path:
            os.rename(file_path, move_to_path)
            print(f"Moved {file_name} to {project_ID}_rawdatalinks")
    except Exception as e:
        print(f"Error: {e}")


def find_exported_csv(script_dir, project_id):
    exported_csv_dir = os.path.join(script_dir, "sample_information_exports")
    if os.path.exists(exported_csv_dir):
        for file in os.listdir(exported_csv_dir):
            if file.startswith(f"exported_csv") and project_id in file and file.endswith(".csv"):
                return os.path.join(exported_csv_dir, file)
    return None


def double_check_metadata(metadata_file, exported_csv):
    try:
        metadata_df = pd.read_csv(metadata_file)
        exported_df = pd.read_csv(exported_csv)

        # Check if metadata file contains required columns
        if "sample_id" not in metadata_df.columns or "customer_label" not in metadata_df.columns:
            return "Check not possible."

        # Create a dictionary from the exported CSV for quick lookup
        exported_dict = exported_df.set_index("sample_id")["sample_name"].to_dict()

        # Check each sample_id and sample_name in the metadata file
        errors_found = False
        for _, row in metadata_df.iterrows():
            sample_id = row["sample_id"]
            metadata_customer_label = row["customer_label"]

            # Skip if sample_id is not in the exported CSV
            if sample_id not in exported_dict:
                print(f"sample_id '{sample_id}' not found in exported CSV.")
                errors_found = True
                continue

            exported_sample_name = exported_dict[sample_id]

            # Compare customer_label
            if metadata_customer_label != exported_sample_name:
                print(f"Mismatch for sample_id '{sample_id}': customer_label '{metadata_customer_label}' in metadata-file does not match sample_name in exported_csv '{exported_sample_name}'.")
                errors_found = True

        if not errors_found:
            # print(f"All sample_id and customer_label pairs in {os.path.basename(metadata_file)} match the exported CSV.")
            return True
        else:
            return False

    except Exception as e:
        print(f"An error occurred while double-checking {os.path.basename(metadata_file)}: {str(e)}")
        return False


def process_csv_file(csv_file, csv_file_path, check_function, column_name, expected_header, exported_csv):

    # Remove whitespace
    remove_whitespace(csv_file_path)
    try:
        df = pd.read_csv(csv_file_path)
    except pd.errors.EmptyDataError:
        print(f"Empty or invalid CSV file: {csv_file}")
        return False

    # Check for any empty rows
    empty_rows = df[df.isnull().all(axis=1)]

    if empty_rows.empty:
        pass
    else:
        print(f"ERROR!!  There are {len(empty_rows)} empty rows in {csv_file}. Open in text-editor and remove empty rows.")

    # Check if the header is correct
    actual_header = list(df.columns)
    if actual_header != expected_header:
        print(f"ERROR!!  Incorrect header in {csv_file}. Expected: {expected_header}, Actual: {actual_header}")
        return False

    # Check if the specified column exists in the DataFrame
    if column_name not in df.columns:
        print(f"ERROR!!  '{column_name}' column not found in {csv_file}.")
        return False

    # Perform double check if an exported CSV is provided
    if exported_csv:
        double_check_success = double_check_metadata(csv_file_path, exported_csv)
        if not double_check_success:
            print(f"Double check with exported CSV failed for {csv_file}.")
            print(f"ERROR!!  {csv_file} looks bad!")
            return False
        elif double_check_success == "Check not possible.":
            print(f"Please note: Metadata file {csv_file} did not contain 'sample_id' and 'customer_label' columns. Can't be double-checked in comparison to exported CSV. Continuing anyway...")
    else:
        print(f"Please note: No exported_csv file present for double_checking customer_lables in {csv_file}. Continuing anyway...")

    # Apply the check function to each row in the specified column
    check_results = df[column_name].apply(lambda x: check_function(x, csv_file))

    # Print the result of the check function
    if all(check_results):
        print(f"SUCCESS! {csv_file} looks good.")
        return True
    else:
        print(f"ERROR!!  {csv_file} looks bad!")
        return False


if __name__ == "__main__":
    # Get the current directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Define Headers
    header_sample_information_form = ["ZymoID", "SampleID", "Read1 Path", "Read2 Path", "Read1 md5", "Read2 md5"]
    header_rawdatalinks = ["sample_id", "customer_label", "Read1 Download", "Read2 Download"]

    # Iterate over all subdirectories in the current directory
    for subdir, _, _ in os.walk(script_dir):

        # Check if a directory name ends with "_metadata"
        if subdir.endswith("_metadata"):
            project_id = os.path.basename(subdir).split("_")[0]
            replace_semicolon_with_comma(subdir)

            # Get a list of all CSV files in the current directory
            csv_files_sample_information_form = [filename for filename in os.listdir(subdir) if filename.endswith(".csv") and "SampleInformationForm" in filename]
            csv_files_rawdata_links = [filename for filename in os.listdir(subdir) if filename.endswith(".csv") and "Rawdatalinks" in filename]

            # Iterate over the csv_files_sample_information_form and process them
            for csv_file in csv_files_sample_information_form:
                print()
                csv_file_path = os.path.join(subdir, csv_file)
                exported_csv = find_exported_csv(script_dir, project_id)
                success = process_csv_file(csv_file, csv_file_path, is_good_sample_id, "SampleID", header_sample_information_form, exported_csv)

            # Iterate over the csv_files_rawdata_links and process them
            for csv_file in csv_files_rawdata_links:
                print()
                csv_file_path = os.path.join(subdir, csv_file)
                exported_csv = find_exported_csv(script_dir, project_id)
                success = process_csv_file(csv_file, csv_file_path, is_good_customer_label, "customer_label", header_rawdatalinks, exported_csv)

        # Check if the directory name ends with "rawdatalinks"
        if subdir.lower().endswith("rawdatalinks"):
            project_id = os.path.basename(subdir).split("_")[0]
            replace_semicolon_with_comma(subdir)

            # Get a list of all CSV files in the current directory
            csv_files_rawdata_links = [filename for filename in os.listdir(subdir) if filename.endswith(".csv") and "Rawdatalinks" in filename]

            # Iterate over the csv_files_rawdata_links and process them
            for csv_file in csv_files_rawdata_links:
                print()
                csv_file_path = os.path.join(subdir, csv_file)
                exported_csv = find_exported_csv(script_dir, project_id)
                success = process_csv_file(csv_file, csv_file_path, is_good_customer_label, "customer_label", header_rawdatalinks, exported_csv)
                if success:
                    move_to_projectID_metadata(script_dir, csv_file_path)
