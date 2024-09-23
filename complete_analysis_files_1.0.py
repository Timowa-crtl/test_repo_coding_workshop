import csv
import os

# Version 1.0: This script completes analysis files by updating the 'UniqueLabel' field for each row based on sample ID mappings derived from corresponding exported CSV files.
# Additionally, it renames analysis files to their respective project IDs in CSV format upon successful addition of unique labels.


def add_unique_labels(analysis_file_path, exported_csv_path):
    rows_updated = 0
    rows_no_match_found = 0

    # Create a dictionary to map sample IDs to sample names
    sample_data = {}
    with open(exported_csv_path, "r") as csv_file:
        exported_csv_rows = csv.DictReader(csv_file)
        for row in exported_csv_rows:
            sample_data[row["sample_id"]] = row["sample_name"]

    # Update UniqueLabel field in analysis file
    analysis_file_data = []
    with open(analysis_file_path, "r") as analysis_file:
        analysis_file_rows = csv.DictReader(analysis_file)

        for row in analysis_file_rows:
            sample_id = row["projectID"] + "_" + row["#num"]
            if sample_id in sample_data and len(sample_data[sample_id]) > 0:
                row["UniqueLabel"] = sample_data[sample_id]
                rows_updated += 1
            else:
                print(f"Missing sample_name for: '{sample_id}'")
                rows_no_match_found += 1
            analysis_file_data.append(row)

    # Write updated data back to the analysis file
    with open(analysis_file_path, "w", newline="") as analysis_file:
        fieldnames = analysis_file_data[0].keys() if analysis_file_data else []
        writer = csv.DictWriter(analysis_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(analysis_file_data)

    return rows_updated, rows_no_match_found


def find_exported_csv(projectID, search_directory):
    exported_csv_path = None
    for file in os.listdir(search_directory):
        if file.lower().startswith("exported_csv") and ("SampleInformation").lower() in file.lower() and projectID.lower() in file.lower() and file.lower().endswith(".csv"):
            exported_csv_path = os.path.join(search_directory, file)
            break
    return exported_csv_path


def main():
    # Set working directory
    script_directory = os.getcwd()

    # Set exports directory
    exported_csv_directory = os.path.join(script_directory, "sample_information_exports")
    # Search for all analysis files and get analysis_file_paths in /Analysis files
    analysis_files_directory = os.path.join(script_directory, "Analysis files")
    for file in os.listdir(analysis_files_directory):
        if file.lower().endswith(".csv") and not file.startswith("zr0000.ZRE"):
            # Get path
            analysis_file_path = os.path.join(analysis_files_directory, file)
            # Get basepath
            analysis_file_basename = os.path.basename(analysis_file_path)

            # Extract project ID from the analysis file name
            filename_without_extension = os.path.splitext(file)[0]
            if "_" in filename_without_extension:
                project_id = file.split("_")[0]
            else:
                project_id = filename_without_extension

            # Find corresponding exported_csv_path
            exported_csv_path = find_exported_csv(project_id, exported_csv_directory)
            if exported_csv_path == None:
                exported_csv_path = find_exported_csv(project_id, script_directory)

            if exported_csv_path:
                # Get basepath
                exported_csv_basename = os.path.basename(exported_csv_path)

                # Update rows
                rows_updated, rows_no_match_found = add_unique_labels(analysis_file_path, exported_csv_path)

                # Check for success
                if rows_no_match_found != 0:
                    print(f"ERROR! Not all UniqueLabels added for '{analysis_file_basename}' found in '{exported_csv_basename}'")
                    print(f"Rows updated: {rows_updated}")
                    print(f"Rows with no match found: {rows_no_match_found}")
                else:
                    print(f"SUCCESS! UniqueLabels added to '{analysis_file_basename}' using '{exported_csv_basename}'")
                    # Rename file to project_id.csv if all unique labels have been added
                    new_filename = f"{project_id}.csv"
                    if analysis_file_basename != new_filename:
                        new_file_path = os.path.join(analysis_files_directory, new_filename)
                        os.rename(analysis_file_path, new_file_path)
                        print(f"File renamed to: {new_filename}")
            else:
                print(f"ERROR! No corresponding exported CSV found for '{analysis_file_basename}'")


if __name__ == "__main__":
    main()
