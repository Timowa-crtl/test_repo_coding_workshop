import csv
import os

# Version 1.0: This script completes rawdatalinks files by updating the 'customer_lable' field for each row based on sample ID mappings derived from corresponding exported CSV files.
# Additionally, it renames rawdatalinks files to their respective project IDs in CSV format upon successful addition of unique labels.


def add_customer_lables(rawdatalinks_file_path, exported_csv_path):
    rows_updated = 0
    rows_no_match_found = 0

    # Create a dictionary to map sample IDs to sample names
    sample_data = {}
    with open(exported_csv_path, "r") as csv_file:
        exported_csv_rows = csv.DictReader(csv_file)
        for row in exported_csv_rows:
            sample_data[row["sample_id"]] = row["sample_name"]

    # Update customer_lable field in rawdatalinks file
    rawdatalinks_file_data = []
    with open(rawdatalinks_file_path, "r") as rawdatalinks_file:
        rawdatalinks_file_rows = csv.DictReader(rawdatalinks_file)

        for row in rawdatalinks_file_rows:
            sample_id = row["sample_id"]
            if sample_id in sample_data and len(sample_data[sample_id]) > 0:
                row["customer_label"] = sample_data[sample_id]
                rows_updated += 1
            else:
                print(f"Missing sample_name for: '{sample_id}'")
                rows_no_match_found += 1
            rawdatalinks_file_data.append(row)

    # Write updated data back to the rawdatalinks file
    with open(rawdatalinks_file_path, "w", newline="") as rawdatalinks_file:
        fieldnames = rawdatalinks_file_data[0].keys() if rawdatalinks_file_data else []
        writer = csv.DictWriter(rawdatalinks_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rawdatalinks_file_data)

    return rows_updated, rows_no_match_found


def find_exported_csv(project_id, search_directory):
    exported_csv_path = None
    for file in os.listdir(search_directory):
        if (
            file.lower().startswith("exported_csv")
            and ("Sample").lower() in file.lower()
            and ("Information").lower() in file.lower()
            and project_id.lower() in file.lower()
            and file.lower().endswith(".csv")
        ):
            exported_csv_path = os.path.join(search_directory, file)
            break

    return exported_csv_path


def main():
    # Set working directory
    script_directory = os.getcwd()

    # Set exports directory
    exported_csv_directory = os.path.join(script_directory, "sample_information_exports")

    # Search for all rawdatalinks files and get rawdatalinks_file_paths in /Rawdatalinks
    rawdatalinks_files_directory = os.path.join(script_directory, "Rawdatalinks")
    for file in os.listdir(rawdatalinks_files_directory):
        if file.lower().endswith(".csv") and "Rawdatalinks" in file and "_" in file:
            # Get path
            rawdatalinks_file_path = os.path.join(rawdatalinks_files_directory, file)
            # Get basepath
            rawdatalinks_file_basename = os.path.basename(rawdatalinks_file_path)

            # Extract project ID from the rawdatalinks file name
            project_id = file.split("_")[0]

            # Find corresponding exported_csv_path
            exported_csv_path = find_exported_csv(project_id, exported_csv_directory)
            if exported_csv_path == None:
                exported_csv_path = find_exported_csv(project_id, script_directory)

            if exported_csv_path:
                # Get basepath
                exported_csv_basename = os.path.basename(exported_csv_path)

                # Update rows
                rows_updated, rows_no_match_found = add_customer_lables(rawdatalinks_file_path, exported_csv_path)

                # Check for success
                if rows_no_match_found != 0:
                    print(f"ERROR! Not all customer_lables added for '{rawdatalinks_file_basename}' found in '{exported_csv_basename}'")
                    print(f"Rows updated: {rows_updated}")
                    print(f"Rows with no match found: {rows_no_match_found}")
                else:
                    print(f"SUCCESS! customer_lables added to '{rawdatalinks_file_basename}' using '{exported_csv_basename}'")
            else:
                print(f"ERROR! No corresponding exported CSV found for '{rawdatalinks_file_basename}'")


if __name__ == "__main__":
    main()
