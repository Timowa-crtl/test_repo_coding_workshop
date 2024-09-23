import csv
import os

# Version 1.2: Added explanation which file to select first and second.
# Version 1.32: Script will now find all ZOE metadatafiles in workingdirectory and will complete them if it finds a corresponding "exported_csv"-file.
# Version 1.51: Script will find and use new and old format of exported csvs in workingdirectory as well as in workingdirectory\sample_information_exports\.


def update_sample_ids_zoe(metadata_file, exported_csv):
    rows_csv1 = 0
    rows_updated = 0
    rows_no_match_found = 0
    # Read CSV1 and store data in a dictionary
    csv1_data = []
    with open(metadata_file, "r") as csv1_file:
        csv1_reader = csv.DictReader(csv1_file)
        for row in csv1_reader:
            csv1_data.append(row)
            rows_csv1 += 1

    # Read CSV2 and update SampleID in CSV1 based on ZymoID
    with open(exported_csv, "r") as csv2_file:
        csv2_reader = csv.DictReader(csv2_file)
        csv2_data = {row["ZymoID"]: row["SampleID"] for row in csv2_reader}

    # Update SampleID in CSV1 based on matching ZymoID values
    for row in csv1_data:
        zymo_id = row["ZymoID"]
        if zymo_id in csv2_data:
            row["SampleID"] = csv2_data[zymo_id]
            rows_updated += 1
        else:
            print(f"Error!: No 'SampleID' found for {zymo_id}.")
            rows_no_match_found += 1

    # Write updated data back to CSV1
    with open(metadata_file, "w", newline="") as csv1_file:
        fieldnames = csv1_data[0].keys()
        csv1_writer = csv.DictWriter(csv1_file, fieldnames=fieldnames)
        csv1_writer.writeheader()
        csv1_writer.writerows(csv1_data)

    return (rows_csv1, rows_updated, rows_no_match_found)


def update_sample_ids_other(metadata_file, exported_csv):
    rows_csv1 = 0
    rows_updated = 0
    rows_no_match_found = 0
    # Read CSV1 and store data in a dictionary
    csv1_data = []
    with open(metadata_file, "r") as csv1_file:
        csv1_reader = csv.DictReader(csv1_file)
        for row in csv1_reader:
            csv1_data.append(row)
            rows_csv1 += 1

    # Read CSV2 and update SampleID in CSV1 based on ZymoID
    with open(exported_csv, "r") as csv2_file:
        csv2_reader = csv.DictReader(csv2_file)
        csv2_data = {row["ZymoID"]: row["SampleID"] for row in csv2_reader}

    # Update SampleID in CSV1 based on matching ZymoID values
    for row in csv1_data:
        zymo_id = row["sample_id"]
        if zymo_id in csv2_data:
            row["customer_label"] = csv2_data[zymo_id]
            rows_updated += 1
        else:
            print(f"Error!: No 'customer_label' found for {zymo_id}.")
            rows_no_match_found += 1

    # Write updated data back to CSV1
    with open(metadata_file, "w", newline="") as csv1_file:
        fieldnames = csv1_data[0].keys()
        csv1_writer = csv.DictWriter(csv1_file, fieldnames=fieldnames)
        csv1_writer.writeheader()
        csv1_writer.writerows(csv1_data)

    return (rows_csv1, rows_updated, rows_no_match_found)


def get_metadatafolders_projectIDs(search_directory):
    projectID_metadatafolder_metadatafile_exportedcsv = {}
    for folder in os.listdir(search_directory):
        if folder.endswith("_metadata") and os.path.isdir(os.path.join(search_directory, folder)):
            project_ID = folder.split("_")[0]
            metadata_folder_path = os.path.join(search_directory, folder)
            projectID_metadatafolder_metadatafile_exportedcsv[project_ID] = {
                "metadata_folder_path": metadata_folder_path,
                "metadata_file": None,
                "exported_csv": None,
            }
    return projectID_metadatafolder_metadatafile_exportedcsv


def get_metadata_files(projectID_metadatafolder_metadatafile_exportedcsv):
    for (
        project_ID,
        metadata_info,
    ) in projectID_metadatafolder_metadatafile_exportedcsv.items():
        metadata_folder = metadata_info["metadata_folder_path"]
        for file in os.listdir(metadata_folder):
            if file.startswith(f"{project_ID}_SampleInformationForm_") and file.endswith(".csv"):
                metadata_info["metadata_file"] = os.path.join(metadata_folder, file)
            elif file.startswith(f"{project_ID}_Rawdatalinks_") and file.endswith(".csv"):
                metadata_info["metadata_file"] = os.path.join(metadata_folder, file)
    return projectID_metadatafolder_metadatafile_exportedcsv


def find_exported_csv(
    projectID_metadatafolder_metadatafile_exportedcsv,
    search_directory1,
    search_directory2,
):
    for (
        project_ID,
        metadata_info,
    ) in projectID_metadatafolder_metadatafile_exportedcsv.items():
        for search_directory in [search_directory1, search_directory2]:
            for file in os.listdir(search_directory):
                if (
                    file.lower().startswith("exported_csv")
                    and ("SampleInformation").lower() in file.lower()
                    and "zoe" in file.lower()
                    and project_ID.lower() in file.lower()
                    and file.lower().endswith(".csv")
                ):
                    metadata_info["exported_csv"] = os.path.join(search_directory, file)
                elif (
                    file.lower().startswith("exported_csv")
                    and ("SampleInformation").lower() in file.lower()
                    and "alba" in file.lower()
                    and project_ID.lower() in file.lower()
                    and file.lower().endswith(".csv")
                ):
                    metadata_info["exported_csv"] = os.path.join(search_directory, file)
                elif file.startswith("exported_csv_ZOE Sample Information Form") and project_ID in file and file.endswith(".csv"):
                    metadata_info["exported_csv"] = os.path.join(search_directory, file)
                elif file.startswith("exported_csv_Alba") and project_ID in file and file.endswith(".csv"):
                    metadata_info["exported_csv"] = os.path.join(search_directory, file)
    return projectID_metadatafolder_metadatafile_exportedcsv


if __name__ == "__main__":

    # set working_directory
    script_directory = os.getcwd()

    # Set exports directory
    exported_csv_directory = os.path.join(script_directory, "sample_information_exports")

    # create directory "projectID_metadatafolder_metadatafile_exportedcsv" to log all projectIDs for which metadatafile will be completed
    projectID_metadatafolder_metadatafile_exportedcsv = {}

    # get metadatafolders and projectIDs
    projectID_metadatafolder_metadatafile_exportedcsv = get_metadatafolders_projectIDs(script_directory)

    # get metadata_files
    projectID_metadatafolder_metadatafile_exportedcsv = get_metadata_files(projectID_metadatafolder_metadatafile_exportedcsv)

    # find_exported_csvs
    projectID_metadatafolder_metadatafile_exportedcsv = find_exported_csv(
        projectID_metadatafolder_metadatafile_exportedcsv,
        script_directory,
        exported_csv_directory,
    )

    # complete the metadatafiles for each projectID using the exported_csvs
    for (
        project_ID,
        metadata_info,
    ) in projectID_metadatafolder_metadatafile_exportedcsv.items():
        metadata_file = metadata_info["metadata_file"]
        exported_csv = metadata_info["exported_csv"]

        print()
        print(project_ID)

        if metadata_file == None:
            print(f"ERROR!: No metadata_file was found for {project_ID}")

        elif exported_csv == None:
            print(f"ERROR!: No exported_csv was found for {project_ID}")

        elif metadata_file == None or exported_csv == None:
            print(f"ERROR!: Can't process {project_ID}.\n")

        elif "alba" in exported_csv.lower():
            rows_csv1, rows_updated, rows_no_match_found = update_sample_ids_other(metadata_file, exported_csv)
            print(f"Project {project_ID} contains data for {rows_csv1} samples.")
            print(f"Sample IDs updated successfully for {rows_updated} samples.\n")
            if rows_no_match_found > 0:
                print(f"ERROR!: Data could not be updated for {rows_no_match_found} samples!\n")

        else:
            rows_csv1, rows_updated, rows_no_match_found = update_sample_ids_zoe(metadata_file, exported_csv)
            print(f"Project {project_ID} contains data for {rows_csv1} samples.")
            print(f"Sample IDs updated successfully for {rows_updated} samples.\n")
            if rows_no_match_found > 0:
                print(f"ERROR!: Data could not be updated for {rows_no_match_found} samples!\n")
