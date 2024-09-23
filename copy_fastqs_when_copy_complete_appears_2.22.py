import csv
import os
import re
import shutil
import sys
import time
import tkinter as tk
from tkinter import filedialog

# 1.43  adding ability to choose which demux folders to copy
# 1.44  frontloading choosing which demux folders to copy for all demux folders instead of waiting for copying demux 1
# 1.45  now ignoring demux folders that appear while folder 1 is being copied
# 1.46  allowing user to either choose source path using file explorer or input the sequencing id manually. Entering the Sequencing ID manually allows starting the script before the directory exists.
#       Also now only asking to choose demux folders to copy if number of demux folders > 1
# 1.50  making additional copy of zoe projects in uploadfolder/project_id_gcloud for gcloud workflow
# 2.20  allowing user to switch between manual or file-dialog input of sequencing-id. fixed issues when running two instances of this script in parallel by using tkinter for both input methods.
# 2.21  only numeric folders within 'Analysis' are now recognized as Demux Folders
# 2.22  fixed minor typos in comments

# constants
OUTPUT_DIRECTORY = "/media/share/novaseq01/Output"


# Function to choose the sequencing run directory within /Output/ using tkinter file dialog
def choose_sequencing_run_directory(initial_directory):
    root = tk.Tk()
    root.withdraw()
    sequencing_run_directory = filedialog.askdirectory(title=f"Select the sequencing run directory within {OUTPUT_DIRECTORY}", initialdir=initial_directory)
    return sequencing_run_directory


def enter_sequencing_run_id(initial_directory):
    root = tk.Tk()
    root.title("Enter Sequencing Run ID")

    sequencing_run_id = tk.StringVar()

    def submit():
        entered_id = entry.get()
        pattern = r"^\w{8}_LH00213_\w{4}_[AB]\w{6,11}$"
        if re.match(pattern, entered_id):
            sequencing_run_id.set(entered_id)
            root.destroy()
        else:
            label.config(text="Invalid format. Please enter in the format specified.")

    def switch_file_dialog():
        sequencing_run_id.set("switch_to_file_dialog")
        root.destroy()
        return None

    label = tk.Label(root, text="Enter Sequencing-Run-ID manually (Format: YYYYMMDD_LH00213_XXXX_(A/B)FLOWCELLID):")
    label.pack()

    entry = tk.Entry(root, textvariable=sequencing_run_id)
    entry.pack()

    button = tk.Button(root, text="Submit", command=submit)
    button.pack()

    button_switch = tk.Button(root, text="Switch To File Dialog", command=switch_file_dialog)
    button_switch.pack()

    root.mainloop()

    entered_id = sequencing_run_id.get()
    if entered_id != "switch_to_file_dialog":
        sequencing_run_directory = os.path.join(initial_directory, entered_id)
    else:
        sequencing_run_directory = None
    return sequencing_run_directory


# Function to copy .gz files to the destination directory
def copy_gz_files(source_directory, destination_directory):
    number_copied_files = 0
    for root, dirs, files in os.walk(source_directory):
        for filename in files:
            if filename.endswith(".gz") and not filename.lower().startswith("undetermined"):
                source_file = os.path.join(root, filename)
                destination_file = os.path.join(destination_directory, filename)

                # Ensure the destination directory exists
                os.makedirs(os.path.dirname(destination_file), exist_ok=True)

                try:
                    with open(source_file, "rb") as source, open(destination_file, "wb") as destination:
                        while True:
                            chunk = source.read(1024)  # Copy in 1KB chunks
                            if not chunk:
                                break
                            destination.write(chunk)
                    number_copied_files += 1
                    print(f"'{filename}' copied successfully.")
                except Exception as e:
                    print(f"Error copying file '{filename}': {str(e)}")
    return number_copied_files


def check_directory(directory):
    """
    Check if the directory exists.
    """
    return os.path.exists(directory)


def get_zoe_project_ids(input_file):
    zoe_projects = {}
    with open(input_file, "r") as csvfile:
        reader = csv.DictReader(csvfile)

        # Ensure the expected columns exist
        expected_columns = ["project_ID", "#samples", "customer"]
        for column in expected_columns:
            if column not in reader.fieldnames:
                raise ValueError(f"Column '{column}' not found")

        # Process rows
        for row in reader:
            # Get the values of the required columns
            project_id = row.get("project_ID")
            customer = row.get("customer")
            samples = row.get("#samples")

            # Check if both values are not empty and customer is "zoe"
            if project_id and customer and customer.lower() == "zoe":
                zoe_projects[project_id] = samples

    return zoe_projects


# Function to copy zoe .gz files to the destination directory
def copy_zoe_gz_files(source_directory, zoe_projects):
    def get_project_id(filename):
        try:
            project_id = filename.split("_")[0]
            return project_id
        except:
            return None

    number_copied_files = 0
    for root, dirs, files in os.walk(source_directory):
        for filename in files:
            project_id = get_project_id(filename)
            if project_id:
                destination_directory = f"{project_id}_gcloud"
                if filename.endswith(".gz") and project_id in zoe_projects:
                    source_file = os.path.join(root, filename)
                    destination_file = os.path.join(destination_directory, filename)

                    # Ensure the destination directory exists
                    os.makedirs(os.path.dirname(destination_file), exist_ok=True)

                    try:
                        with open(source_file, "rb") as source, open(destination_file, "wb") as destination:
                            while True:
                                chunk = source.read(1024)  # Copy in 1KB chunks
                                if not chunk:
                                    break
                                destination.write(chunk)
                        number_copied_files += 1
                        print(f"'{filename}' copied successfully to {os.path.basename(destination_directory)}.")
                    except Exception as e:
                        print(f"Error copying file '{filename}': {str(e)}")
    return number_copied_files


def check_zoe_gcloud_folders(project_id, expected_files, working_directory):
    gcloud_folder_name = f"{project_id}_gcloud"
    gcloud_folder_path = os.path.join(working_directory, gcloud_folder_name)

    # Get the list of files in the folder
    files_in_folder = os.listdir(gcloud_folder_path)

    # Get the number of files in the folder
    num_files_in_folder = len(files_in_folder)

    if num_files_in_folder == expected_files:
        return True
    else:
        return False


if __name__ == "__main__":

    # extract zoe_project_ids from input_file
    input_file = "project_info.csv"
    zoe_projects = get_zoe_project_ids(input_file)

    # Get the source directory using user text input
    sequencing_run_directory = enter_sequencing_run_id(OUTPUT_DIRECTORY)

    if sequencing_run_directory == None:
        # Get the source directory using tkinter file dialog instead
        sequencing_run_directory = choose_sequencing_run_directory(OUTPUT_DIRECTORY)

    if not sequencing_run_directory:
        print("No Sequencing Run Directory given. Terminating script.")
        sys.exit()  # This will terminate the script with the default exit status of 0

    else:
        print(f"Sequencing Run Directory: {sequencing_run_directory}")
        # Check if the Analysis directory exists
        sequencing_run_analysis_directory = os.path.join(sequencing_run_directory, "Analysis")
        while not check_directory(sequencing_run_analysis_directory):
            print(f"Directory {sequencing_run_analysis_directory} does not yet exist. Waiting for 1 hour...")
            time.sleep(3600)

        print("Directory exists. Proceeding with the rest of the script.")

        # get working directory
        working_directory = os.path.dirname(os.path.abspath(__file__))

        # Build the destination directory path relative to the script's location
        destination_directory = os.path.join(working_directory, "fastq")
        number_of_processed_demux_folders = 0
        copied_demux_folders = []
        skipped_demux_folders = []
        total_copied_files = 0
        copied_zoe_gcloud_files = 0

        # create list of zoe gcloud foldernames and create them
        zoe_gcloud_foldernames = []
        for project_id in zoe_projects:
            gcloud_folder_name = f"{project_id}_gcloud"
            zoe_gcloud_foldernames.append(gcloud_folder_name)
            gcloud_folder_path = os.path.join(working_directory, gcloud_folder_name)
            os.makedirs(gcloud_folder_path, exist_ok=True)

        # check for demux folders
        demux_folders = [folder for folder in os.listdir(sequencing_run_analysis_directory) if os.path.isdir(os.path.join(sequencing_run_analysis_directory, folder)) and folder.isdigit()]
        number_of_demux_folders = len(demux_folders)
        print(f"We have {number_of_demux_folders} Demux_Folders: {demux_folders}")

        # copy all demux folders without asking if there is just 1 demux folder
        responses = {}
        if number_of_demux_folders <= 1:
            for demux_folder in demux_folders:
                responses[demux_folder] = "y"

        elif number_of_demux_folders > 1:
            # Ask user which demuxes he wants to copy

            for demux_folder in demux_folders:
                response = ""
                while response not in ["y", "n"]:
                    response = input(f"Do you want to copy files from Demux-Folder '{demux_folder}'? (y/n): ").lower()
                    responses[demux_folder] = response

        # loop until all demux folders a processed
        while True:
            for demux_folder in demux_folders:
                if responses.get(demux_folder) == "n":
                    print(f"Skipping copying files from Demux-Folder '{demux_folder}'.")
                    skipped_demux_folders.append(demux_folder)

                elif responses.get(demux_folder) == "y":
                    copy_complete_directory = os.path.join(sequencing_run_directory, "Analysis", demux_folder)
                    copy_complete_path = os.path.join(copy_complete_directory, "CopyComplete.txt")
                    source_directory = os.path.join(sequencing_run_directory, "Analysis", demux_folder, "Data", "BCLConvert", "fastq")
                    if os.path.exists(copy_complete_path) and demux_folder not in copied_demux_folders and demux_folder not in skipped_demux_folders:
                        # copy .gz files to the destination directory
                        number_copied_files = copy_gz_files(source_directory, destination_directory)
                        total_copied_files += number_copied_files
                        print(f"{number_copied_files} Files from Demux-Folder '{demux_folder}' copied successfully.")
                        print(f"A total of {total_copied_files} files has been copied.")
                        print()
                        # make additional copy for zoe gcloud projects
                        copied_zoe_gcloud_files += copy_zoe_gz_files(source_directory, zoe_projects)

                        copied_demux_folders.append(demux_folder)
                        number_of_copied_demux_folders = len(copied_demux_folders)

            number_of_processed_demux_folders = len(copied_demux_folders) + len(skipped_demux_folders)
            # Exit the loop when files have been copied from all folders
            if number_of_demux_folders == number_of_processed_demux_folders:
                break

            else:
                waiting_time = 1800
                waiting_time_min = waiting_time / 60
                print(f"Waiting... Skript will run again in {waiting_time_min} minutes.")
                time.sleep(waiting_time)  # Sleep for before checking again

    print()
    print(f"We succesfullly copied a total of {total_copied_files} files from {len(copied_demux_folders)} demux folders to /fastq.")
    if copied_zoe_gcloud_files > 0:
        print(f"A total of {copied_zoe_gcloud_files} zoe files have been copied additionally for gcloud workflow.")
    print(f"All demux folders: {demux_folders}.")
    print(f"Copied demux folders: {copied_demux_folders}.")
    print(f"Skipped demux folders: {skipped_demux_folders}.")
    print()

    # check numbers for zoe gcloud folders
    for project_id in zoe_projects:
        expected_files = int(zoe_projects[project_id]) * 2
        success = check_zoe_gcloud_folders(project_id, expected_files, working_directory)
        if success == False:
            print(f"{project_id}: ERROR! Number of files in {project_id}_gcloud folder is not correct!\n")
