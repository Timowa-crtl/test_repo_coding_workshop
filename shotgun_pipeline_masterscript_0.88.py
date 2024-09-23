import os
import subprocess
from os.path import join
import re
import sys

# 0.88 Added complete_analysis_files.py; complete_rawdatalinks.py; removed get_sample_information_form.py


def get_script_path(script_name):
    script_folder = os.path.dirname(os.path.abspath(__file__))

    script_path_map = {}

    # List all files in the working directory
    all_files = os.listdir(script_folder)

    # Define a regular expression to match filenames that end with "_x.x.py" (where x.x is the version number)
    script_pattern = re.compile(rf"{script_name}_(\d+\.\d+)\.py")

    # Search for matching script files and extract their version numbers
    for filename in all_files:
        match = script_pattern.match(filename)
        if match:
            version = float(match.group(1))
            script_path_map[version] = os.path.join(script_folder, filename)

    # Check if there are scripts without a declared version
    no_version_scripts = [filename for filename in all_files if filename.startswith(script_name) and filename.endswith(".py") and not script_pattern.match(filename)]

    # If there are scripts without a declared version, use the one with the highest version number,
    # otherwise, use the latest version with a declared version
    if no_version_scripts:
        script_path = os.path.join(script_folder, no_version_scripts[0])
        script_file_name = os.path.basename(script_path)
    elif script_path_map:
        # Find the script with the highest version number
        latest_version = max(script_path_map.keys())
        script_path = script_path_map[latest_version]
        script_file_name = os.path.basename(script_path)
    else:
        # Handle the case where no matching script is found
        print(f"No script with name '{script_name}' found in the working directory.")
        script_path = "/path/not/defined"
        sys.exit(1)

    return script_path, script_file_name


def log_script_execution(script_file_name, return_code):
    if return_code == 0:
        status = "finished"
    elif return_code == 99:
        status = "not used"
    else:
        status = f"error_{return_code}"

    script_execution_log[script_file_name] = status


def run_and_log_script(script_to_run):
    # Define the command to run the script
    script_path, script_file_name = get_script_path(script_to_run)
    script_command = ["python3", script_path]

    # Print what's running
    print("\n")
    print("____________________________________________________________")
    print(f"Now running: {script_file_name}")
    print("\n\n\n")

    # Run the script
    process = subprocess.Popen(script_command)

    # Wait for it to finish
    returned_code = process.wait()

    # log if script was executed successfully
    log_script_execution(script_file_name, returned_code)  # Check if the script exited with a return code of 0


if __name__ == "__main__":

    # Initialize a dictionary to log which scripts were run successfully
    script_execution_log = {}

    # Get the directory where the main script resides
    script_directory = os.path.dirname(os.path.abspath(__file__))
    # Set the working directory to the script's directory
    os.chdir(script_directory)

    # YYMMDD_Upload_Data_Info_1.1.xlsm --> project_info.csv (user has to generate project_info.csv to be able to do more than copy the fastq-files)

    # Check if project_info.csv is present
    project_info_file = os.path.join(script_directory, "project_info.csv")

    if os.path.exists(project_info_file):
        while True:
            # If the file is present, ask the user if they want to use it
            use_project_info = input("The project_info.csv file is present. Do you want to use it? (y/n): ").strip().lower()
            if use_project_info in ["y", "n"]:
                break
            else:
                print("Please enter 'y' or 'n'.")

    else:
        while True:
            # The file is not present, ask the user if they want to generate it
            exit_to_generate_project_info = input("The project_info.csv file is not present. Do you want to exit this script and generate it? (y/n): ").strip().lower()
            if exit_to_generate_project_info in ["y", "n"]:
                if exit_to_generate_project_info == "n":
                    use_project_info = "n"
                    break
                elif exit_to_generate_project_info == "y":
                    sys.exit(0)  # You can specify an exit status code, e.g., 1 for an error
            else:
                print("Please enter 'y' or 'n'.")

    # _______________________________________________________________________________________________________
    # copy_fastqs_when_copy_complete_appears.py
    script_to_run = "copy_fastqs_when_copy_complete_appears"

    # Ask the user if they want to copy fastqs at all
    while True:
        copy_fastqs = input("Do you want to copy fastq-files? (y/n) ")
        if copy_fastqs == "y" or copy_fastqs == "n":
            break

    # Ask the user if they want to copy two folders
    if copy_fastqs == "y":
        while True:
            another_folder = input("Do you want to copy fastq-files from 2 seperate folders? (y/n) ")
            if another_folder == "y" or another_folder == "n":
                break

        # Define the command to run the script
        script_path, script_file_name = get_script_path(script_to_run)
        script_command = ["python3", script_path]

        # Run one or two instances of the script based on user's choice
        if another_folder == "y":

            print("copy_fastqs_when_copy_complete_appears.py will be started twice to copy fastqs from 2 different folders.")

            # Run the script twice
            process1 = subprocess.Popen(script_command)
            process2 = subprocess.Popen(script_command)

            # Wait for both instances to finish
            returned_code_1 = process1.wait()
            returned_code_2 = process2.wait()

            # log if script was executed successfully
            log_script_execution(f"{script_file_name}_1", returned_code_1)  # Check if the script exited with a return code of 0
            log_script_execution(f"{script_file_name}_2", returned_code_2)  # Check if the script exited with a return code of 0

        else:
            run_and_log_script(script_to_run)

    else:
        print("No fastq files were copied. Continuing.\n")
        returned_code = 99
        log_script_execution(script_to_run, returned_code)

    # Stop here if project_info.csv is not found in working_directory
    if use_project_info != "y":
        print("Nothing else can be done without the file 'project_info.csv'. Terminating masterscript.")
        sys.exit(0)  # You can specify an exit status code, e.g., 1 for an error

    # _______________________________________________________________________________________________________
    # rename_in_fastq_if_rename_is_yes.py
    script_to_run = "rename_in_fastq_if_rename_is_yes"

    # run script and log if succesful
    run_and_log_script(script_to_run)

    # _______________________________________________________________________________________________________
    # concat_with_sanity_and_automatically.py
    script_to_run = "concat_with_sanity_and_automatically"

    # run script and log if succesful
    run_and_log_script(script_to_run)

    # _______________________________________________________________________________________________________
    # sort_by_project.py
    script_to_run = "sort_by_project"

    # run script and log if succesful
    run_and_log_script(script_to_run)

    # _______________________________________________________________________________________________________
    # generate_project_output.py
    script_to_run = "generate_project_output"

    # run script and log if succesful
    run_and_log_script(script_to_run)

    # _______________________________________________________________________________________________________
    # get_analysis_files.py
    script_to_run = "get_analysis_files"

    # run script and log if succesful
    run_and_log_script(script_to_run)

    # _______________________________________________________________________________________________________
    # get_rawdata_links.py
    script_to_run = "get_rawdata_links"

    # run script and log if succesful
    run_and_log_script(script_to_run)

    # _______________________________________________________________________________________________________
    # complete_rawdatalinks.py
    script_to_run = "complete_rawdatalinks"

    # run script and log if succesful
    run_and_log_script(script_to_run)

    # _______________________________________________________________________________________________________
    # complete_almost_all_metadata_files.py
    script_to_run = "complete_almost_all_metadata_files"

    # run script and log if succesful
    run_and_log_script(script_to_run)

    # _______________________________________________________________________________________________________
    # complete_analysis_files.py
    script_to_run = "complete_analysis_files"

    # run script and log if succesful
    run_and_log_script(script_to_run)

    # _______________________________________________________________________________________________________
    # check_metadata_files_and_rawdatalinks.py
    script_to_run = "check_metadata_files_and_rawdatalinks"

    # run script and log if succesful
    run_and_log_script(script_to_run)

    # _______________________________________________________________________________________________________
    # create_emails.py
    script_to_run = "create_emails"

    # run script and log if succesful
    run_and_log_script(script_to_run)

    # _______________________________________________________________________________________________________
    # copy_local_low_reads_for_concat.py
    script_to_run = "copy_local_low_reads_for_concat"

    # Set the working directory to the script's directory
    os.chdir(script_directory)

    # Check if low_reads_info.csv is present
    low_reads_info_file = os.path.join(script_directory, "low_reads_info.csv")

    if os.path.exists(low_reads_info_file):

        # run script and log if succesful
        run_and_log_script(script_to_run)

    # _______________________________________________________________________________________________________
    # prepare_zoe_projects_rename_concat_md5.py
    script_to_run = "prepare_zoe_projects_rename_concat_md5"

    # run script and log if succesful
    run_and_log_script(script_to_run)

    # _______________________________________________________________________________________________________
    # Print the script execution log
    print()
    print("__________________________________________________________________")
    print("Masterscript completed.\n")
    print("Script Execution Log:")
    for script, status in script_execution_log.items():
        print(f"{script}: {status}")
    print()
    print()
    print("You may now start the dataupload")
