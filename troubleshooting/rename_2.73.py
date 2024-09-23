import os
import re

# Version 2.73: Scripts now knows new naming scheme for ZRE-controls. Will also work for old naming scheme.
# New naming scheme: Extra1_PZRE_. Old naming scheme: Extra1_P_. Script should work with ZRC and ZRE controls.
# It will also correctly notice if a filename has already been renamed and recognize it as such.
# For mixed projects with old and new naming scheme, no  problems should arise. Analysis file will be combined of both.
# Will recognize every control called Extra1_P_ as ZRE control. ZRC usually has e.g. Extra1_PA_ or Extra1_PYYMMDD_
# Usage: This script renames ALL files within the folder it sits in without using project_info.csv

# Function to extract project ID from the filename of controls. Works for ZRE and ZRC controls.
def get_extra_project_id(filename):
    if filename.startswith("Extra"):  # Consider only controls (filename always starts with "Extra").
        filename_split = filename.split('_')[1][:]
        extra_project_id = f"Extra{filename_split}"  # Extract project ID from the filename of controls. Works for ZRE and ZRC schemas.
        return extra_project_id

if __name__ == "__main__":

    # Get the current working directory
    working_directory = os.getcwd()

    # List all files in the folder
    files = os.listdir(working_directory)

    # Iterate through the files and check if they start with any of the strings in projects_to_be_renamed
    for filename in files:
        filename_parts = filename.split("_")  # Split the filename by "_"
        old_filename = filename
        new_filename = re.sub(r'_S\d+_L00\d(.*)_001', r'\1', filename)
        new_filename_extra = re.sub(r'_S\d+_(.*)_001', r'\1', filename)

        # For controls (filename starts with "Extra").
        if filename.startswith("Extra"):
            # check for already renamed filenames
            if old_filename == new_filename_extra:
                pass
            # rename ZRE controls and keep the laneinfo "L00X"
            elif filename_parts[1] == "PZRE" or filename_parts[1] == "NZRE":
                os.system("rename -n 's/_S\\d+_(.*)_001/$1/g' {}".format(filename))
                os.system("rename 's/_S\\d+_(.*)_001/$1/g' {}".format(filename))
            elif filename_parts[1] == "P" or filename_parts[1] == "N":
                os.system("rename -n 's/_S\\d+_(.*)_001/$1/g' {}".format(filename))
                os.system("rename 's/_S\\d+_(.*)_001/$1/g' {}".format(filename))                
            # rename ZRC controls and delete laneinfo "L00X"
            else:
                os.system("rename -n 's/_S\d+_L00\d(.*)_001/$1/g' {}".format(filename))
                os.system("rename 's/_S\d+_L00\d(.*)_001/$1/g' {}".format(filename))

        # check for already renamed filenames
        if old_filename == new_filename:
            pass

        else:
            # Rename rawdata
            os.system("rename -n 's/_S\d+_L00\d(.*)_001/$1/g' {}".format(filename))
            os.system("rename 's/_S\d+_L00\d(.*)_001/$1/g' {}".format(filename))

    print()
    print()
    print('Data renaming complete!')
