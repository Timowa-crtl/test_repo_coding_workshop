import os

def rename_files():
    folder_path = os.getcwd()

    try:
        # List all files in the current folder
        files = os.listdir(folder_path)

        # Iterate through each file
        for file_name in files:
            # Check if the file name contains "-"
            if "-" in file_name:
                # Replace "-" with "_"
                new_name = file_name.replace("-", "_")

                # Construct the new path
                old_path = os.path.join(folder_path, file_name)
                new_path = os.path.join(folder_path, new_name)

                # Rename the file
                os.rename(old_path, new_path)

        print("File renaming completed successfully!")

    except Exception as e:
        print(f"An error occurred: {e}")

# Run the function to rename files
rename_files()
