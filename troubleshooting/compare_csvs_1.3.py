import csv
import tkinter as tk
from tkinter import filedialog
import pandas as pd
import sys  # Import the sys module



def check_csvs_identical(file1, file2):
    try:
        # Load CSV files into pandas DataFrames
        df1 = pd.read_csv(file1)
        df2 = pd.read_csv(file2)
        
        # Check if header row spellings are identical
        if df1.columns.tolist() == df2.columns.tolist():
            print("Nice. Header row spellings are identical :)")
        if df1.columns.tolist() != df2.columns.tolist():
            print("Warning!: Header row spellings are not identical :(")
            #return
        
        # Check if DataFrames are identical
        if df1.equals(df2):
            print("Nice. CSV files are identical.")
            print()
        else:
            print("Warning!: CSV files are not identical.")
            print()
            
            # Find and print differences in fields
            diff_rows = []
            for index, row in df1.iterrows():
                if index >= len(df2):
                    diff_rows.append(index)
                elif not row.equals(df2.iloc[index]):
                    diff_rows.append(index)
            
            if diff_rows:
                print("Differences found in the following rows:")
                for row_index in diff_rows:
                    print(f"Row {row_index + 1}:")
                    for column in df1.columns:
                        if column not in df2.columns:
                            print(f"- Column {column} is missing in File 2")
                        elif df1.at[row_index, column] != df2.at[row_index, column]:
                            print(f"- Column: {column}, Value in File 1: {df1.at[row_index, column]}, Value in File 2: {df2.at[row_index, column]}")
            else:
                print("No differing rows found.")
    except Exception as e:
        print("An error occurred:", e)

    
if __name__ == "__main__":
    root = tk.Tk()
    root.withdraw()

    # Ask the user to choose the first CSV file
    csv_file1 = filedialog.askopenfilename(title="Select CSV1 File", filetypes=[("CSV Files", "*.csv")])

    # Ask the user to choose the second CSV file
    csv_file2 = filedialog.askopenfilename(title="Select CSV2 File", filetypes=[("CSV Files", "*.csv")])

    # Check if filepaths are identical.
    if csv_file1 == csv_file2:
        print(f"First filepath: {csv_file1}.")
        print(f"Second filepath: {csv_file2}.")
        print(f"ERROR!! You are trying to compare a file with itself. That does not make sense. Shutting down operation.")
        sys.exit()  # Exit the script gracefully

    # Extract only the filenames without the paths
    csv_filename1 = csv_file1.split("/")[-1]
    csv_filename2 = csv_file2.split("/")[-1]


    print(f"File 1 is {csv_filename1}.")
    print(f"File 2 is {csv_filename2}.")
    print()

    check_csvs_identical(csv_file1, csv_file2)
