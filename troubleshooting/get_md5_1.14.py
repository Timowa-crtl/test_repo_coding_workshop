#!/usr/bin/python
__author__= 'Aishani Prem, Patrick Tripp, Tim Weckerle'
_email__='aprem@zymoresearch.com'
import re
import os
import argparse
import hashlib
import time
import openpyxl


if __name__ == "__main__":

	# Command Line Arguments
	parser=argparse.ArgumentParser(
		description="This script uses python3. Make sure you have the correct version installed.")

	args = parser.parse_args()

	files = os.listdir(".")

	fout = open("md5.txt", "w")
	#md5sums = []
	samples = []
	for file in files:
		if file.endswith("gz"):
			sample = file.split("_R")[0]
			samples.append(sample)
	samples = list(set(samples))
	samples = sorted(list(set(samples)))  # Sort the samples list in alphabetical order
	print(samples)
	print("Give me some time. I will now do my calculations.")

	start_time_all = time.time()

	for sample in samples:
		line = []
		project = sample.split("_")[0]
		
		start_time = time.time()
		
	#	os.system("md5sum %s_R1.fastq.gz" %sample)
		md5sumR1 = hashlib.md5(open('%s_R1.fastq.gz' %sample,'rb').read()).hexdigest()
		md5sumR2 = hashlib.md5(open('%s_R2.fastq.gz' %sample,'rb').read()).hexdigest()
		
		elapsed_time = time.time() - start_time
		
		line.append(sample)
		line.append('s3://zymo-zoe/fastq/%s/%s_R1.fastq.gz' %(project,sample))
		line.append('s3://zymo-zoe/fastq/%s/%s_R2.fastq.gz' %(project,sample))
		line.append(md5sumR1)
		line.append(md5sumR2)
	#	print(line)
		line = ",".join(line)
		fout.write(line)
		fout.write("\n")
		print(f"Ok, {sample} is done. It took {elapsed_time:.2f} seconds. On to the next.")

	fout.close()  # Close the md5.txt file

	# Calculate the number of samples
	num_samples = len(samples)

	elapsed_time_all = time.time() - start_time_all
	elapsed_time_minutes = elapsed_time_all / 60

	print(f"Done. It took me {elapsed_time_minutes:.2f} minutes to calculate {num_samples} samples.")
	print("")

	# here md5.txt is defined as the source for the .xlsx
	filename = "md5.txt"

	# Read the content of the text file
	with open(filename, 'r') as file:
		lines = file.readlines()

	# Create a new workbook and select the active sheet
	workbook = openpyxl.Workbook()
	sheet = workbook.active

	# Write the data to the sheet
	for line_index, line in enumerate(lines):
		fields = line.strip().split(',')
		for field_index, field in enumerate(fields):
			sheet.cell(row=line_index+1, column=field_index+1).value = field

	# Save the workbook as an Excel file
	output_filename = filename.split('.')[0] + '.xlsx'
	workbook.save(output_filename)

	print("You should now have md5.txt and md5.xlsx.") 
	print("Copy the md5-data to the correct masterexcel.") 
	print("Then export the metadata-file named zr52XX_SampleInformationForm_YYMMDD.csv")
