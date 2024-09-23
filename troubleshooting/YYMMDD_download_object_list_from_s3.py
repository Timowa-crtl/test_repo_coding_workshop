import os
import boto3

def generate_object_keys(s3_url, filenames):
    # Parse the S3 URL to get the bucket name and prefix
    s3_url_parts = s3_url.replace("s3://", "").split("/")
    bucket_name = s3_url_parts[0]
    prefix = "/".join(s3_url_parts[1:])

    object_keys = []
    
    for filename in filenames:
        object_key = f"{prefix}{filename}"
        object_keys.append(object_key)

    return bucket_name, object_keys

def download_objects_from_s3(s3_url, object_keys, local_directory):
    # Initialize a Boto3 S3 client
    s3 = boto3.client("s3")

    # Create the local directory if it doesn't exist
    os.makedirs(local_directory, exist_ok=True)

    for object_key in object_keys:
        local_path = os.path.join(local_directory, os.path.basename(object_key))

        try:
            # Download the object from S3 to the local directory
            s3.download_file(bucket_name, object_key, local_path)
            print(f"Downloaded {object_key} to {local_path}")
        except Exception as e:
            print(f"Error downloading {object_key}: {str(e)}")

if __name__ == "__main__":
    s3_url = "s3://zymo-zoe/fastq/zrXXXXX/YYMMDD/"
    filenames = [
        "zr11407_174_R1.fastq.gz", "zr11407_174_R2.fastq.gz",
        "zr11407_176_R1.fastq.gz", "zr11407_176_R2.fastq.gz",
        "zr11407_179_R1.fastq.gz", "zr11407_179_R2.fastq.gz",
        "zr11407_182_R1.fastq.gz", "zr11407_182_R2.fastq.gz",
        "zr11407_193_R1.fastq.gz", "zr11407_193_R2.fastq.gz",
        "zr11407_201_R1.fastq.gz", "zr11407_201_R2.fastq.gz",
        "zr11407_21_R1.fastq.gz", "zr11407_21_R2.fastq.gz",
        "zr11407_235_R1.fastq.gz", "zr11407_235_R2.fastq.gz",
        "zr11407_248_R1.fastq.gz", "zr11407_248_R2.fastq.gz",
        "zr11407_268_R1.fastq.gz", "zr11407_268_R2.fastq.gz",
        "zr11407_28_R1.fastq.gz", "zr11407_28_R2.fastq.gz",
        "zr11407_7_R1.fastq.gz", "zr11407_7_R2.fastq.gz",
        "zr11407_80_R1.fastq.gz", "zr11407_80_R2.fastq.gz"
    ]  # List of filenames

    bucket_name, object_keys = generate_object_keys(s3_url, filenames)
    local_directory = "/media/share/novaseq01/Output/sequencing_data_for_upload/YYMMDD"

    download_objects_from_s3(s3_url, object_keys, local_directory)
