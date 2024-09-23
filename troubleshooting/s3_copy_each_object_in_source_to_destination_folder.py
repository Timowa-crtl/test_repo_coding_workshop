import boto3

if __name__ == "__main__":
    # Initialize the S3 clients for the source and destination buckets
    source_bucket_name = 'source_bucket_name'
    destination_bucket_name = 'destination_bucket_name'
    source_folder = 'source_folder/'
    destination_folder = 'destination_folder/'

    s3 = boto3.client('s3')

    # List objects in the source folder
    objects = s3.list_objects_v2(Bucket=source_bucket_name, Prefix=source_folder)

    # Iterate through the objects and copy them one by one
    for obj in objects.get('Contents', []):
        source_key = obj['Key']
        destination_key = source_key.replace(source_folder, destination_folder)

        # Copy the object from source to destination
        s3.copy_object(
            CopySource={'Bucket': source_bucket_name, 'Key': source_key},
            Bucket=destination_bucket_name,
            Key=destination_key
        )

        print(f"Copied: {source_key} to {destination_key}.")

    print("Copy completed.")
