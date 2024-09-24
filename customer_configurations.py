# Customer configurations
customers_configs = {
    "Alba Health": {
        "upload_priority": 1,
        "local_path_metadata": "{local_path}_metadata",
        "upload_path_fastq1": "s3-external-zymo/rawdata/{run_date}_{project_id}/",
        "upload_path_fastq2": "empty",
        "upload_path_metadata1": "s3-external-zymo/metadata/{run_date}_{project_id}/",
        "upload_path_metadata2": "epiquest-zre/{project_id}/metadata/{run_date}/{random_string}/",
        "uploadpath_rawdatalinks": "s3-external-zymo/rawdata/{run_date}_{project_id}/",
        "bucket_region_fastq1": "eu-north-1",
        "get_expected_objects": lambda samples: samples * 2 + 2,
    },
    ("ExtraPZRE", "ExtraP"): {
        "upload_priority": 1,
        "local_path_metadata": "{local_path}_rawdatalinks",
        "upload_path_fastq1": "zymo-zre-sequencing/{sequencing_id}/",
        "upload_path_fastq2": "epiquest-zre/{project_id}/rawdata/{run_date}/{random_string}/",
        "upload_path_metadata1": "empty",
        "upload_path_metadata2": "empty",
        "uploadpath_rawdatalinks": "epiquest-zre/{project_id}/rawdata/{run_date}/{random_string}/",
        "bucket_region_fastq1": "eu-central-1",
        "get_expected_objects": lambda samples: samples * 2,
    },
}
