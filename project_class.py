import random
import string
from dataclasses import dataclass
from typing import Dict, List, Callable


@dataclass
class Project:
    upload_priority: int
    project_ID: str
    samples: int
    expected_objects: int
    customer: str
    run_date: str
    sequencing_ID: str
    renaming: str
    metagenomics_BI: str
    metatranscriptomics_BI: str
    epigenetics_BI: str
    total_rna_BI: str
    random_string: str
    local_path: str
    local_path_metadata: str
    upload_path_fastq_1: str
    upload_path_fastq_2: str
    upload_path_metadata_1: str
    upload_path_metadata_2: str
    upload_command_fastq_1: str
    upload_command_fastq_2: str
    upload_command_metadata_1: str
    upload_command_metadata_2: str
    uploadpath_rawdatalinks: str
    bucket_region: str

    @staticmethod
    def generate_random_string(length):
        letters = string.ascii_uppercase
        return "".join(random.choice(letters) for _ in range(length))

    @classmethod
    def from_row_and_config(cls, row: Dict[str, str], config: Dict[str, any], sequencing_id: str, run_date: str) -> "Project":
        random_string = cls.generate_random_string(16)
        local_path = f"/media/share/novaseq01/Output/sequencing_data_for_upload/{sequencing_id}/{row['project_ID']}"
        local_path_metadata = config["local_path_metadata"].format(local_path=local_path)

        # Use the get_expected_objects lamda function from the config
        samples = int(row["#samples"])
        expected_objects = config["get_expected_objects"](samples)

        return cls(
            upload_priority=config["upload_priority"],
            project_ID=row["project_ID"],
            samples=samples,
            expected_objects=expected_objects,
            customer=row["customer"],
            run_date=run_date,
            sequencing_ID=sequencing_id,
            renaming=row["renaming?"],
            metagenomics_BI=row["metagenomics_BI?"],
            metatranscriptomics_BI=row["metatranscriptomics_BI?"],
            epigenetics_BI=row["epigenetics_BI?"],
            total_rna_BI=row["total_rna_BI?"],
            random_string=random_string,
            local_path=local_path,
            local_path_metadata=local_path_metadata,
            upload_path_fastq_1=config["upload_path_fastq1"].format(run_date=run_date, project_id=row["project_ID"], sequencing_id=sequencing_id, random_string=random_string),
            upload_path_fastq_2=config["upload_path_fastq2"].format(run_date=run_date, project_id=row["project_ID"], sequencing_id=sequencing_id, random_string=random_string),
            upload_path_metadata_1=config["upload_path_metadata1"].format(run_date=run_date, project_id=row["project_ID"], sequencing_id=sequencing_id, random_string=random_string),
            upload_path_metadata_2=config["upload_path_metadata2"].format(run_date=run_date, project_id=row["project_ID"], sequencing_id=sequencing_id, random_string=random_string),
            upload_command_fastq_1=f"time aws s3 cp {local_path} s3://{config['upload_path_fastq1'].format(run_date=run_date, project_id=row['project_ID'], sequencing_id=sequencing_id, random_string=random_string)} --recursive",
            upload_command_fastq_2=(
                f"time aws s3 cp {local_path} s3://{config['upload_path_fastq2'].format(run_date=run_date, project_id=row['project_ID'], sequencing_id=sequencing_id, random_string=random_string)} --recursive"
                if config["upload_path_fastq2"] != "empty"
                else "empty"
            ),
            upload_command_metadata_1=(
                f"time aws s3 cp {local_path_metadata} s3://{config['upload_path_metadata1'].format(run_date=run_date, project_id=row['project_ID'], sequencing_id=sequencing_id, random_string=random_string)} --recursive"
                if config["upload_path_metadata1"] != "empty"
                else "empty"
            ),
            upload_command_metadata_2=(
                f"time aws s3 cp {local_path_metadata} s3://{config['upload_path_metadata2'].format(run_date=run_date, project_id=row['project_ID'], sequencing_id=sequencing_id, random_string=random_string)} --recursive --acl public-read-write"
                if config["upload_path_metadata2"] != "empty"
                else "empty"
            ),
            uploadpath_rawdatalinks=config["uploadpath_rawdatalinks"].format(run_date=run_date, project_id=row["project_ID"], sequencing_id=sequencing_id, random_string=random_string),
            bucket_region=config["bucket_region_fastq1"],
        )
