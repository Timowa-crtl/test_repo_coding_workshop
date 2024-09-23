import random
import string


def get_upload_priority(customer):
    upload_priority_map = {
        "ExtraP": 1,
        "ExtraPZRE": 1,
        "Alba Health": 1,
        "ZRC_Microbiomics": 2,
        "ZRC": 4,
        "ZRE_Standard_with_BI": 5,
        "Ventra": 6,
        "Zotal": 8,
        "microbiomics_upload_complete": 9,  # uploading microbiomics_upload_complete.txt for ZRC
        "ZRC_Jeffrey": 11,
        "ZRC_Epigenetics": 11,
        "epigenetics_upload_complete": 12,  # uploading epigenetics_upload_complete.txt for ZRC
        "ZRE_Standard_no_BI": 13,
        "ExtraN": 14,
        "ExtraNZRE": 14,
        "ZOE": 15,
        "ZRE_default_no_BI": 6,
        "ZRE_default_metagenomics_BI": 6,
        "ZRE_default_metatranscriptomics_BI": 6,
        "ZRE_default_epigenetics_BI": 6,
        "ZRE_default_total_rna_BI": 6,
        "ZRC_default": 6,
        "ZRE_default_with_BI": 6,
        "default": 6,
    }

    if customer in upload_priority_map:
        upload_priority = upload_priority_map[customer]
    else:
        upload_priority = upload_priority_map["default"]
        print(f"Customer {customer} has not been assinged an upload priority, yet. Upload priority was set to default value = {upload_priority} (high priority)")

    return upload_priority


def generate_random_string(length):
    letters = string.ascii_uppercase
    return "".join(random.choice(letters) for _ in range(length))


def get_expected_objects(customer, samples):
    if customer == "epigenetics_upload_complete" or customer == "microbiomics_upload_complete":
        expected_objects = int(samples)
    elif customer == "Alba Health":
        expected_objects = int(samples) * 2 + 2
    else:
        expected_objects = int(samples) * 2
    return expected_objects


print(f"\nimported customer settings and helper functions from {__name__}.py\n")
