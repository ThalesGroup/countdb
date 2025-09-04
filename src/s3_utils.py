import logging
import os
from typing import List, Dict

import boto3
import botocore.exceptions
from botocore.exceptions import ClientError

from boto3 import Session

from utils import get_session

_DEFAULT_ROOT_FOLDER = "countdb"


def get_bucket() -> str:
    return os.environ["BUCKET"]


def get_root_folder() -> str:
    return os.environ.get("ROOT_FOLDER", _DEFAULT_ROOT_FOLDER)


def upload_content_to_s3(object_path: str, content: str, session: Session = None):
    s3_resource = get_session(session).resource("s3")
    try:
        logging.info(f"Writing file to S3. Key: {object_path}")
        obj = s3_resource.Object(get_bucket(), object_path)
        obj.put(Body=content, ACL="bucket-owner-full-control")
        return []
    except Exception as e:
        return [str(e)]


def upload_file_to_s3(
    file_name: str, object_path: str, session: Session = None
) -> List[str]:
    s3_client = get_session(session).client("s3")
    try:
        logging.info(f"Uploading file to S3. Key: {object_path}")
        s3_client.upload_file(file_name, get_bucket(), object_path)
        return []
    except Exception as e:
        return [str(e)]


def list_s3_folder_keys(folder: str, session: Session = None) -> List[str]:
    s3_client = get_session(session).client("s3")
    result = s3_client.list_objects(Bucket=get_bucket(), Prefix=folder)
    contents = result.get("Contents")
    if contents:
        return [key["Key"] for key in contents]
    else:
        return []


def s3_folder_tree(root: str, prefix: str, session: Session = None) -> dict:
    """
    Build a nested dictionary representing the S3 folder tree.
    Each folder with only objects is mapped to its last modified date (YYYY-MM-DD).
    Each folder with subfolders is mapped to a nested dict.
    """
    s3_client = get_session(session).client("s3")

    paginator = s3_client.get_paginator("list_objects_v2")
    folders = {}

    for page in paginator.paginate(Bucket=get_bucket(), Prefix=root + prefix):
        for obj in page.get("Contents", []):

            key = obj["Key"]

            rel_path = key[len(root) :].lstrip("/")
            parts = rel_path.split("/")

            if not parts or parts == [""]:
                continue

            lm = obj["LastModified"].date().isoformat()

            current = folders
            for i, part in enumerate(parts):
                if i == len(parts) - 2:  # parent folder of file
                    if part not in current or isinstance(current[part], str):
                        current[part] = lm
                    break
                else:
                    current = current.setdefault(part, {})
    return folders


def get_s3_object_content(object_path: str, session: Session = None):
    s3_resource = get_session(session).resource("s3")
    s3_object = s3_resource.Object(get_bucket(), object_path)
    return s3_object.get()["Body"].read().decode()


def list_s3_folders(folder: str, session: Session = None) -> List[str]:
    s3_client = get_session(session).client("s3")
    result = s3_client.list_objects(Bucket=get_bucket(), Prefix=folder, Delimiter="/")
    common_prefixes = result.get("CommonPrefixes")
    if common_prefixes:
        return [prefix.get("Prefix") for prefix in common_prefixes]
    else:
        return common_prefixes


def clear_s3_folder(s3_folder: str, session: Session = None) -> int:
    if not s3_folder.endswith("/"):
        s3_folder += "/"  # make sure to delete only the context of a folder
    s3 = get_session(session).resource("s3")
    bucket = s3.Bucket(get_bucket())
    res = bucket.objects.filter(Prefix=s3_folder).delete()
    if len(res) == 0:
        return 0
    elif "Errors" in res[0]:
        raise Exception(
            f"Errors found: {len(res[0]['Errors'])}, First error: {res[0]['Errors'][0]}"
        )
    elif "Deleted" not in res[0]:
        return 0
    else:
        return len(res[0]["Deleted"])


def delete_s3_object(key: str, session: Session = None):
    s3_client = get_session(session).client("s3")
    s3_client.delete_object(Bucket=get_bucket(), Key=key)


def add_s3_life_cycle_config(
    bucket: str,
    prefix: str,
    delete_days: int = None,
    transition_days: int = None,
    transition_storage_class: str = "STANDARD_IA",
    session: Session = None,
) -> bool:
    s3 = session.client("s3") if session else boto3.client("s3")
    if not prefix.endswith("/"):
        prefix += "/"
    rules_to_add = []
    for rule in _get_lifecycle_rules(s3, bucket):
        if (
            "Filter" in rule
            and "Prefix" in rule["Filter"]
            and rule["Filter"]["Prefix"] != prefix
        ):
            rules_to_add.append(rule)
        elif not _life_cycle_rule_changed(
            rule, delete_days, transition_days, transition_storage_class
        ):
            logging.info(f"No change in life cycle rule for prefix: {prefix}")
            return False

    enabled = not ("TEST" in os.environ and os.environ["TEST"] == "true")
    rule = {
        "ID": prefix,
        "Filter": {"Prefix": prefix},
        "Status": "Enabled" if enabled else "Disabled",
        "NoncurrentVersionExpiration": {"NoncurrentDays": 1},
        "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1},
    }
    if delete_days:
        rule["Expiration"] = {"Days": delete_days}
    if transition_days:
        rule["Transitions"] = [
            {"Days": transition_days, "StorageClass": transition_storage_class}
        ]
    rules_to_add.append(rule)
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket, LifecycleConfiguration={"Rules": rules_to_add}
    )
    logging.info(
        f"Added life cycle rule. Bucket: {bucket}, Prefix: {prefix}, "
        f"Expiration: {delete_days}, Transition: {transition_days}. Enabled: {enabled}"
    )
    return True


def _get_lifecycle_rules(s3, bucket: str) -> List[dict]:
    try:
        existing_rules = s3.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"]
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchLifecycleConfiguration":
            existing_rules = []
        else:
            raise
    return existing_rules


def _life_cycle_rule_changed(
    rule: Dict, delete_days: int, transition_days: int, transition_storage_class: str
):
    if (delete_days is not None) != ("Expiration" in rule):
        return True
    elif delete_days is not None and rule["Expiration"].get("Days", -1) != delete_days:
        return True
    if (transition_days is not None) != ("Transitions" in rule):
        return True
    elif transition_days is not None and (
        len(rule["Transitions"]) != 1
        or rule["Transitions"][0]["Days"] != transition_days
        or rule["Transitions"][0]["StorageClass"] != transition_storage_class
    ):
        return True
    return False


def download_s3_file(key: str, local_path: str, session: Session = None):
    s3 = get_session(session).client("s3")
    try:
        s3.download_file(get_bucket(), key, local_path)
        logging.info(f"Downloaded {key} from bucket {get_bucket()} to {local_path}")
    except ClientError as e:
        logging.error(f"❌ Failed to download {key} from bucket {get_bucket()}: {e}")
        raise
    except Exception as e:
        logging.error(f"❌ Error downloading file: {e}")
        raise


def obj_exists(key: str, session: Session = None) -> bool:
    s3_client = get_session(session).client("s3")
    try:
        s3_client.head_object(Bucket=get_bucket(), Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            raise e
