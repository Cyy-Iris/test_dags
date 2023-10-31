"""This modules encapsulate the logic to download / upload files to s3.

TODO (Guillaume): change everything here as it's only mock

it contains the following function:
    * :func:`download`: a function downloading a file to local filesystem using the 
        provided s3 path.
    * :func:`upload`: a function uploading a file to a local filesystem using the
        provided s3 path and content to upload.
"""

import logging
import os

logger = logging.getLogger("airflow.task")

# TODO(Guillaume): remove the hardcoded local file URL...
#CWD = "/Users/guillaumeraille/Projects/Claim/airflow-local"


def download(s3_path, local_dest=None):
    # download from s3 and returns a path in tmp ?
    logger.info(f"mock downloading file: `{s3_path}`...")
    return f"/data/{os.path.basename(s3_path)}"


def upload(s3_path, content=None):
    # upload to s3 and return path if succesful
    logger.info(f"mock uploading file: `{s3_path}`...")
    mock_path = f"/data/{os.path.basename(s3_path)}"
    """
    with open(mock_path, "w") as f:
        f.write(content)
    """
    return mock_path
