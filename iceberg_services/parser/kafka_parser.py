from urllib import request
import logging
import os

logger = logging.getLogger(__name__)

GDELT_ENDPOINT = "http://data.gdeltproject.org/gdeltv2/"
DOWNLOAD_DIR = "../data/input"


def get_files_from_kafka(key: str, value: dict) -> dict:
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    try:
        dataset_url = value["url"]
        filename = dataset_url.replace(GDELT_ENDPOINT, "").lstrip("/")
        file_path = os.path.join(DOWNLOAD_DIR, filename)

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        if os.path.exists(file_path):
            logger.info(f"Already Downloaded: {file_path}")
            value["file_path"] = file_path
            value["file_size"] = os.stat(file_path).st_size
            {key: value}

        logger.info(f"Downloading Files: {filename}")
        downloaded_path, headers = request.urlretrieve(
            url=dataset_url, filename=file_path
        )

        value["file_path"] = downloaded_path
        value["file_size"] = headers.get("x-goog-stored-content-length", "0")
        logger.info(f"Downloaded: {file_path}")

    except Exception as e:
        logger.error(f"An error occurred when downloading files: {e}")
        raise

    return {key: value}
