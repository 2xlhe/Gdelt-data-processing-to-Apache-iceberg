from dataclasses import dataclass, field
from urllib import request
import logging
import json
import re
import os


logger = logging.getLogger(__name__)

GDELT_ENDPOINT = "http://data.gdeltproject.org/gdeltv2/"
DOWNLOAD_DIR = "../data/input"

YELLOW_COLOUR = "\033[93m"
RESET = "\033[0m"


@dataclass
class DownloadGDeltUpdatedData:
    # Specifications: http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf

    url: str = field(default="http://data.gdeltproject.org/gdeltv2/masterfilelist.txt")
    tables: list = field(default_factory=list)

    def __post_init__(self):
        logger.info("Post initialization")
        str_url = self._parse_url()
        self._unpack_parsed_data(str_url)

    def _parse_url(self):
        try:
            print(f"{YELLOW_COLOUR}\nOpening URL, It may take a while...{RESET}")
            response = request.urlopen(self.url)
            data = response.read().decode("utf-8")
            return "\n".join(data.splitlines()[:3])  # Files may have thousands of lines
        except request.HTTPError as e:
            print(f"Error accessing URL: {e}")
            raise e
        except Exception as e:
            print(f"An error occurred: {e}")
            raise e

    def _unpack_parsed_data(self, data):
        gdelt_url_pattern = re.compile(GDELT_ENDPOINT + r".+")
        valid_urls = re.findall(gdelt_url_pattern, data)

        url_regex = re.compile(r"\.(csv|CSV)\.(ZIP|zip)$")

        # Returns: [Date (YYYYMMDDHHMMSS), Name, url]
        values = [
            [url] + re.sub(url_regex, "", url.replace(GDELT_ENDPOINT, "")).split(".")
            for url in valid_urls
            if url
        ]

        self.tables = {v[-1]: {"url": v[0], "date": v[1]} for v in values}

    def _download_files(self):
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)

        try:
            for dataset in self.tables.values():
                if not dataset or not dataset.get("url"):
                    continue

                dataset_url = dataset["url"]
                filename = dataset_url.replace(GDELT_ENDPOINT, "").lstrip("/")
                file_path = os.path.join(DOWNLOAD_DIR, filename)

                os.makedirs(os.path.dirname(file_path), exist_ok=True)

                if os.path.exists(file_path):
                    logger.info(f"Already Downloaded: {file_path}")
                    dataset["file_path"] = file_path
                    dataset["file_size"] = os.stat(file_path).st_size

                    continue

                logger.info(f"\nDownloading Files: {filename}")
                downloaded_path, headers = request.urlretrieve(
                    url=dataset_url, filename=file_path
                )

                dataset["file_path"] = downloaded_path
                dataset["file_size"] = headers.get("x-goog-stored-content-length", "0")
                logger.info(f"\nDownloaded: {file_path}")

        except Exception as e:
            print(f"An error occurred when downloading files: {e}")
            raise

    def print_tables(self):
        for table in self.tables.values():
            print(json.dumps(table, indent=3))
