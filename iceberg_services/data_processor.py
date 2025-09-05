# %%
import pandas as pd
from parser.gdelt_parser import DownloadGDeltUpdatedData as geltDownload
from parser.kafka_parser import get_files_from_kafka
import logging
import os


logger = logging.getLogger(__name__)

YELLOW_COLOUR = "\033[93m"
GREEN_COLOUR = "\033[92m"
RED_COLOUR = "\033[91m"
RESET = "\033[0m"

HEADERS_PATH = "headers.csv"
SAVE_DIR = "/tmp/output"

# Table dict has the format
# dict {
#   name: {
#       url: str
#       date: str (YYYYMMDDHHmmSS)
#       file_path: str
#       file_size: int
#       df: pd.dataframe
#   }
# }
#
#


def get_smallest_dataset(ds_tables, headers=None):
    min_key, _ = min(ds_tables.items(), key=lambda t: t[1].get("file_size", 0))
    logger.info(f"Reading csv: {min_key}")
    df = pd.read_csv(
        ds_tables.get(min_key).get("file_path"),
        delimiter="\t",
        header=None,
        encoding="utf-8",
        engine="pyarrow",
        compression="zip",
    )
    return df


def read_headers_csv():
    try:
        return pd.read_csv(
            HEADERS_PATH,
            delimiter=";",
        )
    except Exception as e:
        print(f"\n{RED_COLOUR}✗ Failed to read Headers CSV\nERROR: {e}{RESET}\n")


def pre_process_df(df, headers=None):
    df = df.fillna("-1")
    if isinstance(headers, list):
        logger.debug(f"> Putting {len(headers)} headers to DF")
        df.columns = headers

    return df


def read_dataset_csv(datasets):
    headers_df = read_headers_csv()

    for key in datasets.keys():
        logger.info(f"Reading csv: {key}")
        try:
            df = pd.read_csv(
                datasets.get(key).get("file_path"),
                delimiter="\t",
                header=None,
                encoding="utf-8",
                engine="pyarrow",
                compression="zip",
            )

            header = (
                headers_df.loc[headers_df["name"] == key, "headers"]
                .values[0]
                .split(",")
            )

            datasets[key]["df"] = pre_process_df(df, header)

            logger.info(f"✓ Succeeded on reading csv: {key}")
            return datasets

        except Exception as e:
            print(f"✗Failed to read CSV: {key}\nERROR: {e}")
            logger.error(f"{RED_COLOUR}✗ Failed to read CSV: {key}\nERROR: {e}{RESET}")
            print(f"{RED_COLOUR}✗Failed to read CSV: {key}\nERROR: {e}{RESET}")

    return pd.DataFrame()


def save_processed(tables):
    print("Saving dfs to parquets")
    logger.info(f"{YELLOW_COLOUR}Saving dfs to parquets{RESET}")
    print(f"{YELLOW_COLOUR}Saving dfs to parquets{RESET}")

    if not tables:
        return

    saved_files = []

    for key, value in tables.items():
        try:
            filename = f"{key}_{value.get('date', 'no_date')}.parquet"
            file_path = os.path.join(SAVE_DIR, filename)

            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            df = value.get("df", None)
            if df is not None and not df.empty:
                df.to_parquet(path=file_path, engine="pyarrow")
                print(f"✓ Successfully saved {key} dataset to: {file_path}")
                logger.info(
                    f"{GREEN_COLOUR}✓ Successfully saved {key} dataset to: {file_path}{RESET}"
                )
                print(
                    f"{GREEN_COLOUR}✓ Successfully saved {key} dataset to: {file_path}{RESET}"
                )
                saved_files.append(file_path)
            else:
                print(f"✗ Failed to save {key} - DataFrame is empty or does not exist")
                logger.error(
                    f"{RED_COLOUR}✗ Failed to save {key} - DataFrame is empty or does not exist{RESET}"
                )
                print(
                    f"{RED_COLOUR}✗ Failed to save {key} - DataFrame is empty or does not exist{RESET}"
                )

        except TypeError as _:
            try:
                logger.debug("Trying to Save df as Str Type")

                df.astype(str).to_parquet(path=file_path, engine="pyarrow")
                print(
                    f"{GREEN_COLOUR}✓ Successfully saved as string {key} dataset to: {file_path}{RESET}"
                )
                saved_files.append(file_path)
            except Exception as e:
                print(f"✗ Failed to save {key} dataset\nERROR: {e}")
                logger.error(
                    f"{RED_COLOUR}✗ Failed to save {key} dataset\nERROR: {e}{RESET}"
                )
                print(f"{RED_COLOUR}✗ Failed to save {key} dataset\nERROR: {e}{RESET}")

        except Exception as e:
            print(f"✗ Failed to save {key} dataset\nERROR: {e}")
            logger.error(
                f"{RED_COLOUR}✗ Failed to save {key} dataset\nERROR: {e}{RESET}"
            )
            print(f"{RED_COLOUR}✗ Failed to save {key} dataset\nERROR: {e}{RESET}")

    return saved_files


# %%
if __name__ == "__main__":
    files = geltDownload()
    files = get_files_from_kafka(files)
    tables = files.tables

    read_dataset_csv(tables)
    save_processed(tables)
