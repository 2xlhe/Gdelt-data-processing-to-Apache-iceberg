from pyiceberg.catalog import load_catalog, Catalog, Table
import pyarrow as pa
import pyarrow.parquet as parquet
from typing import Tuple, Optional
import logging
import os


TABLE_NAME = "TEST1"
CATALOG_NAME = "postgresql-mgc-1"
logger = logging.getLogger(__name__)

YELLOW_COLOUR = "\033[93m"
GREEN_COLOUR = "\033[92m"
RESET = "\033[0m"


def read_parquet_apply_schema(path: str, schema: pa.Schema) -> Optional[pa.Table]:
    try:
        logger.debug(f"> Reading parquet: {path}")
        table = parquet.read_table(source=path, schema=schema)
        table = table.cast(schema)
        logger.info("✓ Successfully read parquet")

    except Exception as e:
        logger.error("\n✗ Error on reading")
        logger.error(f"✗ Error: {e}")
        return None

    return table


def create_schemas() -> Tuple[pa.Schema, pa.Schema, pa.Schema]:
    schemas = {}

    logger.info("Starting schema Processing")

    schemas["export"] = pa.schema(
        [
            ("GLOBALEVENTID", pa.string()),
            ("Day", pa.string()),
            ("EventCode", pa.string(), pa.string()),
            ("NumMentions", pa.int32()),
            ("NumSources", pa.int32()),
            ("SourceURL", pa.string()),
        ]
    )
    logger.info("✓ Successufuly read export_schema")

    logger.info("Creating gkg_schema")
    schemas["gkg"] = pa.schema(
        [
            ("GKGRECORDID", pa.string()),
            ("DATE", pa.string()),
            ("DocumentIdentifier", pa.string()),
            ("V2Tone", pa.string()),
            ("geoCoordinates", pa.string()),
        ]
    )
    logger.info("✓ Successufuly read gkg_schema")

    logger.info("Creating mentions_schema")
    schemas["mentions"] = pa.schema(
        [
            ("GLOBALEVENTID", pa.int64()),
            ("EventTimeDate", pa.string()),
            ("MentionIdentifier", pa.string()),
            ("Confidence", pa.int32()),
            ("MentionDocLen", pa.int32()),
        ]
    )
    logger.info("✓ Successufuly read mentions_schema")

    print(f"{GREEN_COLOUR}✓ Schemas Created!{RESET}")
    return schemas


def create_table_and_namespace(catalog: Catalog, name: str, schema: pa.Schema) -> Table:
    try:
        catalog.create_namespace_if_not_exists(name)
        return catalog.create_table_if_not_exists(name + "." + name, schema=schema)
    except Exception as e:
        print(f"An error ocurred while creating table: {name}")
        print(f"namespaces: {catalog.list_namespaces()}")
        logger.error(e)
        return None


def insert_into_table(table: Table, data: pa.Table):
    try:
        table.append(data)
        print(f"{GREEN_COLOUR}> Successfully inserted into Iceberg{RESET}")
    except Exception as e:
        print(f"\nError on append table {table}")
        logger.error(e)


def remove_file(path):
    try:
        os.remove(path)
        logger.info(f"Successfully removed file: {path}")
    except Exception as e:
        logger.error(f"Failed to remove file {path}: {e}")


def table_to_iceberg(name: str, catalog: Catalog, path: str, schema: pa.Schema):
    if os.path.exists(path):
        data = read_parquet_apply_schema(path, schema)
        table = create_table_and_namespace(catalog=catalog, name=name, schema=schema)
        insert_into_table(table=table, data=data)

        remove_file(path)
    else:
        print(f"{name} Path does not exist")


def gdelt_into_iceberg(paths):
    export_path, gkg_path, mentions_path = paths
    catalog = load_catalog(name=CATALOG_NAME)

    schemas = create_schemas()

    table_to_iceberg(
        name="export", catalog=catalog, path=export_path, schema=schemas["export"]
    )
    table_to_iceberg(name="gkg", catalog=catalog, path=gkg_path, schema=schemas["gkg"])
    table_to_iceberg(
        name="mentions",
        catalog=catalog,
        path=mentions_path,
        schema=schemas["mentions"],
    )


def insert_to_iceberg(path: str):
    table_types = ["export", "gkg", "mentions"]
    name = next((t for t in table_types if t in path), None)

    if name is None:
        logger.error("Path had no match")
        return

    try:
        catalog = load_catalog(name=CATALOG_NAME)
        schemas = create_schemas()
    except Exception as e:
        logger.error(f"\nError creating catalog {CATALOG_NAME}: {e}")

    try:
        print(f"> Appending {path} to catalog {CATALOG_NAME}")
        table_to_iceberg(name=name, catalog=catalog, path=path, schema=schemas[name])
    except Exception as e:
        logger.error(f"\nError inserting {name} from {path}: {e}")


if __name__ == "__main__":
    gdelt_into_iceberg(
        (
            "/tmp/output/export_20150218230000.parquet",
            "/tmp/output/gkg_20150218230000.parquet",
            "/tmp/output/mentions_20150218230000.parquet",
        )
    )
