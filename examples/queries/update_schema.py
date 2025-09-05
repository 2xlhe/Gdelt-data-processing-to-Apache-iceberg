from pyiceberg.catalog import load_catalog
from pyiceberg.types import StringType
import pandas as pd

CATALOG_NAME = "postgresql-mgc-1"
MENTIONS_TABLE_NAME = "mentions"

YELLOW_COLOUR = "\033[93m"
GREEN_COLOUR = "\033[32m"
RESET = "\033[0m"

NEW_COLUMN_NAME = "EXAMPLE"

pd.set_option("display.width", 0)


catalog = load_catalog(CATALOG_NAME)
mentions_table = catalog.load_table(
    ".".join([MENTIONS_TABLE_NAME, MENTIONS_TABLE_NAME])
)


mentions_columns = mentions_table.schema().column_names
print(f"export columns: {mentions_columns}")

print(f"{GREEN_COLOUR}\n Before Update{RESET}")
mentions_column = mentions_table.scan(
    selected_fields=("GLOBALEVENTID", "MentionIdentifier", "Confidence"),
    limit=10,
).to_pandas()
print("\nShape:", mentions_column.shape)
print(mentions_column.head(5))

try:
    with mentions_table.update_schema() as update_schema:
        update_schema.add_column(path=NEW_COLUMN_NAME, field_type=StringType())

except ValueError as _:
    print(f"Column ALready Exists: {NEW_COLUMN_NAME}")
except Exception as e:
    print(e)

print(f"{YELLOW_COLOUR}\n After Update{RESET}")
mentions_column = mentions_table.scan(
    selected_fields=(
        NEW_COLUMN_NAME,
        "GLOBALEVENTID",
        "MentionIdentifier",
        "Confidence",
    ),
    limit=10,
).to_pandas()
print("\nShape:", mentions_column.shape)
print(mentions_column.head(5))
