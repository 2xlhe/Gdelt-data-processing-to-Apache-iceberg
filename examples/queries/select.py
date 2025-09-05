from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import GreaterThanOrEqual
import pandas as pd

CATALOG_NAME = "postgresql-mgc-1"
EXPORT_TABLE_NAME = "export"

pd.set_option("display.width", 0)


catalog = load_catalog(CATALOG_NAME)
export_table = catalog.load_table(".".join([EXPORT_TABLE_NAME, EXPORT_TABLE_NAME]))


export_columns = export_table.schema().column_names
print(f"export columns: {export_columns}")

export_scan_df = export_table.scan(
    row_filter=GreaterThanOrEqual("NumMentions", 3),
    selected_fields=("EventCode", "NumMentions", "SourceURL"),
    limit=10,
).to_pandas()


print("\nShape:", export_scan_df.shape)
print(export_scan_df.head(5))
