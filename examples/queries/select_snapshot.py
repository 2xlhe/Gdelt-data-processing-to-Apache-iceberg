from pyiceberg.catalog import load_catalog
import pandas as pd

CATALOG_NAME = "postgresql-mgc-1"
EXPORT_TABLE_NAME = "export"
GKG_TABLE_NAME = "gkg"
MENTIONS_TABLE_NAME = "mentions"

YELLOW_COLOUR = "\033[93m"
GREEN_COLOUR = "\033[32m"
RESET = "\033[0m"


pd.set_option("display.width", 0)


catalog = load_catalog(CATALOG_NAME)
gkg_table = catalog.load_table(".".join([GKG_TABLE_NAME, GKG_TABLE_NAME]))


current_snap = gkg_table.current_snapshot()
all_snap = list(map(lambda t: t.snapshot_id, gkg_table.snapshots()))

print(f"\nCurrent Snapshot: {current_snap}")
print(f"{GREEN_COLOUR}\nAll Snapshot:{RESET} {pd.DataFrame(all_snap)}")

gkg_columns = gkg_table.schema().column_names
print(f"gkg columns: {gkg_columns}")

print(f"{GREEN_COLOUR}\nCurrent Snapshot: {current_snap.snapshot_id}{RESET}")
gkg_columns = gkg_table.scan(
    selected_fields=("GKGRECORDID", "DATE", "DocumentIdentifier"),
    limit=10,
).to_pandas()
print("\nShape:", gkg_columns.shape)
print(gkg_columns.head(5))


print(f"{YELLOW_COLOUR}\nSnapshot:{all_snap[-10]}{RESET}")
gkg_columns = gkg_table.scan(
    selected_fields=("GKGRECORDID", "DATE", "DocumentIdentifier"),
    snapshot_id=all_snap[-10],
    limit=10,
).to_pandas()
print("\nShape:", gkg_columns.shape)
print(gkg_columns.head(5))
