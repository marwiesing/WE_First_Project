
import os
from pathlib import Path
import datetime as dt
import pandas as pd
from utils._mssql_connect import MSSQLDatabaseConnection

FILES_DIR = Path("Files")  


def read_txt(path: Path):
    return pd.read_csv(path, sep=None, engine="python", dtype=str, keep_default_na=False, na_filter=False)


def read_excel_all_sheets(path: Path, file_index: int):
    print(f"[INFO] Reading Excel file: {path.name}")
    book = pd.read_excel(path, sheet_name=None, dtype=str, keep_default_na=False, na_filter=False)
    rows = []
    for sheet_name, df in (book or {}).items():
        file_index += 1
        print(f"[INFO] Reading sheet: {sheet_name}, in file: {path.name} and index: {file_index}")
        if df is None or df.empty:
            continue

        #rows.extend(df.values.tolist())
        for row in df.itertuples(index=False, name=None):
            values = list(row)
            rows.append(values + [file_index])


    print(f"[INFO] Read {len(rows)} rows from {path.name}")
    return rows, file_index


def collect_schema(files_dir: Path):
    items = []
    file_index = 0
    
    for name in sorted(os.listdir(files_dir)):
        p = files_dir / name

        if not p.is_file():
            continue

        if p.suffix.lower() == ".txt":
            try:
                df = read_txt(p)
                file_index += 1

                print(f"{p.name}: +{len(df)} rows (txt), and columns: {list(df.columns)}")
                for row in df.itertuples(index=False, name=None):
                    txname, id_value, txpassword = row
                    items.append([txname, id_value, txpassword, file_index])

                # value = df.values.tolist()
                # items.extend(value)
            except Exception as e:
                 print(f"[WARN] Could not read {p.name}: {e}")
        elif p.suffix.lower() in (".xlsx", ".xls"):
            try:
                df, file_index = read_excel_all_sheets(p, file_index)
                print(f"{p.name}: +{len(df)} rows (xlsx all sheets)")
                items.extend(df)
            except Exception as e:
                print(f"[WARN] Could not read {p.name}: {e}")

    return items


def save_to_database(data):
    db = MSSQLDatabaseConnection()
    if db.is_connected():
        query = "IF OBJECT_ID('DWH_Bronze_Clean_Password') IS NOT NULL select 1 else select 0"        
        if db.read_sql_query(query).values[0][0] == 1:
            query = "SELECT TOP 0 * FROM DWH_Bronze_Clean_Password"
            print(db.read_sql_query(query).columns.tolist())
            for row in data:
                txname, id_value, txpassword, file_index = row
                query = """
                INSERT INTO DWH_Bronze_Clean_Password (txname, idvalue, txpassword, idindex, dtcreatedate)
                VALUES (?, ?, ?, ?, ?)
                """
                params = (txname, id_value, txpassword, file_index, dt.datetime.now())
                db.execute_query(query, params)
            print(f"Inserted {len(data)} rows into DWH_Bronze_Clean_Password.\n")
        else:
            print("Table does not exist.\n")


if __name__ == "__main__":
    item = collect_schema(FILES_DIR)
    if item:
        print(f"\nTotal Array: {len(item)}")
        save_to_database(item)
