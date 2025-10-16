from utils._mssql_connect import MSSQLDatabaseConnection
import os 

db = MSSQLDatabaseConnection()

def test_Database():
    print(db.get_current_connection())
    db.change_connection(database_server=os.getenv("MSSQL_HOST_TEST"))
    print(db.get_current_connection())
    db.change_connection(database_server=os.getenv("MSSQL_HOST_PROD"))
    print(db.get_current_connection())


# -------------------------------------------
# ---- Exploration / iteration examples  ----
# ------------------------------------------- 


#  Execute a SELECT and return a pandas DataFrame.   
def read_sql(query):
    return db.read_sql_query(query)


# Loop rows (Series) and print each column/value.
def example1(df):
    for i, row in df.iterrows():
        print(f"\nRow: {i}")
        for col in df.columns:
            print(f" {col}: {row[col]}")

# Fast row iteration using itertuples (attribute-style access).
def example2(df):
    for row in df.itertuples(index=False):
        print(row)

# Column-wise iteration: go column by column, printing values.
def example3(df):
    for col in df.columns:
        print(f"\nColumn: {col}")
        for val in df[col]:
            print(val)        

#  Quick EDA: head/tail, dtypes, stats, shape, and column names.
def basic_exploration(df):
    print("\n=== HEAD ===")
    print(df.head())
    print("\n=== TAIL ===")
    print(df.tail())
    print("\n=== INFO ===")
    print(df.info())
    print("\n=== DESCRIBE (numeric) ===")
    print(df.describe())
    print("\n=== SHAPE ===")
    print(df.shape)
    print("\n=== COLUMNS ===")
    print(df.columns.tolist())

# Subset specific columns and print row-wise values.
# Adjust column names to match your table.
def filtering_data(df):

    cols = ['PSPElementKey', 'BuchungskreisKey', 'KostenrechnungskreisKey', 'ProfitcenterKey']
    subset_cols = [c for c in cols if c in df.columns]
    subset = df[subset_cols]

    print("\n=== SUBSET ===")
    print(subset)

    for _, row in subset.iterrows():
        print()
        for col in subset.columns:
            print(f"  {col}: {row[col]}")


# Nested index-based access using iloc (row idx, col idx).
def index_based_selection(df):
    for i in range(len(df)):                 
        print(f"Row {i}:")
        for j in range(len(df.columns)):     
            print(f"  {df.columns[j]} = {df.iloc[i, j]}")



    

if __name__ == "__main__":

    #test_Database()
    sql_code = "Select top 10 * from [sapbd].[dimPSPElement]"
    df = read_sql(sql_code)


    example1(df)
    #example2(df)
    #example3(df)
    #basic_exploration(df)
    #filtering_data(df)
    #index_based_selection(df)


    db.disconnect()

    
