import os
import pyodbc

server = os.getenv("MSSQL_HOST_PROD", "localhost")
database = os.getenv("MSSQL_DBNAME")
encrypt = os.getenv("MSSQL_ENCRYPT", "True")
trust_cert = os.getenv("MSSQL_TRUSTSERVERCERT", "True")

connection_string = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"Trusted_Connection=yes;"
    f"Encrypt={encrypt};"
    f"TrustServerCertificate={trust_cert};"
)
print(connection_string)

conn = pyodbc.connect(connection_string)
cursor = conn.cursor()
cursor.execute("SELECT @@VERSION")
row = cursor.fetchone()
print(row)