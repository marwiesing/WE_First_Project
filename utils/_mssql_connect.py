import pyodbc
import pandas as pd
import sqlparse as sp
import os


class MSSQLDatabaseConnection:
	def __init__(self):
		self.database_server = os.getenv("MSSQL_HOST_PROD", "localhost")
		self.database_name = os.getenv("MSSQL_DBNAME")
		self.trusted_connection = os.getenv("TRUSTED_CONNECTION", "yes")
		self.encrypt = os.getenv("MSSQL_ENCRYPT", "True")  
		self.trust_server_cert = os.getenv("MSSQL_TRUSTSERVERCERT", "True")  
		self.odbc_driver = self._pick_driver()  
		self.connection = self.connect_to_database()
		if self.connection is None:
			raise ConnectionError("❌ Could not connect to MSSQL database.")
	
	def _pick_driver(self):
		preferred = [
					"ODBC Driver 18 for SQL Server",
					"ODBC Driver 17 for SQL Server",
					"SQL Server"]
		available = set(pyodbc.drivers())
		for d in preferred:
			if d in available:
				return d

		raise RuntimeError(
			f"Kein passender ODBC-Treiber gefunden. Verfügbare Treiber: {list(available)}.\n"
			"Bitte installiere einen ODBC Driver for SQL Server von Microsoft: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver17"
		)		
	
	def connection_string(self):
		return (
			f"DRIVER={self.odbc_driver};"
			f"SERVER={self.database_server};"
			f"DATABASE={self.database_name};"
			f"Trusted_Connection={self.trusted_connection};"
			f"Encrypt={self.encrypt};"
			f"TrustServerCertificate={self.trust_server_cert};"
		)	

	def connect_to_database(self):
		try:
			print(f"Connecting to MSSQL database:\n"
				  f"Host: {self.database_server}, "
				  f"Database: {self.database_name}, "
				  f"Driver: {self.odbc_driver}, " 
				  f"Trusted Connection: {self.trusted_connection}, " 
				  f"Encrypt: {self.encrypt}, " 
				  f"Trust Server Certificate: {self.trust_server_cert}\n"
				  )
			connection = pyodbc.connect(self.connection_string(), timeout=5, autocommit=False)
			print("✅ Verbindung erfolgreich\n")
			return connection
		except Exception as e:
			print("❌ Fehler bei Verbindung:\n", e)
			return None
		
	def is_connected(self):
		return self.connection is not None		

	def disconnect(self):
		if self.is_connected():
			try:
				self.connection.close()
				print("Database connection closed.\n")
			except Exception as e:
				print("Error while closing the connection:", e)
			finally:
				self.connection = None

	def change_connection(self, database_name=None, database_server=None):
		updated_info=[]
		if database_name:
			self.database_name = database_name
			updated_info.append(f"Database: {self.database_name}")
		if database_server:
			self.database_server = database_server
			updated_info.append(f"Server: {self.database_server}")
		
		if not updated_info:
			print("No Changes provided.")
			return self.connection
		else:
			print("New target ==> " + ", ".join(updated_info) + "\n")
			self.disconnect()
		
		self.connection = self.connect_to_database()
		if self.connection is None:
			raise ConnectionError("❌ Failed to change database to MSSQL database.")
		return self.connection

	def ensure_connection(self):
		if not self.is_connected():
			self.connect_to_database()

	def read_sql_query(self, query, params=None):
		self.ensure_connection()
		try:
			with self.connection.cursor() as cursor:
				cursor.execute(query, params or ())
				rows = cursor.fetchall()
				columns = [col[0] for col in cursor.description]
				data = [tuple(row) for row in rows]
				return pd.DataFrame(data, columns=columns)
		except Exception as e:
			print("❌ Error while executing SELECT query:", e)
			return pd.DataFrame()

	def execute_query(self, query, params=None):
		self.ensure_connection()
		try:
			with self.connection.cursor() as cursor:
				cursor.execute(query, params or ())
				self.connection.commit()
		except Exception as e:
			print("❌ Error while executing query:", e)
			self.connection.rollback()

	def execute_sql_file(self, filepath):	
		self.ensure_connection()	
		base_dir = os.path.dirname(os.path.abspath(__file__))
		sql_path = os.path.join(base_dir, filepath)
		try:
			with open(sql_path, "r") as file:
				sql_content = file.read()
			sql_statements = sp.split(sql_content)
			with self.connection.cursor() as cursor:
				for statement in sql_statements:
					clean = statement.strip()
					if clean:
						print(f"\n🟡 Executing:\n{clean}")
						cursor.execute(clean)
						try:
							result = cursor.fetchall()
							for row in result:
								print("📄", row)
						except pyodbc.ProgrammingError:
							pass 
				self.connection.commit()
			print("✅ SQL file executed successfully.")
		except Exception as e:
			print("❌ Error while executing SQL file:", e)
			self.connection.rollback()
			
	def get_current_connection(self):
		return self.read_sql_query("SELECT @@SERVERNAME as ServerName, DB_NAME() AS CurrentDatabase").to_string(index=False)

	def __del__(self):
		try:
			print("Closing database connection...")
			self.disconnect()
		except Exception:
			pass