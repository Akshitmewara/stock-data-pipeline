import pyodbc

conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=msdb;UID=sa;PWD=NewStrongPassword123!'
)
cursor = conn.cursor()
cursor.execute("SELECT 1")
print(cursor.fetchone())
conn.close()
