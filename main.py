import pandas as pd
import psycopg2
from psycopg2 import sql

# URL of the public Google Sheet (in CSV format)
google_sheet_url = 'https://docs.google.com/spreadsheets/d/{YOUR_SHEET_ID}/export?format=csv'

# Load the data into a pandas DataFrame
df = pd.read_csv(google_sheet_url)

# PostgreSQL Setup
conn = psycopg2.connect(
    dbname='your_dbname',
    user='your_username',
    password='your_password',
    host='your_host',
    port='your_port'
)

# Create a cursor object
cur = conn.cursor()

# Define the table name
table_name = 'your_table_name'

# Create table if not exists
columns = list(df.columns)
col_str = ', '.join([f'"{col}" TEXT' for col in columns])

create_table_query = f'''
CREATE TABLE IF NOT EXISTS {table_name} (
    {col_str}
);
'''

cur.execute(create_table_query)
conn.commit()

# Insert Data into PostgreSQL
for index, row in df.iterrows():
    insert_query = sql.SQL('''
        INSERT INTO {} ({}) VALUES ({})
    ''').format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(map(sql.Identifier, columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(row))
    )
    cur.execute(insert_query, list(row))

# Commit changes and close the connection
conn.commit()
cur.close()
conn.close()
