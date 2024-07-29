import sqlite3
import pandas as pd

# Connect to the SQLite3 service
conn = sqlite3.connect('STAFF.db')

# Define table parameters
table_name = 'DEPARTMENTS'
attribute_list = ['DEPT_ID', 'DEPT_NAME', 'MANAGER_ID', 'LOC_ID']

# Read the CSV data
file_path = '/home/project/databases/Departments.csv'
df = pd.read_csv(file_path, names = attribute_list)

# Load the CSV to the database
df.to_sql(table_name, conn, if_exists = 'replace', index =False)
print('Table is ready')

# Define data to be appended
data_dict = {'DEPT_ID' : [9],
            'DEPT_NAME' : ['Quality Assurance'],
            'MANAGER_ID' : [30010],
            'LOC_ID' : ['L0010']}
data_append = pd.DataFrame(data_dict)

# Append data to the table
data_append.to_sql(table_name, conn, if_exists = 'append', index =False)
print('Data appended successfully')

# Query 4: Display the count of the total number of rows.
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Close the connection
conn.close()