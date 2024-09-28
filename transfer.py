import mysql.connector
import pandas as pd 

df= pd.read_excel("census_2011.xlsx")
# Connect to MySQL
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='census_db'
)
cursor = conn.cursor()

# Create table query (adjust columns as needed)
cursor.execute('''
    CREATE TABLE IF NOT EXISTS census_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        State_UT VARCHAR(255),
        District VARCHAR(255),
        Population INT,
        Literate_Male INT,
        Literate_Female INT,
        Households_Rural INT,
        Households_Urban INT
    )
''')

# Insert data into MySQL
for _, row in df.iterrows():
    cursor.execute('''
        INSERT INTO census_data (State_UT, District, Population, Literate_Male, Literate_Female, Households_Rural, Households_Urban)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    ''', (row['State/UT'], row['District'], row['Population'], row['Literate_Male'], row['Literate_Female'], row['Households_Rural'], row['Households_Urban']))

conn.commit()
print("Data uploaded to MySQL successfully")
