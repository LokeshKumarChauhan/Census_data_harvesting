import pandas as pd

# Load the dataset
df = pd.read_csv('census_data.csv')

# Column renaming dictionary
rename_dict = {
    'State name': 'State/UT',
    'District name': 'District',
    'Male_Literate': 'Literate_Male',
    'Female_Literate': 'Literate_Female',
    'Rural_Households': 'Households_Rural',
    'Urban_Households': 'Households_Urban',
    'Age_Group_0_29': 'Young_and_Adult',
    'Age_Group_30_49': 'Middle_Aged',
    'Age_Group_50': 'Senior_Citizen',
    'Age not stated': 'Age_Not_Stated'
}

# Rename columns
df.rename(columns=rename_dict, inplace=True)
print("Columns renamed successfully")
#===============================================================================
# Task 2

# Function to format State/UT names
def format_state_name(name):
    return ' '.join([word.capitalize() if word != 'and' else word for word in name.split()])

# Apply the function to State/UT names
df['State/UT'] = df['State/UT'].apply(format_state_name)
print("State/UT names formatted successfully")

#===============================================================================
# Task 3
# Load district files
telangana_districts = set(open('Data/Telangana.txt').read().splitlines())
ladakh_districts = {'Leh', 'Kargil'}

# Update State/UT based on districts
df.loc[df['District'].isin(telangana_districts), 'State/UT'] = 'Telangana'
df.loc[df['District'].isin(ladakh_districts), 'State/UT'] = 'Ladakh'
print("New State/UT formations handled successfully")

#===============================================================================
# Task 4
# Calculate missing data percentage before filling
missing_before = df.isna().mean() * 100

# Fill missing data
df['Population'] = df['Male'] + df['Female']
df['Literate'] = df['Literate_Male'] + df['Literate_Female']
df['Population'] = df['Young_and_Adult'] + df['Middle_Aged'] + df['Senior_Citizen'] + df['Age_Not_Stated']
df['Households'] = df['Households_Rural'] + df['Households_Urban']

# Calculate missing data percentage after filling
missing_after = df.isna().mean() * 100
print("Missing data processed successfully")


#===============================================================================
#task 5
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['census_data']
collection = db['census']

# Convert DataFrame to dictionary and insert into MongoDB
data_dict = df.to_dict('records')
collection.insert_many(data_dict)
print("Data saved to MongoDB successfully")


#===============================================================================
#task 6
import mysql.connector

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

#===============================================================================
#===============================================================================
