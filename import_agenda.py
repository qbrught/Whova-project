import pandas as pd
from db_table import db_table

# load the Excel file with manual headers
df = pd.read_excel('agenda.xls', header=None, skiprows=13, names=['Date', 'Start Time', 'End Time', 'Session Type', 'Title', 'Room', 'Description', 'Speakers'])
# table schema
schema = {
    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "date": "TEXT",
    "time_start": "TEXT",
    "time_end": "TEXT",
    "session_type": "TEXT",
    "title": "TEXT",
    "room": "TEXT",
    "description": "TEXT",
    "speakers": "TEXT"
}
#create table
agenda_table = db_table("agenda", schema)
#get rows of dataframe
for index, row in df.iterrows():
    agenda_table.insert({
        "date": row['Date'],
        "time_start": row['Start Time'],
        "time_end": row['End Time'],
        "session_type": row['Session Type'],
        "title": row['Title'],
        "room": row['Room'],
        "description": row['Description'],
        "speakers": row['Speakers']
    })
agenda_table.close()
