import sys
from db_table import db_table

def print_session(row):
    print("{} {} {} {} {} {} {}".format(
        row["date"], row["start_time"], row["end_time"], row["session_type"], row["title"], row["room"], row["speakers"]))

def lookup_agenda(column, value):
    #create the table
    agenda_table = db_table("agenda", {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "date": "TEXT",
        "start_time": "TEXT",
        "end_time": "TEXT",
        "session_type": "TEXT",
        "title": "TEXT",
        "room": "TEXT",
        "description": "TEXT",
        "speakers": "TEXT"
    })
    #look up based on column name
    if column in ["date", "start_time", "end_time", "title", "room", "description"] or column == "location":
        #location case
        if column == "location":
            column = "room"
        #replace hyphens with space
        value = value.replace("-", " ")
        where = {column: value}
        sessions = agenda_table.select(where=where)
    elif column == "speaker":
        #speaker case
        value = value.replace("-", " ")  #replace hyphens aswell
        sessions = agenda_table.select(where={"speakers": value})
    else:
        print("Invalid column specified")
        return

    #print sessions and subsessions
    for session in sessions:
        print_session(session)
        subsessions = agenda_table.select(where={"session_type": "Sub", "title": session["title"]})
        for subsession in subsessions:
            print_session(subsession)

    agenda_table.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: ./lookup_agenda.py <column> <value>")
        sys.exit(1)

    column = sys.argv[1]
    value = sys.argv[2].replace("-", " ")
    lookup_agenda(column, value)
