import sys
from db_table import db_table

def print_session(row):
    print("{} {} {} {} {} {} {}".format(
        row["date"], row["start_time"], row["end_time"], row["session_type"], row["title"], row["room"], row["speakers"]))
    
def parse_arguments(args):
    """
    Parse the command line arguments into a dictionary of column-value pairs.
    The first command line argument after the script name is considered the column,
    and the rest of the arguments are concatenated to form the value.
    Allows for multiple column-value pairs where values can include spaces.
    """
    valid_columns = ["date", "start_time", "end_time", "session_type", "title", "room", "description", "speakers"]
    where = {}
    column = None

    try:
        if len(args) < 3:
            raise ValueError("Incorrect arguments. requires at least one column with a value.")

        #check each argument after script name
        for arg in args[1:]:
            if arg in valid_columns:
                if column is not None and column not in where:
                    #raise error if the column value pair DNE
                    raise ValueError(f"Value not provided for column '{column}'.")
                column = arg
            elif column:
                #add value to the current column key
                if column in where:
                    where[column] += " " + arg
                else:
                    where[column] = arg
            else:
                #raise error if theres no matching column key
                raise ValueError(f"Unexpected argument '{arg}' before any column.")

        if column is not None and column not in where:
            #raise if no matching value for last column key
            raise ValueError(f"Value not provided for column '{column}'.")

    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    return where

def lookup_agenda(where):
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

    #replace location in case of "location"
    if "location" in where:
        where["room"] = where.pop("location")
    #replace speaker in case of "speaker"
    if "speaker" in where:
            where["speakers"] = where.pop("speaker")
    #session look up
    sessions = agenda_table.select(where=where)

    #print sessions and subsessions
    #(sub session format in the readme doesn't match the given agenda file so this is an assumption)
    for session in sessions:
        print_session(session)
        subsessions = agenda_table.select(where={"session_type": "Sub", "title": session["title"]})
        for subsession in subsessions:
            print_session(subsession)

    agenda_table.close()

if __name__ == "__main__":
    where = parse_arguments(sys.argv)
    if not where:
        print("Usage: ./lookup_agenda.py <column>=<value> or ./lookup_agenda.py <column1>=<value1>,<column2>=<value2>")
        sys.exit(1)

    lookup_agenda(where)
