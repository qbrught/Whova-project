import sys
from db_table import db_table

def print_session(row):
    print("{} {} {} {} {} {} {}".format(
        row["date"], row["start_time"], row["end_time"], row["session_type"], row["title"], row["room"], row["speakers"]))
#values with spaces should be entered as hyphen seperated ie. Coral-Lounge
#there are two options for arguments: single column value pair, and multiple
# def parse_arguments(args):
#     """
#     Parse the command line arguments into a dictionary of column-value pairs.
#     Supports both single column-value pair and multiple pairs separated by commas.
#     """
#     where = {}
#     #try for error handling
#     try :
#         if len(args) == 3:
#             #single column-value pair
#             column = args[1]
#             value = args[2].replace("-", " ")  
#             where[column] = value
#         elif len(args) == 2:
#             #mulitple key value pairs, comma seperated ie. room=Lobby,speaker=Brad-Calder
#             for part in args[1].split(","):
#                 column, value = part.split("=")
#                 where[column] = value.replace("-", " ")
#         else:
#             raise ValueError("Invalid argument format.")
#     except ValueError as e:
#         print(f"Error parsing given arguments: {e}")
#         sys.exit(1)
#     return where
def parse_arguments(args):
    """
    Parse the command line arguments into a dictionary of column-value pairs.
    The first command line argument after the script name is considered the column,
    and the rest of the arguments are concatenated to form the value.
    """
    where = {}
    #try for error handling
    try:
        if len(args) < 3:
            raise ValueError("Insufficient arguments provided.")
        else:
            #first arg is column
            column = args[1]
            #rest of args are the value
            value = " ".join(args[2:])
            where[column] = value
    except ValueError as e:
        print(f"Error parsing given arguments: {e}")
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
