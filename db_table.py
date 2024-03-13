# sqlite db communication
import sqlite3

#
# Very basic SQLite wrapper
#
# Creates table from schema
# Provides small set of utility functions to query the database
#
# If you need to change the schema of an already created table, reset the database
# If you need to reset the database, just delete the database file (db_table.DB_NAME)
#
class db_table:

    # SQLite database filename
    DB_NAME = "interview_test.db"

    #
    # model initialization
    # records table name and schema
    # creates the table if it does not exist yet in DB
    #
    # \param name    string                name of the DB table
    # \param schema  dict<string, string>  schema of DB table, mapping column name to their DB type & constraint
    #
    # Example: table("users", { "id": "integer PRIMARY KEY", "name": "text" })
    #
    def __init__(self, name, schema):
        # error handling
        if not name:
            raise RuntimeError("invalid table name")
        if not schema:
            raise RuntimeError("invalid database schema")

        # init fields and initiate database connection
        self.name    = name
        self.schema  = schema
        self.db_conn = sqlite3.connect(self.DB_NAME)
        
        # ensure the table is created
        self.create_table()

    #
    # CREATE TABLE IF NOT EXISTS wrapper
    # Create the database table based on self.name and self.schema
    # If table already exists, nothing is done even if the schema has changed
    # If you need to apply schema changes, please delete the database file
    #
    def create_table(self):
        # { "id": "integer", "name": "text" } -> "id integer, name text"
        columns_query_string = ', '.join([ "%s %s" % (k,v) for k,v in self.schema.items() ])


        # CREATE TABLE IF NOT EXISTS users (id integer PRIMARY KEY, name text)
        #
        # Note that columns are formatted into the string without using sqlite safe substitution mechanism
        # The reason is that sqlite does not provide substitution mechanism for columns parameters
        # In the context of this project, this is fine (no risk of user malicious input)
        self.db_conn.execute("CREATE TABLE IF NOT EXISTS %s (%s)" % (self.name, columns_query_string))
        self.db_conn.commit()

    #
    # SELECT wrapper
    # Query the database by applying the specified filters
    #
    # \param columns  array<string>         columns to be fetched. if empty, will query all the columns
    # \param where    dict<string, string>  where filters to be applied. only combine them using AND and only check for strict equality
    #
    # \return [ { col1: val1, col2: val2, col3: val3 } ]
    #
    # Example table.select(["name"], { "id": "42" })
    #         table.select()
    #         table.select(where={ "name": "John" })
    #
    def select(self, columns = [], where = {}):
        # by default, query all columns
        if not columns:
            columns = [ k for k in self.schema ]

        # build query string
        columns_query_string = ", ".join(columns)
        query                = "SELECT %s FROM %s" % (columns_query_string, self.name)
        # build where query string
        if where:
            where_query_string = [ "%s = '%s'" % (k,v) for k,v in where.items() ]
            query             += " WHERE " + ' AND '.join(where_query_string)
        
        result = []
        # SELECT id, name FROM users [ WHERE id=42 AND name=John ]
        #
        # Note that columns are formatted into the string without using sqlite safe substitution mechanism
        # The reason is that sqlite does not provide substitution mechanism for columns parameters
        # In the context of this project, this is fine (no risk of user malicious input)
        for row in self.db_conn.execute(query):
            result_row = {}
            # convert from (val1, val2, val3) to { col1: val1, col2: val2, col3: val3 }
            for i in range(0, len(columns)):
                result_row[columns[i]] = row[i]
            result.append(result_row)

        return result

    #
    # INSERT INTO wrapper
    # insert the given item into database
    #
    # \param item  dict<string, string>   item to be insert in DB, mapping column to value
    #
    # \return id of the created record
    #
    # Example table.insert({ "id": "42", "name": "John" })
    #
    def insert(self, item):
        columns = ", ".join(item.keys())   
        #SQL place holder
        placeholders = ", ".join(["?" for _ in item.values()])  
        #dictionary items
        values = list(item.values())      
        #SQL statement with placeholders
        sql = f"INSERT INTO {self.name} ({columns}) VALUES ({placeholders})"     
        #execute the command
        cursor = self.db_conn.cursor()
        cursor.execute(sql, values)
        self.db_conn.commit()
        
        last_row_id = cursor.lastrowid  #last ID pointer
        cursor.close()  
        return last_row_id  #return ID pointer

    #
    # UPDATE wrapper
    # update multiple rows matching the specified condition
    #
    # \param values  dict<string, string>  values to be updates, mapping column to value
    # \param where   dict<string, string>  where filters to be applied. only combine them using AND and only check for strict equality
    #
    # \return number of updated records
    #
    # Example table.update({ "name": "Simon" }, { "id": 42 })
    #
    def update(self, values, where):
        #SET placeholders
        set_parts = [f"{column} = ?" for column in values]    
        #WHERE placeholders
        where_parts = [f"{column} = ?" for column in where]     
        #SQL statement with placeholders
        sql = f"UPDATE {self.name} SET {', '.join(set_parts)} WHERE {' AND '.join(where_parts)}"
        #param pairs
        parameters = list(values.values()) + list(where.values())       
        #Same process as insert()
        cursor = self.db_conn.cursor()
        cursor.execute(sql, parameters)
        self.db_conn.commit()
        
        updated_rows = cursor.rowcount  #changed rows
        cursor.close()
        
        return updated_rows
    def drop_table(self):
        """Drops the table from the database."""
        sql = f"DROP TABLE IF EXISTS {self.name}"
        cursor = self.db_conn.cursor()
        cursor.execute(sql)
        self.db_conn.commit()
        cursor.close()
    def truncate_table(self):
        """Deletes all rows in the table but keeps the table structure."""
        sql = f"DELETE FROM {self.name}"
        cursor = self.db_conn.cursor()
        cursor.execute(sql)
        self.db_conn.commit()
        cursor.close()


    #
    # Close the database connection
    #
    def close(self):
        self.db_conn.close()
