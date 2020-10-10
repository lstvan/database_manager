from threading import Lock as lock_thread
from sqlite3 import connect


class database_manager():

    def initialize_thread(self):
        self.thread_lock = lock_thread()
    
    def connect_database(self, path=''):
        if (path != ''):
            self.database_connection = connect(path, check_same_thread=False, timeout=3600)
            self.database_cursor = self.database_connection.cursor()
    
    def execute(self, request=None, commit=False, fetch_all=False, fetch=0):
        if (request != ''):
            self.thread_lock.acquire(True)
            self.database_cursor.execute(*request) if (isinstance(request, tuple) == True) else self.database_cursor.execute(request)
            self.database_connection.commit() if commit == True else None
            if (fetch_all == True) and (fetch ==  0):
                results = list(self.database_cursor.fetchall())
            elif (fetch_all == False) and (fetch > 0):
                results = list(self.database_cursor.fetchall())[:fetch]
            else:
                results = None
            self.thread_lock.release()
            return results if results != None else None

    def insert(self, table='', values=[]):
        if (table != '') and (values != []):
            request = f'INSERT INTO {table} VALUES ({("?," * len(values))[:-1]})'
            self.execute(request=((request, values)), commit=True)

    def select(self, table='', columns=['*'], condition_column='', condition_operation='=', condition_value='', result_count=-1, sort_by='', sort_order='ASC'):
        if (table != '') and (columns != []):
            request = f'SELECT {", ".join(columns)} FROM {table} WHERE {condition_column} {condition_operation} "{condition_value}"' if (condition_column !='') and (condition_value != '') else f'SELECT {", ".join(columns)} FROM {table}'
            request += f' ORDER BY {sort_by} {sort_order}' if (sort_by != '') and (sort_order != '') else ''
            results = self.execute(request=request, fetch_all=True)
            results = results[0:result_count] if (result_count != -1) else results[0:len(results)]
            return results
    
    def delete(self, table='', condition_column='', condition_value=''):
        if (table != ''):
            request = f'DELETE FROM {table} WHERE {condition_column} = "{condition_value}"' if (condition_column != '') and (condition_value != '') else f'DELETE FROM {table}'
            self.execute(request=request, commit=True)

    def migrate(self, source_table='', destination_table='', source_columns=['*'], destination_columns=['*'], condition_column='', condition_value=''):
        if (source_table != '') and (destination_table != ''):
            if (condition_column != '') and (condition_value != ''):
                request = f'INSERT INTO {destination_table}({", ".join(destination_columns)}) SELECT {", ".join(source_columns)} FROM {source_table} WHERE {condition_column} = "{condition_value}"'
            else:
                request = f'INSERT INTO {destination_table}{ "(" + ", ".join(destination_columns) + ")" if len(destination_columns) > 1 else "" } SELECT {", ".join(source_columns)} FROM {source_table}'
            self.execute(request=request, commit=True)

    def __init__(self, path=''):
        if (path != ''):
            self.initialize_thread()
            self.connect_database(path=path)
