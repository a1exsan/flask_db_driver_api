import datetime
import sqlite3
import pandas as pd
import json

class history_agent():
    def __init__(self, request, auth):
        self.db_name = 'request_history_1.db'
        self.db_data_name = 'oligomap_history_2.db'
        self.request = request
        self.auth = auth
        self.write_data()

    def write_data(self):
        date = datetime.datetime.now().date().strftime("%d.%m.%Y")
        time = datetime.datetime.now().time().strftime("%H:%M:%S")
        user_name = self.auth.current_user()

        db = uny_litebase(self.db_name)
        db.insert_data('main_tab', [user_name, date, time, self.request.url, self.request.remote_addr])

    def write_oligomap_data(self):
        date = datetime.datetime.now().date().strftime("%d.%m.%Y")
        time = datetime.datetime.now().time().strftime("%H:%M:%S")
        user_name = self.auth.current_user()

        db = uny_litebase(self.db_data_name)
        db.insert_data('main_tab', [user_name, date, time, self.request.url, self.request.json])



class uny_litebase():
    def __init__(self, base_name):
        self.base_name = base_name
        self.tables = {}

    def dict_to_text(self, dd):
        return json.dumps(dd)

    def text_to_dict(self, text):
        #print(text)
        return json.loads(text)

    def df_to_text(self, dataframe):
        return json.dumps(dataframe.to_dict('records'))

    def text_to_df(self, text):
        return pd.DataFrame(json.loads(text))

    def add_item(self, tab_name, item_name, item_type):
        if tab_name not in list(self.tables.keys()):
            self.tables[tab_name] = {'items': [], 'types': []}

        self.tables[tab_name]['items'].append(item_name)
        self.tables[tab_name]['types'].append(item_type)

    def create_tables(self):
        db = sqlite3.connect(self.base_name)
        cursor = db.cursor()

        for tab_name in list(self.tables.keys()):
            #print(tab_name)
            s = 'id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, '
            for item_, type_ in zip(self.tables[tab_name]['items'], self.tables[tab_name]['types']):
                s += f'{item_} {type_}, '
            #print(s)
            #print(s[:-2])
            cursor.execute(f"""DROP TABLE IF EXISTS {tab_name}""")
            #print(f""" CREATE TABLE {tab_name} ({s[:-2]}) """)
            cursor.execute(f""" CREATE TABLE {tab_name} ({s[:-2]}) """)

        db.commit()
        db.close()

    def get_all_tables_name(self):
        db = sqlite3.connect(self.base_name)
        cursor = db.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table'")
        results = cursor.fetchall()
        db.close()
        return results


    def get_table_col_names(self, tab_name):
        db = sqlite3.connect(self.base_name)
        cursor = db.cursor()
        cursor.execute(f'SELECT * FROM {tab_name}')
        db.close()
        return [description[0] for description in cursor.description]

    def insert_data(self, tab_name, value_list):
        status = 'ok'
        try:
            connection = sqlite3.connect(self.base_name)
            cursor = connection.cursor()

            s = f'INSERT INTO {tab_name} ( '
            t = ' VALUES ('
            for value, column in zip(value_list, self.get_table_col_names(tab_name)[1:]):
                s += f"{column}, "
                t += f"?, "
            s, t = s[:-2] + ')', t[:-2] + ')'
            #print(s+t)
            cursor.execute(s + t, tuple(value_list))

            connection.commit()
        except Exception as e:
            connection.rollback()
            status = f'error {e}'
        finally:
            connection.close()
        return cursor.lastrowid, status

    def control_exist(self, tab_name, field_name, value):
        connection = sqlite3.connect(self.base_name)
        cursor = connection.cursor()

        cursor.execute(f'SELECT * FROM {tab_name} WHERE {field_name} = ?', [value])

        results = cursor.fetchall()
        connection.close()
        if len(results) > 0:
            return True
        else:
            return False

    def update_data(self, tab_name, id, name_list, value_list):
        status = 'ok'
        try:
            connection = sqlite3.connect(self.base_name)
            cursor = connection.cursor()

            s = f'UPDATE {tab_name} SET '
            for value, column in zip(value_list, name_list):
                s += f"{column} = ?, "
            s = s[:-2]
            s += 'WHERE id = ?'
            #print(s+t)
            value_list.append(id)
            cursor.execute(s, tuple(value_list))

            connection.commit()
        except Exception as e:
            connection.rollback()
            status = f'error {e}'
        finally:
            connection.close()
        return cursor.lastrowid, status

    def delete_row(self, tab_name, id):
        status = 'ok'
        try:
            connection = sqlite3.connect(self.base_name)
            cursor = connection.cursor()

            s = f'DELETE FROM {tab_name} WHERE id = ?'
            cursor.execute(s, (id,))

            connection.commit()
        except Exception as e:
            connection.rollback()
            status = f'error {e}'
        finally:
            connection.close()
        return status

    def get_all_tab_data(self, tab_name):
        connection = sqlite3.connect(self.base_name)
        cursor = connection.cursor()

        cursor.execute(f'SELECT * FROM {tab_name}')

        results = cursor.fetchall()
        connection.close()

        return results

    def get_all_tab_data_by_keys(self, tab_name, item_name, item_value):
        connection = sqlite3.connect(self.base_name)
        cursor = connection.cursor()

        cursor.execute(f'SELECT * FROM {tab_name} WHERE {item_name} = ?', [item_value])

        results = cursor.fetchall()
        connection.close()

        return results


def create(tab_name):
    db = sqlite3.connect('test_1.db')
    cursor = db.cursor()

    cursor.execute(f"""DROP TABLE IF EXISTS {tab_name}""")
    cursor.execute(f""" CREATE TABLE {tab_name}(
        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    	pos_name VARCHAR(255),
    	unicode VARCHAR(255),
    	units VARCHAR(255),
    	description text,
    	lower_limit INTEGER
    )
        """)

    db.commit()
    db.close()

def test_run():
    db = uny_litebase('test_2.db')

    #db.add_item('first_tab', 'name', 'VARCHAR(255)')
    #db.add_item('first_tab', 'number', 'INTEGER')
    #db.add_item('first_tab', 'amount', 'FLOAT')
    #db.add_item('first_tab', 'description', 'text')

    #db.add_item('second_tab', 'name', 'VARCHAR(255)')
    #db.add_item('second_tab', 'number', 'INTEGER')
    #db.add_item('second_tab', 'amount', 'VARCHAR(255)')

    #db.create_tables()

    #print(db.get_table_col_names('first_tab'))
    db.insert_data('first_tab', ['name1', 1, 2.44, 'abracadabra 1'])
    db.insert_data('first_tab', ['name2', 2, 3.14, 'abracadabra 2'])
    db.insert_data('second_tab', ['text 2', 10000, 'abracadabra 2'])
    db.insert_data('second_tab', ['ijbcuhjkedce', 23423, 'hello world'])

    for row in db.get_all_tab_data('first_tab'):
        print(row)

    for row in db.get_all_tab_data('second_tab'):
        print(row)

def create_file_base():
    db = uny_litebase('map_analytics_1.db')

    db.add_item('lcms_tab', 'map_id', 'INTEGER')
    db.add_item('lcms_tab', 'order_id', 'INTEGER')
    db.add_item('lcms_tab', 'map_position', 'VARCHAR(255)')
    db.add_item('lcms_tab', 'filename', 'VARCHAR(255)')
    db.add_item('lcms_tab', 'tags', 'VARCHAR(255)')

    #db.add_item('main_tab', 'chrom_filename', 'VARCHAR(255)')
    #db.add_item('main_tab', 'report_filename', 'VARCHAR(255)')

    db.create_tables()

def create_monitor_data_base():
    db = uny_litebase('monitor_changes_1.db')

    db.add_item('actual', 'job_id', 'VARCHAR(255)')
    db.add_item('actual', 'code_info', 'VARCHAR(255)')
    db.add_item('actual', 'date', 'VARCHAR(255)')
    db.add_item('actual', 'time', 'VARCHAR(255)')

    db.add_item('stack', 'job_id', 'VARCHAR(255)')

    db.create_tables()

def remake_stock_db():
    db = uny_litebase('stock_oligolab_5.db')

    db.add_item('total_tab', 'pos_name', 'VARCHAR(255)')
    db.add_item('total_tab', 'unicode', 'VARCHAR(255)')
    db.add_item('total_tab', 'units', 'VARCHAR(255)')
    db.add_item('total_tab', 'description', 'text')
    db.add_item('total_tab', 'lower_limit', 'INTEGER')

    db.add_item('input_tab', 'pos_name', 'VARCHAR(255)')
    db.add_item('input_tab', 'unicode', 'VARCHAR(255)')
    db.add_item('input_tab', 'amount', 'FLOAT')
    db.add_item('input_tab', 'date', 'VARCHAR(255)')
    db.add_item('input_tab', 'time', 'VARCHAR(255)')
    db.add_item('input_tab', 'telegram_id', 'VARCHAR(255)')

    db.add_item('output_tab', 'pos_name', 'VARCHAR(255)')
    db.add_item('output_tab', 'unicode', 'VARCHAR(255)')
    db.add_item('output_tab', 'amount', 'FLOAT')
    db.add_item('output_tab', 'date', 'VARCHAR(255)')
    db.add_item('output_tab', 'time', 'VARCHAR(255)')
    db.add_item('output_tab', 'telegram_id', 'VARCHAR(255)')

    db.add_item('users', 'name', 'VARCHAR(255)')
    db.add_item('users', 'telegram_id', 'VARCHAR(255)')
    db.add_item('users', 'status', 'VARCHAR(255)')
    db.add_item('users', 'pin', 'VARCHAR(255)')
    db.add_item('users', 'date', 'VARCHAR(255)')

    db.create_tables()

def create_request_hist_tab():
    db = uny_litebase('request_history_1.db')

    db.add_item('main_tab', 'user_name', 'VARCHAR(255)')
    #db.add_item('main_tab', 'user_id', 'VARCHAR(255)')
    db.add_item('main_tab', 'date', 'VARCHAR(255)')
    db.add_item('main_tab', 'time', 'VARCHAR(255)')
    db.add_item('main_tab', 'url', 'text')
    db.add_item('main_tab', 'remote_addr', 'VARCHAR(255)')

    db.create_tables()

def create_omap_history_table():
    db = uny_litebase('oligomap_history_2.db')

    db.add_item('main_tab', 'user_name', 'VARCHAR(255)')
    db.add_item('main_tab', 'date', 'VARCHAR(255)')
    db.add_item('main_tab', 'time', 'VARCHAR(255)')
    db.add_item('main_tab', 'url', 'text')
    db.add_item('main_tab', 'omap_json', 'text')

    db.create_tables()


def show_all_tabs():
    db = uny_litebase('stock_oligolab_5.db')
    print(db.get_table_col_names('users'))
    data = db.get_all_tab_data('users')
    for r in data:
        print(r)

    #db = uny_litebase('oligomap_history_2.db')
    #print(db.get_table_col_names('main_tab'))
    #data = db.get_all_tab_data('main_tab')
    #for r in data:
    #    print(r)

    #db = uny_litebase('stock_oligolab_5.db')
    #print(db.get_table_col_names('output_tab'))
    #data = db.get_all_tab_data('output_tab')
    #for r in data:
    #    print(r)

def rewrite_stock_db():
    db = uny_litebase('stock_oligolab_4.db')
    db_dest = uny_litebase('stock_oligolab_5.db')

    tab_name = ['total_tab', 'output_tab', 'input_tab', 'users']

    for t_n in tab_name:
        tab = db.get_all_tab_data(t_n)
        for row in tab:
            #print(row)
            #d = row[4][:row[4].find(' ')]
            #t = row[4][row[4].find(' ') + 1:row[4].find('.')]
            #insert_list = [row[1], row[2], row[3], d, t, row[5]]
            insert_list = list(row[1:])
            if t_n == 'users':
                insert_list.append('5511')
                insert_list.append(datetime.datetime.now().date().strftime('%d.%m.%Y'))
            print(insert_list)
            db_dest.insert_data(t_n, insert_list)



if __name__ == '__main__':
    #create('test')
    show_all_tabs()
    #create_file_base()
    #create_monitor_data_base()
    #remake_stock_db()
    #rewrite_stock_db()
    #create_request_hist_tab()
    #create_omap_history_table()