import sqlite3
from datetime import datetime

import pandas as pd


class reagent_name_accordance():

    def __init__(self):
        self.accord = {
            'A': 'INIT_BASE_CODE_OLIGO_LAB_0000120',
            'C': 'INIT_BASE_CODE_OLIGO_LAB_0000121',
            'G': 'INIT_BASE_CODE_OLIGO_LAB_0000122',
            'T': 'INIT_BASE_CODE_OLIGO_LAB_0000123',
            '+A': 'INIT_BASE_CODE_OLIGO_LAB_0000124',
            '+C': 'INIT_BASE_CODE_OLIGO_LAB_0000126',
            '+G': 'INIT_BASE_CODE_OLIGO_LAB_0000125',
            '+T': 'INIT_BASE_CODE_OLIGO_LAB_0000127',
            '6FAM': 'INIT_BASE_CODE_OLIGO_LAB_0000128',
            'HEX': 'INIT_BASE_CODE_OLIGO_LAB_0000129',
            'R6G': 'INIT_BASE_CODE_OLIGO_LAB_0000131',
            'Alk': 'INIT_BASE_CODE_OLIGO_LAB_0000133'
        }

class db_admin():

    def __init__(self, db_name):
        self.db_name = db_name
        self.current_operation = 'Списание'
        self.user_id = 0
        self.user_name = ''
        self.current_unicode = ''
        self.current_amount = 0.

    def __str__(self):
        return f'{self.user_name} // {self.user_id} // {self.current_operation} // {self.current_unicode}'

    def insert_total_tab(self,
                         pos_name,
                         unicode,
                         units,
                         description,
                         lower_limit,
                         update_need=False):

        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()

        cursor.execute(f'SELECT * FROM total_tab WHERE unicode = ?',
                       [unicode])
        results = cursor.fetchall()

        if len(results) == 0:

            cursor.execute("""INSERT INTO total_tab (
                                            pos_name, unicode, units, description, lower_limit) 
                                            VALUES (?, ?, ?, ?, ?)""",
                       (pos_name, unicode, units, description, lower_limit))
        else:
            if update_need:
                cursor.execute("""UPDATE total_tab SET pos_name = ?, units = ?, description = ?, lower_limit = ?
                                                        WHERE unicode = ? """,
                               (pos_name, units, description, lower_limit, unicode))

        connection.commit()
        connection.close()

    def delete_row(self, id):
        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()

        cursor.execute("""DELETE FROM total_tab WHERE id = ?""",
                           (id,))

        connection.commit()
        connection.close()

    def delete_from_total_tab(self, id):
        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()
        cursor.execute("""DELETE FROM total_tab WHERE id = ?""",
                       (id,))
        connection.commit()
        connection.close()


    def show_all_data_in_table(self, table_name):

        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()

        if table_name == 'total_tab':
            cursor.execute(f'SELECT id, pos_name, unicode, units, description, lower_limit FROM {table_name}')
        elif table_name == 'users':
            cursor.execute(f'SELECT id, name, telegram_id, status FROM {table_name}')
        elif table_name == 'output_tab' or table_name == 'input_tab':
            cursor.execute(f'SELECT id, pos_name, unicode, amount, date, telegram_id FROM {table_name}')

        results = cursor.fetchall()

        text = ''
        for row in results:
            text += f' {row} \n'

        connection.close()
        return text

    def get_all_data_in_tab(self, tab_name):
        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()

        cursor.execute(f'SELECT * FROM {tab_name}')

        results = cursor.fetchall()
        connection.close()

        return results

    def add_user(self,
                 name,
                 telegram_id,
                 status,
                 update_need=False):
        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()

        cursor.execute(f'SELECT * FROM users WHERE telegram_id = ?',
                       [telegram_id])
        results = cursor.fetchall()

        if len(results) == 0:

            cursor.execute("""INSERT INTO users (
                                            name, telegram_id, status) 
                                            VALUES (?, ?, ?)""",
                       (name, telegram_id, status))
        else:
            if update_need:
                cursor.execute("""UPDATE users SET name = ?, status = ?
                                                        WHERE telegram_id = ? """,
                               (name, status, telegram_id))

        connection.commit()
        connection.close()

    def get_users(self):
        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()

        cursor.execute(f'SELECT * FROM users')
        results = cursor.fetchall()
        connection.commit()
        connection.close()
        return results

    def get_user_status(self, telegram_id):
        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()

        cursor.execute(f'SELECT * FROM users WHERE telegram_id = ?',
                       [telegram_id])
        results = cursor.fetchall()
        connection.commit()
        connection.close()
        if len(results) > 0:
            return results[0][3]
        else:
            'None'

    def get_user_name(self, telegram_id):
        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()

        cursor.execute(f'SELECT * FROM users WHERE telegram_id = ?',
                       [telegram_id])
        results = cursor.fetchall()
        connection.commit()
        connection.close()
        if len(results) > 0:
            return results[0][1]
        else:
            'None'

    def insert_in_out_put_tab(self,
                              tab_name,
                              pos_name,
                              unicode,
                              amount,
                              date,
                              telegram_id):

        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()

        cursor.execute(f'SELECT * FROM users WHERE telegram_id = ?',
                       [telegram_id])
        results = cursor.fetchall()

        #print(tab_name, pos_name, unicode, amount, date, telegram_id)
        #print(results)

        if len(results) > 0:
            if results[0][3] in ['owner', 'user', 'admin']:
                if tab_name == 'input_tab':
                    #print(results)
                    #print(tab_name, pos_name, unicode, amount, date, telegram_id)
                    cursor.execute("""INSERT INTO input_tab (
                                                            pos_name, unicode, amount, date, telegram_id) 
                                                            VALUES (?, ?, ?, ?, ?)""",
                               (pos_name, unicode, amount, date, telegram_id))
                elif tab_name == 'output_tab':
                    #print(tab_name, pos_name, unicode, amount, date, telegram_id)
                    cursor.execute("""INSERT INTO output_tab (
                                                            pos_name, unicode, amount, date, telegram_id) 
                                                            VALUES (?, ?, ?, ?, ?)""",
                               (pos_name, unicode, amount, date, telegram_id))
                connection.commit()
                connection.close()

    def get_pos_name(self, unicode):
        pos_name, units = '', ''
        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()

        cursor.execute(f'SELECT id, pos_name, units FROM total_tab WHERE unicode = ?',
                       [unicode])
        results = cursor.fetchall()
        if len(results) > 0:
            pos_name = results[0][1]
            units = results[0][2]
        connection.commit()
        connection.close()
        return pos_name, units

    def add_goods(self, amount):
        print(self.user_name, self.current_operation, self.current_unicode, amount)

        pos_name, units = self.get_pos_name(self.current_unicode)
        if pos_name != '':
            s = f'{self.user_name}; {pos_name}; {self.current_unicode}; {amount} {units}'
        else:
            s = f'товар [ {self.current_unicode} ] не зарегистрирован в базе'

        if self.current_operation == 'Списание' and pos_name != '':

            rr = self.get_remaining_stock(unicode=self.current_unicode)
            if rr >= amount:
                self.insert_in_out_put_tab('output_tab',
                                           pos_name,
                                           self.current_unicode,
                                           amount,
                                           datetime.now(),
                                           self.user_id)
                s = (f'{self.user_name}; {self.current_operation}; {pos_name}; {amount} {units}; '
                     f'остаток {rr - amount}')
            else:
                s = f'{self.user_name}; {self.current_operation}; {pos_name}; {amount} недостаточно'

        elif self.current_operation == 'Приход' and pos_name != '':
            self.insert_in_out_put_tab('input_tab',
                                       pos_name,
                                       self.current_unicode,
                                       amount,
                                       datetime.now(),
                                       self.user_id)
            rr = self.get_remaining_stock(unicode=self.current_unicode)
            s = (f'{self.user_name}; {self.current_operation}; {pos_name}; {amount} {units}; '
                 f'остаток {rr}')

        return s

    def get_remaining_stock(self, unicode):
        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()

        cursor.execute(f'SELECT id, amount FROM input_tab WHERE unicode = ?',
                       [unicode])
        results = cursor.fetchall()
        input_count = 0
        for row in results:
            input_count += row[1]

        cursor.execute(f'SELECT id, amount FROM output_tab WHERE unicode = ?',
                       [unicode])
        results = cursor.fetchall()
        output_count = 0
        for row in results:
            output_count += row[1]

        connection.commit()
        connection.close()

        return input_count - output_count

def test():
    db = db_admin('stock_oligolab_1.db')
    db.insert_total_tab(pos_name='Ацетонитрил, ХЧ', unicode='BASE_CODE_CONTENT_CODE_12345', units='1L flask',
                        description='Ацетонитрил, ХЧ; 80 ppm H2O; для синтеза (необходимо сушить над ситами 3 или 4 ангстрема) и ВЭЖХ',
                        lower_limit=3, update_need=True)
    db.add_user(name='Alex Fomin', telegram_id='1848570232', status='owner', update_need=True)
    db.insert_in_out_put_tab(tab_name='input_tab', pos_name='Ацетонитрил, ХЧ', unicode='CODE12345',
                             amount=10, date=datetime.now(), telegram_id='1848570232')
    #db.delete_from_total_tab(3)
    #db.delete_from_total_tab(4)
    print(db.show_all_data_in_table('total_tab'))
    print(db.show_all_data_in_table('users'))
    print(db.show_all_data_in_table('input_tab'))
    #print(db.show_all_data_in_table('output_tab'))

def test2():
    db = db_admin('stock_oligolab_1.db')
    print(db.show_all_data_in_table('users'))
    print(db.show_all_data_in_table('input_tab'))
    print(db.show_all_data_in_table('output_tab'))
    print(db.show_all_data_in_table('total_tab'))


def test3():
    db = db_admin('stock_oligolab_1.db')
    db.insert_total_tab(pos_name='Ацетонитрил, ХЧ', unicode='INIT_BASE_CODE_OLIGO_LAB_0000001', units='1L flask',
                        description='Ацетонитрил, ХЧ; 80 ppm H2O; для синтеза (необходимо сушить над ситами 3 или 4 ангстрема) и ВЭЖХ',
                        lower_limit=3, update_need=True)

def test4():
    db = db_admin('stock_oligolab_3.db')
    #db.add_user(name='Alex Fomin', telegram_id='1848570232', status='owner', update_need=True)
    #db.add_user(name='Elena Melnik', telegram_id='1783121115', status='user', update_need=True)
    db.add_user(name='Anna Shabelnikova', telegram_id='1859529109', status='user', update_need=True)

def init_base_load():
    df = pd.read_csv('init_base_load.csv', sep='\t')

    db = db_admin('stock_oligolab_1.db')

    #print(df)

    count = 118
    for pos_name, unicode, units, description, lower_limit, amount in zip(
        df['Name'], df['Unicode'], df['Units'], df['Description'], df['lower_limit'], df['Amount']
    ):
        if str(count) in unicode:
            db.insert_total_tab(pos_name=pos_name, unicode=unicode, units=units,
                            description=description,
                            lower_limit=lower_limit, update_need=True)
            #if db.get_remaining_stock(unicode) == 0:
            #    db.insert_in_out_put_tab(tab_name='input_tab', pos_name=pos_name, unicode=unicode, amount=amount,
            #                             date=datetime.now(), telegram_id=1848570232)
            count += 1
                                     


def show_total_tab():
    db = db_admin('stock_oligolab_3.db')
    #print(db.show_all_data_in_table('total_tab'))

    data = db.get_all_data_in_tab('output_tab')
    for row in data:
        print(row)

def copy_db():
    db_1 = db_admin('stock_oligolab_1.db')
    db_3 = db_admin('stock_oligolab_3.db')

    data = db_1.get_all_data_in_tab('total_tab')
    for row in data:
        db_3.insert_total_tab(row[1], row[2], row[3], row[4], row[5])

    data = db_1.get_all_data_in_tab('users')
    for row in data:
        db_3.add_user(row[1], row[2], row[3])

    data = db_1.get_all_data_in_tab('input_tab')
    #print(data)
    for row in data:
        db_3.insert_in_out_put_tab('input_tab', row[1], row[2], row[3], row[4], row[5])

    data = db_1.get_all_data_in_tab('output_tab')
    for row in data:
        db_3.insert_in_out_put_tab('output_tab', row[1], row[2], row[3], row[4], row[5])


if __name__ == '__main__':
    #show_total_tab()
    test4()
    #init_base_load()
    #copy_db()