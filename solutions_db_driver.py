import sqlite3
from datetime import datetime
from telebot import types

import pandas as pd
import json

class sol_db_admin():

    def __init__(self, db_name):
        self.db_name = db_name
        self.user_id = 0
        self.user_name = ''
        self.current_unicode = ''
        self.current_amount = 0
        self.current_volume = 0

        self.unicod_list = [
            'INIT_BASE_CODE_OLIGO_LAB_s0000001',
            'INIT_BASE_CODE_OLIGO_LAB_s0000002',
            'INIT_BASE_CODE_OLIGO_LAB_s0000003',
            'INIT_BASE_CODE_OLIGO_LAB_s0000004',
            'INIT_BASE_CODE_OLIGO_LAB_s0000005',
        ]

        self.name_list = [
            'DEBL',
            'ACTIV',
            'CAPA',
            'CAPB',
            'OXID'
        ]

    def insert_compounds_tab(self,
                             name,
                             unicode,
                             conc_json,
                             volumes,
                             description,
                             update_need=False):

        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()

        cursor.execute(f'SELECT * FROM compounds WHERE unicode = ?',
                       [unicode])
        results = cursor.fetchall()

        if len(results) == 0:

            cursor.execute("""INSERT INTO compounds (
                                            name, unicode, conc_json, volumes, description) 
                                            VALUES (?, ?, ?, ?, ?)""",
                       (name, unicode, conc_json, volumes, description))
        else:
            if update_need:
                cursor.execute("""UPDATE compounds SET name = ?, conc_json = ?, volumes = ?, description = ?
                                                        WHERE unicode = ? """,
                               (name, conc_json, volumes, description, unicode))

        connection.commit()
        connection.close()

    def check_unicode(self, unicode):
        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()

        cursor.execute(f'SELECT * FROM compounds WHERE unicode = ?',
                       [unicode])
        results = cursor.fetchall()
        connection.close()
        if len(results) > 0:
            return True
        else:
            return False

    def show_all_data_in_table(self, table_name):

        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()

        cursor.execute(f'SELECT * FROM {table_name}')

        results = cursor.fetchall()

        text = ''
        for row in results:
            text += f' {row} \n'

        connection.close()
        return text

    def get_sol_composition(self, unicode):
        out = ''
        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()
        cursor.execute(f'SELECT * FROM compounds WHERE unicode = ?',
                       [unicode])
        results = cursor.fetchall()
        if len(results) > 0:
            compos_dict = json.loads(results[0][3])
            volumes = json.loads(results[0][4])
            out = str(compos_dict)
        else:
            compos_dict = {}
            volumes = {}

        connection.close()
        return out, compos_dict, volumes

    def get_sol_name(self, unicode):
        out = ''
        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()
        cursor.execute(f'SELECT * FROM compounds WHERE unicode = ?',
                       [unicode])
        results = cursor.fetchall()
        if len(results) > 0:
            out = results[0][1]

        connection.close()
        return out

    def prepare_solution(self):
        text = 'Раствор не найден в базе'
        if self.check_unicode(self.current_unicode):
            text, compos, vol = self.get_sol_composition(self.current_unicode)

            connection = sqlite3.connect(self.db_name)
            cursor = connection.cursor()
            cursor.execute("""INSERT INTO preparation_history (
                                                    unicode, volume, units, amount, date, telegram_id) 
                                                    VALUES (?, ?, ?, ?, ?, ?)""",
                       (self.current_unicode, self.current_volume, 'ml', 1,
                        datetime.now(), self.user_id))
            connection.commit()
            connection.close()

            text = 'Состав:\n'
            for key in list(compos.keys()):
                if list(compos[key].keys())[0] == '%':
                    text += f"{key} - {compos[key]['%'] * self.current_volume / 100} ml;\n"
                elif list(compos[key].keys())[0] in 'g/ml':
                    text += f"{key} - {compos[key]['g/ml'] * self.current_volume} g;\n"
        return text

    def get_solutions_list_menu(self):

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        item1 = types.KeyboardButton("DEBL")
        item2 = types.KeyboardButton("ACTIV")
        item3 = types.KeyboardButton("CAPA")
        item4 = types.KeyboardButton("CAPB")
        item5 = types.KeyboardButton("OXID")
        item6 = types.KeyboardButton("sExit")
        markup.add(item1, item2)
        markup.add(item3, item4)
        markup.add(item5, item6)
        return markup

    def get_answer_solution(self, message):
        if message.text in self.name_list:
            self.current_unicode = self.unicod_list[self.name_list.index(message.text)]
            return True
        else:
            self.current_unicode = ''
            return False



def add_solution():

    name = 'DEBL'
    unicode = 'INIT_BASE_CODE_OLIGO_LAB_s0000001'
    conc_json = json.dumps({
                'дихлоруксусная кислота': {'%': 3},
                'бензол': {'%': 97}
    })
    volumes = json.dumps({
                '250 ml': {'ml': 250},
                '500 ml': {'ml': 500},
                '1000 ml': {'ml': 1000}
    })
    description = 'Раствор для снятия диметокситритильной защиты амидитов'

    #print(name, unicode, conc_json, volumes, description)

    db = sol_db_admin('solutions_oligolab_1.db')
    db.insert_compounds_tab(name=name, unicode=unicode, conc_json=conc_json, volumes=volumes,
                            description=description, update_need=True)


def add_capA_capB_OXID():

    name = 'CAPA'
    unicode = 'INIT_BASE_CODE_OLIGO_LAB_s0000003'
    conc_json = json.dumps({
                'пропионовый ангидрид': {'%': 15},
                'ацетонитрил': {'%': 85}
    })
    volumes = json.dumps({
                '200 ml': {'ml': 200},
                '400 ml': {'ml': 400},
                '800 ml': {'ml': 800}
    })
    description = 'Раствор для кэпирования непрореагировавших ОН групп'

    #print(name, unicode, conc_json, volumes, description)

    db = sol_db_admin('solutions_oligolab_1.db')
    db.insert_compounds_tab(name=name, unicode=unicode, conc_json=conc_json, volumes=volumes,
                            description=description, update_need=True)

    name = 'CAPB'
    unicode = 'INIT_BASE_CODE_OLIGO_LAB_s0000004'
    conc_json = json.dumps({
        'метилимидазол': {'%': 10},
        'пиридин': {'%': 10},
        'ацетонитрил': {'%': 80}
    })
    volumes = json.dumps({
        '200 ml': {'ml': 200},
        '400 ml': {'ml': 400},
        '800 ml': {'ml': 800}
    })
    description = 'Раствор для кэпирования непрореагировавших ОН групп'

    # print(name, unicode, conc_json, volumes, description)

    db = sol_db_admin('solutions_oligolab_1.db')
    db.insert_compounds_tab(name=name, unicode=unicode, conc_json=conc_json, volumes=volumes,
                            description=description, update_need=True)

    name = 'OXID'
    unicode = 'INIT_BASE_CODE_OLIGO_LAB_s0000005'
    conc_json = json.dumps({
        'вода': {'%': 2},
        'пиридин': {'%': 10},
        'йод, кристаллический': {'g/ml': 0.0052},
        'ацетонитрил': {'%': 88}
    })
    volumes = json.dumps({
        '500 ml': {'ml': 500},
        '1000 ml': {'ml': 1000}
    })
    description = 'Раствор для окисления фосфора'

    # print(name, unicode, conc_json, volumes, description)

    db = sol_db_admin('solutions_oligolab_1.db')
    db.insert_compounds_tab(name=name, unicode=unicode, conc_json=conc_json, volumes=volumes,
                            description=description, update_need=True)


def add_ACTIV_BTT():

    name = 'activator BTT'
    unicode = 'INIT_BASE_CODE_OLIGO_LAB_s0000002'
    conc_json = json.dumps({
        'бензилтиотетразол': {'g/ml': 0.05},
        'ацетонитрил': {'%': 100}
    })
    volumes = json.dumps({
        '200 ml': {'ml': 200},
        '400 ml': {'ml': 400}
    })
    description = 'Раствор активатора для синтеза олигов'

    # print(name, unicode, conc_json, volumes, description)

    db = sol_db_admin('solutions_oligolab_1.db')
    db.insert_compounds_tab(name=name, unicode=unicode, conc_json=conc_json, volumes=volumes,
                            description=description, update_need=True)
def show_data():
    db = sol_db_admin('solutions_oligolab_1.db')
    print(db.show_all_data_in_table(table_name='compounds'))
    print(db.show_all_data_in_table(table_name='preparation_history'))

if __name__ == '__main__':
    #add_solution()
    #add_ACTIV_BTT()
    add_capA_capB_OXID()
    show_data()

