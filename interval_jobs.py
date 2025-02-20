from flask_apscheduler import APScheduler
import uny_db_driver
import pandas as pd
import json
from datetime import datetime
import random

import telebot
import oligoSYN_lab_token
#import tg_bot
import subprocess

class pincode_manager():
    def __init__(self):
        self.db_name = 'stock_oligolab_5.db'
        self.limit_days = 1

    def generate_pincodes(self):
        db = uny_db_driver.uny_litebase(self.db_name)
        data = db.get_all_tab_data('users')
        for u in data:
            pin = str(random.randint(1000, 10000))
            d = datetime.now().date().strftime('%d.%m.%Y')
            db.update_data('users', u[0], ['pin', 'date'], [pin, d])
            #print(u[0], pin)

    def check_pincodes(self):
        db = uny_db_driver.uny_litebase(self.db_name)
        data = db.get_all_tab_data('users')
        last_date = datetime.strptime(data[0][5], '%d.%m.%Y')
        now_date = datetime.now()

        if (now_date - last_date).days >= self.limit_days:
            #print((now_date - last_date).days)
            self.generate_pincodes()

    def get_tokens(self):

        self.check_pincodes()

        db = uny_db_driver.uny_litebase(self.db_name)
        data = db.get_all_tab_data('users')
        tokens = {}
        for u in data:
            tokens[u[4]] = u[1]
        return tokens

    def get_pins(self):
        self.check_pincodes()
        db = uny_db_driver.uny_litebase(self.db_name)
        data = db.get_all_tab_data('users')
        pins = {}
        for u in data:
            pins[u[2]] = u[4]
        return pins


class data_changes_monitor():
    def __init__(self):
        self.db_name = 'asm2000_map_1.db'
        self.change_db_name = 'monitor_changes_1.db'
        self.date_format = "%d.%m.%Y"
        self.time_format = "%H:%M:%S"

    def send_message(self, chat_id, text):
        bot = telebot.TeleBot(oligoSYN_lab_token.TOKEN, parse_mode=None)
        bot.send_message(chat_id, text)

    def get_oligomaps_data(self):
        db = uny_db_driver.uny_litebase(self.db_name)
        data = db.get_all_tab_data('main_map')
        out = []
        for r in data:
                d = {}
                d['#'] = r[0]
                d['Map name'] = r[2]
                d['Synth number'] = r[3]
                d['Date'] = r[1]
                d['map data'] = pd.DataFrame(json.loads(r[4]))
                out.append(d)
        return out

    def get_actual_maps(self):
        total_maps = self.get_oligomaps_data()
        if len(total_maps) > 0:
            out = []
            for row in total_maps:
                df = row['map data']
                if df.shape[0] > 0:
                    if df[(df['DONE'] == True)|(df['Wasted'] == True)].shape[0] != df.shape[0]:
                        d = {}
                        d['#'] = row['#']
                        d['Map name'] = row['Map name']
                        d['Synth number'] = row['Synth number']
                        d['Date'] = row['Date']
                        d['map data'] = row['map data']
                        out.append(d)
            return out
        else:
            return []

    def monitor_oligomap_status_task(self):

        total_maps = self.get_actual_maps()
        if len(total_maps) > 0:
            for row in total_maps:
                text = f"{row['Map name']}  "
                df = row['map data']
                if df.shape[0] > 0:
                    text  += f"Wasted: {df[df['Wasted'] == True].shape[0]}/{df.shape[0]}; "
                    text  += f"DONE: {df[df['DONE'] == True].shape[0]}/{df.shape[0]}; "
                    text  += f"LCMS: {df[df['Done LCMS'] == True].shape[0]}/{df[df['Do LCMS'] == True].shape[0]}; "

                db = uny_db_driver.uny_litebase(self.change_db_name)
                data = db.get_all_tab_data_by_keys('actual', 'job_id', row['Map name'])
                if len(data) == 0:
                    db.insert_data('actual', [row['Map name'], text,
                                          datetime.now().date().strftime(self.date_format),
                                          datetime.now().time().strftime(self.time_format)])
                    db.insert_data('stack', [row['Map name']])
                    self.send_message('1848570232', text)
                    print(text)
                else:
                    ctrl = False
                    for r in data:
                        if r[2] == text:
                            ctrl = True
                            break
                    if not ctrl:
                        db.insert_data('actual', [row['Map name'], text,
                                              datetime.now().date().strftime(self.date_format),
                                              datetime.now().time().strftime(self.time_format)])
                        self.send_message('1848570232', text)
                        print(text)

        db = uny_db_driver.uny_litebase(self.change_db_name)
        stack_data = db.get_all_tab_data('stack')
        if len(stack_data) > len(total_maps):
            pass



class job_class():
    def __init__(self, app):
        self.name = 'Job class'
        self.scheduler = APScheduler()
        self.scheduler.init_app(app)
        self.scheduler.api_enabled = True
        self.scheduler.start()
        self.monitor = data_changes_monitor()

        self.telegram_bot_proc = subprocess.Popen(['python', 'tg_bot.py'])

    def interval_task(self):
        print('Hello APScheduler')

        #bot = telebot.TeleBot(oligoSYN_lab_token.TOKEN, parse_mode=None)
        #bot.send_message('1848570232', 'Hello Alex')

    def add_job_1(self):
        self.scheduler.add_job(id='test_job_1', func=self.interval_task, trigger='interval', seconds=20)

    def add_oligomap_status_monitor_job(self):
        self.scheduler.add_job(id='oligomap_status_monitor_1',
                               func=self.monitor.monitor_oligomap_status_task, trigger='interval', seconds=20)
    def tg_bot_add_job(self):
        self.scheduler.add_job(id='run_telegram_bot_1',
                               func=self.run_bot, trigger='interval', seconds=60*1)

    def run_bot(self):
        print('Check Telegram Bot')
        if self.telegram_bot_proc.poll() != None:
            print('RE_RUN Telegram Bot')
            self.telegram_bot_proc = subprocess.Popen(['python', 'tg_bot.py'])


def test1():
    pins = pincode_manager()
    print(pins.get_tokens())

if __name__ == '__main__':
    test1()