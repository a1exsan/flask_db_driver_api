import oligoSYN_lab_token as token
import telebot
from telebot import types
import qrcode_work
import stock_db_driver as db_manager
import solutions_db_driver as sol_db_manager
import interval_jobs
import stat_unit
import json


class users():

    def __init__(self, db_name, sol_db_name):
        self.db_name = db_name
        self.sol_db_name = sol_db_name
        self.users_db = dict()
        self.users_sol_db = dict()
        self.current_db = 'stock_oligolab_5'
        self.load_users()

    def load_users(self):
        db = db_manager.db_admin(db_name=self.db_name)
        user_list = db.get_users()
        for u in user_list:
            if u[3] != 'Deleted':
                self.users_db[u[2]] = db_manager.db_admin(self.db_name)
                self.users_sol_db[u[2]] = sol_db_manager.sol_db_admin(self.sol_db_name)
                self.users_db[u[2]].user_id = u[2]
                self.users_sol_db[u[2]].user_id = u[2]
                self.users_db[u[2]].user_name = u[1]
                self.users_sol_db[u[2]].user_name = u[1]
                print(self.users_db[u[2]], type(u[2]), u[2])



bot = telebot.TeleBot(token.TOKEN, parse_mode=None)
users_ = users('stock_oligolab_5.db', 'solutions_oligolab_1.db')

def send_text_message(text, user_id):
    bot.send_message(user_id, text)

def create_menu_in_out(message):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    #print(message.chat.id)
    if message.chat.id in [1848570232]:
        item1 = types.KeyboardButton("Списание")
        item2 = types.KeyboardButton("Приход")
        item3 = types.KeyboardButton("Остаток")
        item4 = types.KeyboardButton("Растворы")
        item5 = types.KeyboardButton("PIN")
        item6 = types.KeyboardButton("STAT")
        markup.add(item1, item2, item3)
        markup.add(item4, item5, item6)
        bot.send_message(message.chat.id, 'Выберите что вам надо', reply_markup=markup)
    else:
        item1 = types.KeyboardButton("Списание")
        item2 = types.KeyboardButton("Приход")
        item3 = types.KeyboardButton("Остаток")
        item4 = types.KeyboardButton("Растворы")
        item5 = types.KeyboardButton("PIN")
        markup.add(item1, item2, item3)
        markup.add(item4, item5)
        bot.send_message(message.chat.id, 'Выберите что вам надо', reply_markup=markup)

def create_solutions_menu(message):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    item1 = types.KeyboardButton("sПриготовить")
    item2 = types.KeyboardButton("sСписок")
    item3 = types.KeyboardButton("sСклад")
    markup.add(item1, item2)
    markup.add(item3)
    bot.send_message(message.chat.id, 'Выберите что вам надо', reply_markup=markup)

def create_menu_yes_no(message):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    item1 = types.KeyboardButton("Да")
    item2 = types.KeyboardButton("Нет")
    markup.add(item1)
    markup.add(item2)
    bot.send_message(message.chat.id, 'Подтвердить операцию?', reply_markup=markup)

def create_statistics_menu(message):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    item1 = types.KeyboardButton("Общая статистика")
    item2 = types.KeyboardButton("Заказы в работе")
    item3 = types.KeyboardButton("За сегодня")
    item4 = types.KeyboardButton("За 5 дней")
    item5 = types.KeyboardButton("За 30 дней")
    item6 = types.KeyboardButton("sСклад")
    markup.add(item1, item2)
    markup.add(item3, item4, item5)
    markup.add(item6)
    bot.send_message(message.chat.id, 'Выберите что вам надо', reply_markup=markup)

@bot.message_handler(commands=['dumpdb'])
def dump_db(message):
    if message.chat.id == 1848570232:
        bot.reply_to(message, 'dumping')
        filename_list = [
            'asm2000_map_1.db',
            'request_history_1.db',
            'stock_oligolab_5.db',
            'scheduler_oligolab_2.db',
            'map_analytics_1.db'
        ]
        for filename in filename_list:
            print(filename)
            with open(filename, 'rb') as f:
                doc = f.read()
            bot.send_document(message.chat.id, document=doc, caption=filename)

@bot.message_handler(commands=['get_id'])
def get_id(message):
    print(message.from_user.id)
    print(type(message.chat.id))
    bot.reply_to(message, message.from_user.id)

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    bot.reply_to(message, "How are you doing?")
    create_menu_in_out(message)

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    #print(message.chat.id)
    u_ids = list(users_.users_db.keys())
    c_u = str(message.chat.id)
    if c_u in u_ids:
        bot.reply_to(message, 'keep photo')

        fileID = message.photo[-1].file_id
        print(fileID)
        file_info = bot.get_file(fileID)
        print(file_info.file_path)
        downloaded_file = bot.download_file(file_info.file_path)
        with open('test.png', 'wb') as f:
            f.write(downloaded_file)

        qr = qrcode_work.qr_reader('test.png')
        try:
            value = qr.read()
            bot.reply_to(message, str(value))
            if users_.current_db == 'stock_oligolab_5':
                users_.users_db[c_u].current_unicode = str(value)
            elif users_.current_db == 'solutions_oligolab_1':
                users_.users_sol_db[c_u].current_unicode = str(value)
        except:
            bot.reply_to(message, 'Bad request, Try again')

@bot.message_handler(content_types=['text'])
def send_menu_in_out(message):
    u_ids = list(users_.users_db.keys())
    c_u = str(message.chat.id)
    if c_u in u_ids:
        if message.text in ['Списание', 'Приход']:
            users_.users_db[c_u].current_operation = message.text
            print(users_.users_db[c_u])
            bot.reply_to(message, f'текущая позиция - [ '
                                  f'{users_.users_db[c_u].get_pos_name(users_.users_db[c_u].current_unicode)}]')

        elif '.' in message.text:
            #print('float', message.text)
            if users_.current_db == 'stock_oligolab_5':
                users_.users_db[c_u].current_amount = float(message.text.replace(',', '.'))
                create_menu_yes_no(message)
            elif users_.current_db == 'solutions_oligolab_1':
                users_.users_sol_db[c_u].current_volume = float(message.text.replace(',', '.'))
                create_menu_yes_no(message)

        elif message.text == 'Да':

            if users_.current_db == 'stock_oligolab_5':
                out = users_.users_db[c_u].add_goods(users_.users_db[c_u].current_amount)
                bot.reply_to(message, out)
                create_menu_in_out(message)
            elif users_.current_db == 'solutions_oligolab_1':
                text = users_.users_sol_db[c_u].prepare_solution()
                bot.reply_to(message, text)
                create_solutions_menu(message)

        elif message.text == 'Нет':

            if users_.current_db == 'stock_oligolab_5':
                create_menu_in_out(message)
            elif users_.current_db == 'solutions_oligolab_1':
                create_solutions_menu(message)

        elif message.text == 'Остаток':
            bot.reply_to(message, f'Остаток по позиции - [ '
                                  f'{users_.users_db[c_u].get_pos_name(users_.users_db[c_u].current_unicode)}] составил '
                                  f'{users_.users_db[c_u].get_remaining_stock(users_.users_db[c_u].current_unicode)}')
        elif message.text == 'Растворы':
            users_.current_db = 'solutions_oligolab_1'
            create_solutions_menu(message)
        elif message.text == 'sСклад':
            users_.current_db = 'stock_oligolab_5'
            create_menu_in_out(message)
        elif message.text == 'sПриготовить':
            text, compos, vols = users_.users_sol_db[c_u].get_sol_composition(users_.users_sol_db[c_u].current_unicode)
            bot.reply_to(message, f' введите возможный объем раствора: '
                                  f'{users_.users_sol_db[c_u].get_sol_name(users_.users_sol_db[c_u].current_unicode)}: '
                                  f'{str(vols)}')

        elif message.text == 'sСписок':
            markup = users_.users_sol_db[c_u].get_solutions_list_menu()
            bot.send_message(message.chat.id, 'Выберите раствор из списка:', reply_markup=markup)

        elif message.text == 'sExit':
            create_solutions_menu(message)

        elif users_.users_sol_db[c_u].get_answer_solution(message=message):
            text, compos, vols = users_.users_sol_db[c_u].get_sol_composition(users_.users_sol_db[c_u].current_unicode)
            bot.reply_to(message, f' введите возможный объем раствора: '
                                  f'{users_.users_sol_db[c_u].get_sol_name(users_.users_sol_db[c_u].current_unicode)}: '
                                  f'{str(vols)}')
        elif message.text == 'PIN':
            pin_manager = interval_jobs.pincode_manager()
            pins = pin_manager.get_pins()
            bot.reply_to(message, f'Ваш код: {pins[c_u]}')

        elif message.text == 'STAT':
            create_statistics_menu(message)
        elif message.text == 'Общая статистика':
            st_hist = stat_unit.status_history_stat()
            data = json.loads(st_hist.get_last_update()[3])
            #print(data)
            bot.reply_to(message, f"Статистика по олигонуклеотидам на {st_hist.get_last_update()[1]} "
                                  f"{st_hist.get_last_update()[2]}:")
            bot.reply_to(message, f"очередь: {data['in queue']}")
            bot.reply_to(message, f"синтез: {data['synthesis']}")
            bot.reply_to(message, f"очистка: {data['purification']}")
            bot.reply_to(message, f"лиофилизация: {data['formulation']}")
            bot.reply_to(message, f"готово: {data['finished']}")

        elif message.text == 'Заказы в работе':
            stat = stat_unit.orders_statistic()
            data = stat.get_orders_in_progress()
            if len(data) < 5 and len(data) > 1:
                bot.reply_to(message, f"В работе {len(data)} заказа:")
            elif len(data) > 4:
                bot.reply_to(message, f"В работе {len(data)} заказов:")
            else:
                bot.reply_to(message, f"В работе {len(data)} заказ:")
            for row in data:
                bot.reply_to(message, str(row))

        elif message.text == 'За сегодня':
            st_hist = stat_unit.status_history_stat()
            data = st_hist.get_last_x_days_period(days=1)
            for key in data.keys():
                bot.reply_to(message, f"{key}: {data[key]}")

        elif message.text == 'За 5 дней':
            st_hist = stat_unit.status_history_stat()
            data = st_hist.get_last_x_days_period(days=5)
            for key in data.keys():
                bot.reply_to(message, f"{key}: {data[key]}")

        elif message.text == 'За 30 дней':
            st_hist = stat_unit.status_history_stat()
            data = st_hist.get_last_x_days_period(days=30)
            for key in data.keys():
                bot.reply_to(message, f"{key}: {data[key]}")




if __name__ == '__main__':
    bot.polling(none_stop=True)