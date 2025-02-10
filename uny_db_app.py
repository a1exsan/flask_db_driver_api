
from flask import Flask, request
from flask_cors import CORS
import uny_db_driver
import json
import interval_jobs
import sqlite3

from flask_httpauth import HTTPBasicAuth
from flask_httpauth import HTTPTokenAuth
from werkzeug.security import generate_password_hash, check_password_hash
import os


app = Flask(__name__)
CORS(app)

#auth = HTTPBasicAuth()
auth = HTTPTokenAuth(scheme='Pincode')
pin_auth = interval_jobs.pincode_manager()
tokens = pin_auth.get_tokens()

#@auth.verify_password
#def verify_pwd(username, password):
#    if (username in users and
#        check_password_hash(users.get(username), password)):
#        return username

@auth.verify_token
def verify_token(token):
    if token in tokens:
        return tokens[token]

@app.route('/', methods=['GET', 'POST'])
#@auth.login_required
def home():
    print(request.date)
    print(request.data)
    #print(request.authorization)
    #print(request.headers.get('X-Api-Key'))
    #print(auth.get_auth())
    return f"DataBase {auth.current_user()}"

@app.route('/get_info/<db_name>')
#@auth.login_required
def get_db_info(db_name):
    #print(request.remote_addr)
    #print(auth.current_user())
    db = uny_db_driver.uny_litebase(db_name)
    tabs = db.get_all_tables_name()
    tabs = [i[0] for i in tabs]
    info = {}
    for tab in tabs:
        record_number = db.get_all_tab_data(tab)
        cols = db.get_table_col_names(tab)
        info[tab] = cols
        info[f'{tab}_records'] = len(record_number)
    if info != None:
        return info, 200
    return 'Server error', 404

@app.route('/get_info/<db_name>/<tab_name>')
@auth.login_required
def get_db_tab_info(db_name, tab_name):
    uny_db_driver.history_agent(request, auth)

    db = uny_db_driver.uny_litebase(db_name)
    cols = db.get_table_col_names(tab_name)
    if cols != None:
        return cols, 200
    return 'Server error', 404

@app.route('/get_all_tab_data/<db_name>/<tab_name>')
@auth.login_required
def get_all_tab_data(db_name, tab_name):
    uny_db_driver.history_agent(request, auth)

    db = uny_db_driver.uny_litebase(db_name)
    data = db.get_all_tab_data(tab_name)
    if data != None:
        return data, 200
    return 'Server error', 404

@app.route('/get_keys_data/<db_name>/<tab_name>/<item>/<value>')
@auth.login_required
def get_keys_data(db_name, tab_name, item, value):
    uny_db_driver.history_agent(request, auth)

    db = uny_db_driver.uny_litebase(db_name)
    data = db.get_all_tab_data_by_keys(tab_name, item, value)
    if data != None:
        return data, 200
    return 'Server error', 404

@app.route('/insert_data/<db_name>/<tab_name>', methods=['POST'])
@auth.login_required
def insert_data(db_name, tab_name):
    hist = uny_db_driver.history_agent(request, auth)
    hist.write_oligomap_data()
    #r = requests.post('http://127.0.0.1:8881/insert_data/test_1.db/main_tab',
    # json=json.dumps(['test_name','test_content']))

    db = uny_db_driver.uny_litebase(db_name)
    data = json.loads(request.json)
    cursor, status = db.insert_data(tab_name, data)
    if status == 'ok':
        return status, 200
    return 'Server error', 404

@app.route('/update_data/<db_name>/<tab_name>/<record_id>', methods=['PUT'])
@auth.login_required
def update_data(db_name, tab_name, record_id):
    hist = uny_db_driver.history_agent(request, auth)
    hist.write_oligomap_data()
    #r = requests.put('http://127.0.0.1:8881/update_data/test_1.db/main_tab/10',
    # json=json.dumps({'name_list':['name', 'text'],'value_list':['test_name','CONTENT111']}))

    db = uny_db_driver.uny_litebase(db_name)
    data = json.loads(request.json)
    cursor, status = db.update_data(tab_name, record_id, data['name_list'], data['value_list'])
    if status == 'ok':
        return status, 200
    #app.logger.info(status)
    return 'Server error', 404

@app.route('/delete_data/<db_name>/<tab_name>/<record_id>', methods=['DELETE'])
@auth.login_required
def delete_data(db_name, tab_name, record_id):
    uny_db_driver.history_agent(request, auth)
    #r = requests.put('http://127.0.0.1:8881/update_data/test_1.db/main_tab/10',
    # json=json.dumps({'name_list':['name', 'text'],'value_list':['test_name','CONTENT111']}))

    db = uny_db_driver.uny_litebase(db_name)
    status = db.delete_row(tab_name, record_id)
    if status == 'ok':
        return status, 200
    return 'Server error', 404

@app.route('/insert_file_data/<db_name>/<tab_name>', methods=['POST'])
@auth.login_required
def insert_file_data(db_name, tab_name):
    uny_db_driver.history_agent(request, auth)
    #r = requests.post('http://127.0.0.1:8881/insert_file_data/map_analytics_1.db/lcms_tab',
    # json=json.dumps({'filename': 'test_file.txt', 'content': 'hWBCQUWEBC', 'to_base': [1, 2, 'A1', 'test_file.txt']}))

    db = uny_db_driver.uny_litebase(db_name)
    data = json.loads(request.json)

    if data['content'] != '':
        cursor, status = db.insert_data(tab_name, data['to_base'])
        with open(f"lcms_files/{data['filename']}", 'w') as f:
            f.write(data['content'])
    else:
        status = 'None'

    if status == 'ok':
        return status, 200
    return 'Server error', 404

@app.route('/get_file_data/<db_name>/<tab_name>/<id>/<order_id>/<pos>', methods=['GET'])
@auth.login_required
def get_file_data(db_name, tab_name, id, order_id, pos):
    uny_db_driver.history_agent(request, auth)
    #r = requests.post('http://127.0.0.1:8881/insert_file_data/map_analytics_1.db/lcms_tab',
    # json=json.dumps({'filename': 'test_file.txt', 'content': 'hWBCQUWEBC', 'to_base': [1, 2, 'A1', 'test_file.txt']}))

    db = uny_db_driver.uny_litebase(db_name)
    results = db.get_all_tab_data_by_keys(tab_name, 'filename',f'{id}_{order_id}_{pos}.mzdata.xml')

    if len(results) > 0:
        filename_ = results[-1][4]
    else:
        filename_ = ''

    #data = json.loads(request.json)
    #cursor, status = db.insert_data(tab_name, data['to_base'])

    if filename_ != '':
        with open(f"lcms_files/{filename_}", 'r') as f:
            data = f.read()
    else:
        data = None

    if data is not None:
        return {'file_content': data, 'tag_tab': results[-1][5]}, 200
    return 'Server error', 404

@app.route('/update_file_data/<db_name>/<tab_name>/<record_id>', methods=['PUT'])
@auth.login_required
def update_file_data(db_name, tab_name, record_id):
    uny_db_driver.history_agent(request, auth)
    #r = requests.put('http://127.0.0.1:8881/update_data/test_1.db/main_tab/10',
    # json=json.dumps({'name_list':['name', 'text'],'value_list':['test_name','CONTENT111']}))

    db = uny_db_driver.uny_litebase(db_name)
    data = json.loads(request.json)
    cursor, status = db.update_data(tab_name, record_id, data['name_list'], data['value_list'])
    if status == 'ok':
        return status, 200
    #app.logger.info(status)
    return 'Server error', 404

@app.route('/get_lcms_json_data/<file_name>', methods=['GET'])
@auth.login_required
def get_json_file_data(file_name):
    uny_db_driver.history_agent(request, auth)
    try:
        with open(f'lcms_files/{file_name}', 'r') as f:
            data = json.load(f)
        return data, 200
    except:
        return 'No file', 200

@app.route('/post_lcms_json_data/<file_name>', methods=['POST'])
@auth.login_required
def save_json_file_data(file_name):
    uny_db_driver.history_agent(request, auth)
    try:
        with open(f'lcms_files/{file_name}', 'w') as f:
            f.write(request.json)
        return f'succesfully saved {file_name}', 200
    except:
        return f'file not saved {file_name}', 404


@app.route('/get_orders_by_status/<db_name>/<status>', methods=['GET'])
@auth.login_required
def special_get_orders_by_status(db_name, status):
    uny_db_driver.history_agent(request, auth)
    db = uny_db_driver.uny_litebase(db_name)
    data = db.get_all_tab_data_by_keys('orders_tab', 'status', status)

    out = []
    if len(data) > 0:
        for id, client_id, order_id, input_date, output_date, status, name, sequence, \
                end5, end3, amount, purification, lenght in data:

            invoce_data = db.get_all_tab_data_by_keys('invoice_tab', 'id', order_id)

            d = {}
            d['#'] = id
            d['status'] = status
            d['input date'] = input_date
            d['output date'] = output_date
            d['client id'] = invoce_data[0][2]
            d['order id'] = invoce_data[0][1]
            d["5'-end"] = str(end5)
            d["Sequence"] = str(sequence)
            d["3'-end"] = str(end3)
            d['Amount, oe'] = str(amount)
            d['Purification'] = str(purification)
            d['Lenght'] = str(lenght)
            d['Name'] = str(name)
            out.append(d)

        return out, 200
    else:

        d = {}
        d['#'] = 0
        d['status'] = ''
        d['input date'] = ''
        d['output date'] = ''
        d['client id'] = ''
        d['order id'] = ''
        d["5'-end"] = 'none'
        d["Sequence"] = ''
        d["3'-end"] = 'none'
        d['Amount, oe'] = ''
        d['Purification'] = ''
        d['Lenght'] = ''
        d['Name'] = ''
        out.append(d)

        return out, 200


def get_counts(order_list):
        d = {}
        d['in queue'] = 0
        d['synthesis'] = 0
        d['purification'] = 0
        d['formulation'] = 0
        d['finished'] = 0
        d['arhive'] = 0
        d['total'] = 0

        for row in order_list:
            d['out date'] = row[4]
            d[row[5]] += 1
            d['total'] += 1
        return d

def is_all_finished(order_list):
        ctrl = True
        for row in order_list:
            if row[5] not in ['finished', 'arhive']:
                ctrl = False
                break
        return ctrl

@app.route('/get_all_invoces/<db_name>', methods=['GET'])
@auth.login_required
def special_get_all_invoces(db_name):
    uny_db_driver.history_agent(request, auth)
    try:
        db = uny_db_driver.uny_litebase(db_name)
        data = db.get_all_tab_data('invoice_tab')

        out = []
        for row in data:
            d = {}
            orders_list = db.get_all_tab_data_by_keys('orders_tab', 'order_id', row[0])
            counts = get_counts(orders_list)
            if len(orders_list) > 0:
                if is_all_finished(orders_list):
                    d['#'] = row[0]
                    d['invoce'] = row[1]
                    d['client'] = row[2]
                    d['input date'] = orders_list[0][3]
                    d['status'] = 'complited'
                else:
                    d['#'] = row[0]
                    d['invoce'] = row[1]
                    d['client'] = row[2]
                    d['input date'] = orders_list[0][3]
                    d['status'] = 'in progress'

                d['out date'] = counts['out date']
                d['number'] = counts['total']
                d['in queue%'] = f"{counts['in queue']}" #round(counts['in queue'] * 100 / counts['total'])
                d['synth%'] = f"{counts['synthesis']}" #round(counts['synthesis'] * 100 / counts['total'])
                d['purif%'] = f"{counts['purification']}" #round(counts['purification'] * 100 / counts['total'])
                d['formul%'] = f"{counts['formulation']}" #round(counts['formulation'] * 100 / counts['total'])
                d['fin%'] = f"{counts['finished']} / {counts['total']}" #round(counts['finished'] * 100 / counts['total'])
                d['archived%'] = round(counts['arhive'] * 100 / counts['total'])

                out.append(d)
        return out, 200
    except:
        return [], 404

@app.route('/get_all_invoces_by_orders/<db_name>', methods=['GET'])
@auth.login_required
def special_get_all_invoces_by_order(db_name):
    uny_db_driver.history_agent(request, auth)
    order_list = json.loads(request.json)
    db = uny_db_driver.uny_litebase(db_name)
    out_list = []
    for order_id in order_list:
        out_data = db.get_all_tab_data_by_keys('orders_tab', 'id', order_id)
        client_data = db.get_all_tab_data_by_keys('invoice_tab', 'id', out_data[0][1])
        out_list.append({'client': client_data[0][2], 'invoce': client_data[0][1]})
    return out_list, 200


@app.route('/get_remaining_stock/<db_name>/<unicode>', methods=['GET'])
@auth.login_required
def special_get_remaining_stock(db_name, unicode):
    #uny_db_driver.history_agent(request, auth)
    connection = sqlite3.connect(db_name)
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

    return {'exist': input_count - output_count}, 200

if __name__ == '__main__':

    #job = interval_jobs.job_class(app)
    #job.tg_bot_add_job()

    #job.add_job_1()
    #job.add_oligomap_status_monitor_job()

    #os.system('python tg_bot.py')

    app.run(host='0.0.0.0', port=8012, debug=True)