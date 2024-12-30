from fileinput import filename

from flask import Flask, request, Response
from flask_cors import CORS
import uny_db_driver
import json
import os.path
import pickle
import logging
import base64

app = Flask(__name__)
CORS(app)

#logging.basicConfig(level=logging.DEBUG)

@app.route('/')
def home():
    return 'UNY_DB_APP'

@app.route('/get_info/<db_name>')
def get_db_info(db_name):
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
def get_db_tab_info(db_name, tab_name):
    db = uny_db_driver.uny_litebase(db_name)
    cols = db.get_table_col_names(tab_name)
    if cols != None:
        return cols, 200
    return 'Server error', 404

@app.route('/get_all_tab_data/<db_name>/<tab_name>')
def get_all_tab_data(db_name, tab_name):
    db = uny_db_driver.uny_litebase(db_name)
    data = db.get_all_tab_data(tab_name)
    if data != None:
        return data, 200
    return 'Server error', 404

@app.route('/get_keys_data/<db_name>/<tab_name>/<item>/<value>')
def get_keys_data(db_name, tab_name, item, value):
    db = uny_db_driver.uny_litebase(db_name)
    data = db.get_all_tab_data_by_keys(tab_name, item, value)
    if data != None:
        return data, 200
    return 'Server error', 404

@app.route('/insert_data/<db_name>/<tab_name>', methods=['POST'])
def insert_data(db_name, tab_name):

    #r = requests.post('http://127.0.0.1:8881/insert_data/test_1.db/main_tab',
    # json=json.dumps(['test_name','test_content']))

    db = uny_db_driver.uny_litebase(db_name)
    data = json.loads(request.json)
    cursor, status = db.insert_data(tab_name, data)
    if status == 'ok':
        return status, 200
    return 'Server error', 404

@app.route('/update_data/<db_name>/<tab_name>/<record_id>', methods=['PUT'])
def update_data(db_name, tab_name, record_id):

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
def delete_data(db_name, tab_name, record_id):

    #r = requests.put('http://127.0.0.1:8881/update_data/test_1.db/main_tab/10',
    # json=json.dumps({'name_list':['name', 'text'],'value_list':['test_name','CONTENT111']}))

    db = uny_db_driver.uny_litebase(db_name)
    status = db.delete_row(tab_name, record_id)
    if status == 'ok':
        return status, 200
    return 'Server error', 404

@app.route('/insert_file_data/<db_name>/<tab_name>', methods=['POST'])
def insert_file_data(db_name, tab_name):

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
def get_file_data(db_name, tab_name, id, order_id, pos):

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
def update_file_data(db_name, tab_name, record_id):

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
def get_json_file_data(file_name):
    if os.path.exists(file_name):
        with open(f'lcms_files/{file_name}', 'r') as f:
            data = f.read()
        return data
    else:
        return ''

@app.route('/post_lcms_json_data/<file_name>', methods=['POST'])
def save_json_file_data(file_name):
    try:
        with open(f'lcms_files/{file_name}', 'w') as f:
            f.write(request.json)
        return f'succesfully saved {file_name}', 200
    except:
        return f'file not saved {file_name}', 404



if __name__ == '__main__':
#    app.run(port=8881, debug=True)
    app.run(host='0.0.0.0', port=8012, debug=True)