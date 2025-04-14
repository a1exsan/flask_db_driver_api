import pandas as pd
import uny_db_driver
from datetime import datetime
from datetime import timedelta
import json

class db_tabs():
    def __init__(self):
        self.orders_db = 'scheduler_oligolab_2.db'
        self.maps_db = 'asm2000_map_1.db'
        self.hist_maps_db = 'oligomap_history_2.db'
        self.stock_db = 'stock_oligolab_5'
        self.status_hist_db = 'oligo_status_history_1.db'

class orders_statistic():
    def __init__(self):
        self.db = db_tabs()

    def get_total_oligos_tab(self):
        db = uny_db_driver.uny_litebase(self.db.orders_db)
        data = db.get_all_tab_data('orders_tab')
        out = []

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

        return pd.DataFrame(out)

    def get_total_status_stat(self, data, date_old, date, all_data=True):
        d = {}
        df = data.copy()
        if not all_data:
            df['i_date'] = pd.to_datetime(df['input date'], format="%m.%d.%Y")
            #df['o_date'] = pd.to_datetime(df['input date'], format="%m.%d.%Y")
            df = df[(df['i_date'] >= date_old)&(df['i_date'] <= date)]

        for status in ['in queue', 'synthesis', 'purification', 'formulation', 'finished']:
            df_1 = df[df['status'] == status]
            d[status] = df_1.shape[0]
        return d

    def get_counts(self, order_list):
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

    def is_all_finished(self, order_list):
        ctrl = True
        for row in order_list:
            if row[5] not in ['finished', 'arhive']:
                ctrl = False
                break
        return ctrl

    def get_all_invoces_tab(self):
        try:
            db = uny_db_driver.uny_litebase(self.db.orders_db)
            data = db.get_all_tab_data('invoice_tab')

            out = []
            for row in data:
                d = {}
                orders_list = db.get_all_tab_data_by_keys('orders_tab', 'order_id', row[0])
                counts = self.get_counts(orders_list)
                if len(orders_list) > 0:
                    if self.is_all_finished(orders_list):
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
                    d['in queue%'] = f"{counts['in queue']}"  # round(counts['in queue'] * 100 / counts['total'])
                    d['synth%'] = f"{counts['synthesis']}"  # round(counts['synthesis'] * 100 / counts['total'])
                    d['purif%'] = f"{counts['purification']}"  # round(counts['purification'] * 100 / counts['total'])
                    d['formul%'] = f"{counts['formulation']}"  # round(counts['formulation'] * 100 / counts['total'])
                    d[
                        'fin%'] = f"{counts['finished']} / {counts['total']}"  # round(counts['finished'] * 100 / counts['total'])
                    d['archived%'] = round(counts['arhive'] * 100 / counts['total'])
                    # commit
                    d['send'] = json.loads(row[3])['send']

                    out.append(d)
            return out
        except:
            return []

    def get_orders_in_progress(self):
        data = self.get_all_invoces_tab()
        out = []
        for row in data:
            if row['status'] == 'in progress' or not row['send']:
                out.append(row)
        return out

class status_history_stat():
    def __init__(self):
        self.db = db_tabs()

    def show_status_history(self):
        db = uny_db_driver.uny_litebase(self.db.status_hist_db)
        data = db.get_all_tab_data('main_tab')
        return data

    def get_last_update(self):
        db = uny_db_driver.uny_litebase(self.db.status_hist_db)
        data = db.get_last_record_data('main_tab')
        return data[0]

    def get_last_date_time(self):
        data = self.get_last_update()
        return data[1], data[2]

    def get_last_x_days_period(self, days=5):
        data = self.show_status_history()
        out = []
        for row in data:
            d = {}
            d['date'] = datetime.strptime(f"{row[1]} {row[2]}", "%d.%m.%Y %H:%M:%S")
            d['stat'] = row[3]
            out.append(d)
        df = pd.DataFrame(out)
        df.sort_values('date', ascending=True, inplace=True)
        df = df[df['date'] >= datetime.now() - timedelta(days=days)]
        df.reset_index(inplace=True)
        ret = {}
        if df.shape[0] > 0:
            first = json.loads(df.loc[0]['stat'])
            last = json.loads(df.loc[df.shape[0]-1]['stat'])
            ret['in queue'] = abs(last['in queue'] - first['in queue'])
            ret['synthesis'] = abs(last['synthesis'] - first['synthesis'])
            ret['purification'] = abs(last['purification'] - first['purification'])
            ret['formulation'] = abs(last['formulation'] - first['formulation'])
            ret['finished'] = abs(last['finished'] - first['finished'])
        else:
            ret['in queue'] = 0
            ret['synthesis'] = 0
            ret['purification'] = 0
            ret['formulation'] = 0
            ret['finished'] = 0
        return ret


def test1():
    st1 = orders_statistic()
    print(st1.get_total_status_stat(
                                    st1.get_total_oligos_tab(),
                                    datetime.strptime('01.08.2024', "%d.%m.%Y"),
                                    datetime.strptime('01.09.2024', "%d.%m.%Y"),
                                    all_data=False),
                                    )

def test2():
    st_hist = status_history_stat()
    print(st_hist.get_last_date_time())
    for i in st_hist.show_status_history():
        print(i)

    print(st_hist.get_last_x_days_period(days=30))

def compute_status_history_list():

    days_inc = 1

    init_date = datetime.strptime('01.05.2024', "%d.%m.%Y")
    final_date = datetime.strptime('11.04.2025', "%d.%m.%Y")
    date = init_date + timedelta(days=days_inc)

    db = uny_db_driver.uny_litebase('oligo_status_history_1.db')

    while date < final_date:
        st1 = orders_statistic()
        data = st1.get_total_status_stat(
        st1.get_total_oligos_tab(),
        init_date,
        date,
        all_data=False)

        print(date, data)

        db.insert_data('main_tab', [
                                                        date.date().strftime("%d.%m.%Y"),
                                                        date.time().strftime("%H:%M:%S"),
                                                        json.dumps(data)
                                                    ])

        date += timedelta(days=days_inc)



if __name__ == '__main__':
    #test2()
    compute_status_history_list()