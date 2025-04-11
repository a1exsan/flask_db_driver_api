import pandas as pd
import uny_db_driver
from datetime import datetime

class db_tabs():
    def __init__(self):
        self.orders_db = 'scheduler_oligolab_2.db'
        self.maps_db = 'asm2000_map_1.db'
        self.hist_maps_db = 'oligomap_history_2.db'
        self.stock_db = 'stock_oligolab_5'
        self.status_hist_db = 'oligo_status_history_1.db'

class ordrs_statistic():
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


def test1():
    st1 = ordrs_statistic()
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

if __name__ == '__main__':
    test2()