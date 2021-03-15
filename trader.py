import math
import pathlib
import os
import pandas as pd
import numpy as np
from build_lstm import get_top_stocks

training_data_folder = "training_data"

def format_date(date):
    if date.count("/") > 0:
        parts = date.split("/")
    else:
        parts = date.split("-")

    return parts[2] + "-" + parts[1] + "-" + parts[0]

def get_trade_time(time):
    mapping = {'09-10':'09:00',
               '10-11':'10:00',
               '11-12':'11:00',
               '12-13':'12:00',
               '13-14':'13:00',
               '14-15':'14:00',
               '15-16':'15:00'}
    return mapping[time]

def compute_top_trades(top, src_dir):
    trades = pd.DataFrame()
    for file in pathlib.Path(src_dir).iterdir():
        symb = file.stem
        if not (symb in top):
            continue
        df = pd.read_csv(file)
        df = df[df['LSTM_Y'].notna()]
        record = {'BUY_ORDER': '',
                  'BUY_DATE': '',
                  'BUY_TIME': '',
                  'BUY_PRICE': 0,
                  'SELL_ORDER': '',
                  'SELL_DATE': '',
                  'PROFIT': 0}
        lowest = float('inf')
        lowestDate = ''
        lowestTime = ''
        maxprofit = 0

        for i, row in df.iterrows():
            if row['LSTM_Y'] < lowest:
                lowest = row['LSTM_Y']
                lowestDate = row['DATE']
                lowestTime = row['TIME']
            if row['LSTM_Y'] - lowest > maxprofit:
                maxprofit = row['LSTM_Y'] - lowest
                record['PROFIT'] = int(maxprofit)
                record['BUY_ORDER'] = format_date(lowestDate) + ' ' + get_trade_time(lowestTime) + ' buy ' + symb
                record['BUY_DATE'] = format_date(lowestDate)
                record['BUY_TIME'] = get_trade_time(lowestTime)
                record['BUY_PRICE'] = lowest
                record['SELL_ORDER'] = format_date(row['DATE']) + ' ' + get_trade_time(row['TIME']) + ' sell ' + symb
                record['SELL_DATE'] = format_date(row['DATE'])
                record['SELL_TIME'] = get_trade_time(row['TIME'])

        trades = trades.append(record, ignore_index=True)

    trades = trades.sort_values(by=['PROFIT'], ascending=False)
    trades = trades[:9]
    return trades

def format_trades(trades):
    for i, row in trades.iterrows():
        nshares = int(10000 / row['BUY_PRICE'])

        order = row['BUY_ORDER']
        order = order.split(" ")
        order.insert(-1, str(nshares))
        order.insert(-1, "shares")
        order.insert(-1, "of")
        trades.at[i, 'BUY_ORDER'] = " ".join(order)

        order = row['SELL_ORDER']
        order = order.split(" ")
        order.insert(-1, str(nshares))
        order.insert(-1, "shares")
        order.insert(-1, "of")
        trades.at[i, 'SELL_ORDER'] = " ".join(order)

    return trades

def sort_trades(trades):
    orders = pd.DataFrame()
    for i, row in trades.iterrows():
        orders = orders.append({'ORDER': row['BUY_ORDER'],
                                'DATE': row['BUY_DATE'],
                                'TIME': row['BUY_TIME'],
                                }, ignore_index=True)
        orders = orders.append({'ORDER': row['SELL_ORDER'],
                                'DATE': row['SELL_DATE'],
                                'TIME': row['SELL_TIME'],
                                }, ignore_index=True)

    orders = orders.sort_values(by=['DATE', 'TIME'], ascending=True)

    return orders['ORDER']

def print_top_trades(year, month1, month2, write_dir):
    top = get_top_stocks(write_dir)
    src_dir = os.path.join(write_dir, training_data_folder)
    trades = compute_top_trades(top, src_dir)
    trades = format_trades(trades)
    orders = sort_trades(trades)
    print(orders)






