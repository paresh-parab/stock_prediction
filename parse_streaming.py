import pandas as pd
import os
import pathlib
import re
import pprint
import numpy as np
import datetime
import warnings

warnings.filterwarnings("ignore")

intm_data_folder = "intm_data"
training_data_folder = "training_data"

periods = ['09-10', '10-11', '11-12', '12-13', '13-14', '14-15', '15-16']


#   Catering to inconsistency of the time format
def convert_time(x):
    try:
        if x.count(":") > 1:
            x = ":".join(x.split(":", 2)[:2])
        return pd.to_datetime(x).strftime('%H:%M')
    except:
        print(x, ' caused Exception')

#   Filter out unnecessary info
def club_daily(monthdir, write_dir):
    splits = monthdir.split("/")
    year = splits[0]
    month = splits[1]

    target_dir = os.path.join(write_dir, intm_data_folder)
    if not os.path.exists(target_dir):
        os.mkdir(target_dir)

    for day_folder in pathlib.Path(monthdir).iterdir():
        if day_folder.is_dir() and day_folder.name.isnumeric():
            day = day_folder.name
            date = day + "-" + month + "-" + year
            for file in pathlib.Path(day_folder).iterdir():
                if file.name == 'streaming':
                    intm_data = pd.read_csv(file, delim_whitespace=True,
                                            names=['SYMB', 'TIME', 'PRICE', 'CHNG', 'PRCHNG', 'QTY', 'OPEN',
                                                   'HIGH', 'LOW', 'BID', 'ASK'])
                    intm_data.drop(['CHNG', 'PRCHNG', 'QTY', 'OPEN', 'HIGH', 'LOW', 'BID', 'ASK'], axis=1, inplace=True)

                    if int(year) <= 2006:
                        intm_data = intm_data[intm_data['TIME'].str.contains(":", na=False)]
                    else:
                        intm_data = intm_data[intm_data['TIME'].str.count(":") == 2]
                    intm_data['DATE'] = date
                    intm_data['TIME'] = intm_data['TIME'].apply(lambda x: convert_time(x))
        intm_data = intm_data.sort_values(['SYMB', 'TIME'])
        intm_data.to_csv(os.path.join(target_dir, date + ".csv"), mode='w+', encoding='utf-8', index=False)


def getRange(time):
    if time >= '09:00' and time < '10:00':
        return '09-10'
    elif time >= '10:00' and time < '11:00':
        return '10-11'
    elif time >= '11:00' and time < '12:00':
        return '11-12'
    elif time >= '12:00' and time < '13:00':
        return '12-13'
    elif time >= '13:00' and time < '14:00':
        return '13-14'
    elif time >= '14:00' and time < '15:00':
        return '14-15'
    elif time >= '15:00' and time <= '16:00':
        return '15-16'
    else:
        return 'OUT_OF_HOURS'


#   Club all trades in an hour's window, by taking mean of the prices
def club_hourly(write_dir):
    src_dir = os.path.join(write_dir, intm_data_folder)

    target_dir = os.path.join(write_dir, intm_data_folder)
    if not os.path.exists(target_dir):
        os.mkdir(target_dir)

    for file in pathlib.Path(src_dir).iterdir():
        intm_data = pd.read_csv(file, sep=",")
        intm_data['TIME'] = intm_data['TIME'].apply(lambda x: getRange(x))
        intm_data = intm_data[intm_data['TIME'] != 'OUT_OF_HOURS']
        intm_data = intm_data[intm_data["PRICE"].notna()]
        intm_data["PRICE"] = pd.to_numeric(intm_data["PRICE"])

        intm_data = intm_data.groupby(['SYMB', 'TIME', 'DATE']).agg(
            {'PRICE': 'mean'}).reset_index()
        intm_data.to_csv(file, mode='w+', encoding='utf-8',
                         index=False)


#   From intermediate data, separte individual stock information
def club_symb(write_dir):

    src_dir = os.path.join(write_dir, intm_data_folder)

    target_dir = os.path.join(write_dir, training_data_folder)
    if not os.path.exists(target_dir):
        os.mkdir(target_dir)

    for file in pathlib.Path(src_dir).iterdir():
        mod_data = pd.read_csv(file, sep=",")
        for symb, grp in mod_data.groupby('SYMB'):
            symb = re.sub("[^A-Z]+", "_", symb)
            if symb == 'PRN':
                symb += '1'
            output_path = os.path.join(target_dir, symb + ".csv")
            grp.to_csv(output_path,
                       mode='a' if os.path.exists(output_path) else 'w+',
                       header=not os.path.exists(output_path),
                       encoding='utf-8',
                       index=False)


#   Identify business days from the two months and record them
def get_all_slots(month1dir, month2dir):

    record = []
    splits = month1dir.split("/")
    year = splits[0]
    month = splits[1]

    for day_folder in pathlib.Path(month1dir).iterdir():
        if day_folder.is_dir() and day_folder.name.isnumeric():
            day = day_folder.name
            date = day + "-" + month + "-" + year
            for p in periods:
                record.append([date, p])
    count = 0
    splits = month2dir.split("/")
    year = splits[0]
    month = splits[1]
    for day_folder in pathlib.Path(month2dir).iterdir():
        if day_folder.is_dir() and day_folder.name.isnumeric():
            day = day_folder.name
            date = day + "-" + month + "-" + year
            for p in periods:
                record.append([date, p])
                count += 1
    record = pd.DataFrame(record, columns=['DATE', 'TIME'])
    return [record, count]


#   When the slot is absent, copy price from previously available slot
def fill_slots(write_dir, record):

    src_dir = os.path.join(write_dir, training_data_folder)
    for file in pathlib.Path(src_dir).iterdir():
        symb_data = pd.read_csv(file, sep=",")
        symb_dict = symb_data.to_dict(orient='records')
        mod_symb_dict = []
        prev = []
        r = 0
        # 'SYMB','TIME','DATE', 'PRICE','CHNG','PRCHNG','QTY','OPEN','HIGH','LOW','BID','ASK'

        for i, row in enumerate(symb_dict):
            while r < len(record) and (row['TIME'] != record.iloc[r]['TIME'] or row['DATE'] != record.iloc[r]['DATE']):
                if i == 0:
                    prev = symb_dict[i]
                intm = dict(prev)

                intm['TIME'] = record.iloc[r]['TIME']
                intm['DATE'] = record.iloc[r]['DATE']

                mod_symb_dict.append(intm)
                prev = intm
                r += 1

            mod_symb_dict.append(row)
            prev = row
            r += 1

        while r < len(record):
            intm = dict(prev)

            intm['TIME'] = record.iloc[r]['TIME']
            intm['DATE'] = record.iloc[r]['DATE']
            mod_symb_dict.append(intm)
            r += 1

        mod_symb_df = pd.DataFrame(mod_symb_dict)
        mod_symb_df.to_csv(file, mode='w', encoding='utf-8', index=False)


def find_y(write_dir):
    def find_price_aft_5(date, time, df):
        pindex = -1;
        for i, p in enumerate(periods):
            if p == time:
                pindex = i
                break

        ds = str(date).split("-")

        date_obj = datetime.datetime(int(ds[2]), int(ds[1]), int(ds[0]))
        date_obj += datetime.timedelta(days=(1 if (pindex + 3) >= 7 else 0))
        date_obj = date_obj.replace(year=date_obj.year + 5)

        new_date = str(date_obj.day) + "-" + str(date_obj.month) + "-" + str(date_obj.year)
        new_time = periods[(pindex + 3) % 7]

        res = df.loc[(df['DATE'] == new_date) & (df['TIME'] == new_time)]

        if res.empty:
            return np.NaN
        else:
            return res['PRICE'].values[0]

    src_dir = os.path.join(write_dir, training_data_folder)

    for file in pathlib.Path(src_dir).iterdir():
        df = pd.read_csv(file, sep=",")
        df['Y'] = df.apply(lambda row: find_price_aft_5(row['DATE'], row['TIME'], df), axis=1)
        df.to_csv(file, mode='w', encoding='utf-8', index=False)

#    First parse the daily files into one location
#    Then, club the hourly data
#    Then, club by symbol and create separate files for stocks
#    Then, fill in blank slots for consistency in lstm input
def parse_streaming(year, month1, month2, write_dir):

    # club_daily(month1, write_dir)
    # club_hourly(write_dir)
    # club_symb(write_dir)
    slots, pred_count = get_all_slots(month1, month2)
    fill_slots(write_dir, slots)
    return pred_count

    # find_y(write_dir)
