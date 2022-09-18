# todo: DONE get_filenames_to_download_monthly and get_filenames_to_download_daily can be one function
# todo: DONE daily can start only when monthly is completed
# todo: fix setting status id problem < need set to default on error
# todo: check commits and rollbacks
# todo: parallel start historical_data can have a problem - files deletion in one instance, when other instace works with files. Crash

# libs
import requests
from datetime import datetime, timedelta
import datetime
import zipfile
import csv
# from db_works import db_connect, db_tables
# import stock_dwh_functions
import os
import json
from datetime import datetime, timedelta
import datetime
import zipfile
import csv
from db_works import db_connect
# import stock_dwh_functions

# todo: move it to config file
# path to downloaded files
TMP_PATH = "tmp/"

# create temporary directory for downloaded files
def create_temp_dir():
    try:
        os.mkdir(TMP_PATH)
    except OSError as error:
        print(error)

# delete old files if exist in temp directory
def delete_old_files():
    for f in os.listdir(TMP_PATH[0:len(TMP_PATH)-1]):
        os.remove(os.path.join(TMP_PATH[0:len(TMP_PATH)-1], f))
    print("old files deleted")

# get settings from config json
def get_settings_json():
    with open("global_config.json") as json_conf:
        app_conf = (json.load(json_conf))
    print("conf file opened")
    db_binance_schema_name = app_conf["db_binance_schema_name"]
    db_binance_klines_table_name = app_conf["db_binance_klines_table_name"]
    db_binance_settings_table_name = app_conf["db_binance_settings_table_name"]
    return db_binance_schema_name, db_binance_klines_table_name, db_binance_settings_table_name

# get settings
def get_settings(interval_param_):
    cursor, cnxn = db_connect()

    # interval parameter: current - API data; daily_hist - data from daily files; monthly_hist - data from monthly files
    if interval_param_ == "daily_hist":
        cursor.execute(
            "SELECT download_settings_id, market, tick_interval, data_granulation, stock_type, stock_exchange, "
            "current_range_to_overwrite, download_api_interval_sec, daily_update_from_files, monthly_update_from_files, start_hist_download_ux_timestamp "
            "FROM " + db_binance_schema_name + "." + db_binance_settings_table_name + " WHERE daily_update_from_files = 1 and "
                                                                      # "download_setting_status_id = 0 and "
                                                                      "daily_hist_complete = 0 AND "
                                                                      "monthly_hist_complete = 1 AND "
                                                                      "coalesce(start_hist_download_ux_timestamp, 0) <= "
            + str(
                int(datetime.datetime.utcnow().timestamp())) + " order by download_settings_id desc, start_hist_download_ux_timestamp asc limit 1")

    elif interval_param_ == "monthly_hist":
        cursor.execute(
            "SELECT download_settings_id, market, tick_interval, data_granulation, stock_type, stock_exchange, "
            "current_range_to_overwrite, download_api_interval_sec, daily_update_from_files, monthly_update_from_files, start_hist_download_ux_timestamp "
            "FROM " + db_binance_schema_name + "." + db_binance_settings_table_name + " WHERE monthly_update_from_files = 1 and "
                                                                      #"download_setting_status_id = 0 and "
                                                                      "monthly_hist_complete = 0 AND "
                                                                      "coalesce(start_hist_download_ux_timestamp, 0) <= "
            + str(
                int(datetime.datetime.utcnow().timestamp())) + " order by download_settings_id desc, start_hist_download_ux_timestamp asc limit 1")

    else:
        exit()

    download_setting = cursor.fetchall()
    if len(download_setting) > 0:
         download_settings_id = download_setting[0][0]
         market = download_setting[0][1]
         tick_interval = download_setting[0][2]
         data_granulation = download_setting[0][3]
         stock_type = download_setting[0][4]
         stock_exchange = download_setting[0][5]
         range_to_download = download_setting[0][6]
         download_api_interval_sec = download_setting[0][7]
         daily_update_from_files = download_setting[0][8]
         monthly_update_from_files = download_setting[0][9]
         start_hist_download_ux_timestamp = download_setting[0][10]

    else:
         print("no data to download")
         exit()

    # block current setting changing its status
    cursor.execute("UPDATE " + db_binance_schema_name + "." + db_binance_settings_table_name + " SET download_setting_status_id = %s where download_settings_id = %s", (1, download_settings_id))
    cnxn.commit()
    print("settings blocked")
    print(download_settings_id, market, tick_interval, data_granulation, stock_type, stock_exchange, range_to_download, download_api_interval_sec, daily_update_from_files, monthly_update_from_files, start_hist_download_ux_timestamp)
    return download_settings_id, market, tick_interval, data_granulation, stock_type, stock_exchange, range_to_download, download_api_interval_sec, daily_update_from_files, monthly_update_from_files, start_hist_download_ux_timestamp


# todo: it can be move to global_config_json
def get_filenames_to_download(interval_param_):
    if interval_param_ == "monthly_hist":
        date_length = 7
        hist_days = int(datetime.datetime.utcnow().timestamp()) / 86400 - start_hist_download_ux_timestamp / 86400 # for monthly_files
    elif interval_param_ == "daily_hist":
        date_length = 10
        hist_days = 35 # for all daily files
    elif interval_param_ == "monthly_update":
        date_length = 7
        hist_days = 1
    elif interval_param_ == "daily_update":
        date_length = 10
        hist_days = 3
    else:
        date_length = 10

    start_date = datetime.datetime.strptime(str(datetime.datetime.utcnow() - timedelta(days=hist_days))[0:10], "%Y-%m-%d")
    end_date = datetime.datetime.strptime(str(datetime.datetime.utcnow() - timedelta(days=1))[0:10], "%Y-%m-%d") # 2021/10/05 changed 0 to 1
    delta = end_date - start_date
    l = []
    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        l.insert(i, "" + market +"-"+ tick_interval +"-"+ str(day)[0:date_length] +"")
        # print(day)
    return sorted(list(set(l))) # works like distinct

def get_files_monthly():
    # todo: file import
    r = requests.get(base_url)
    open(file_path + ".zip", "wb").write(r.content)

    # todo: zip unpack
    with zipfile.ZipFile(file_path + ".zip", "r") as zip_ref:
        zip_ref.extractall(TMP_PATH[0:len(TMP_PATH)-1])

    # get data from csv file
    with open(file_path + ".csv") as file_handle:
        csv_reader = csv.reader(file_handle)
        csv_data = [row for row in csv_reader]

    open_time_min = min(csv_data)[0]
    open_time_max = max(csv_data)[0]

    # delete old data to overwrite
    cursor.execute("DELETE FROM " + db_binance_schema_name + "." + db_binance_klines_table_name + " where open_time >= %s and open_time <= %s and download_settings_id = %s", (open_time_min, open_time_max, download_settings_id))
    print("delete done")

    # insert data
    for i in csv_data:
        cursor.execute("INSERT INTO " + db_binance_schema_name+"."+db_binance_klines_table_name +"(open_time, open, high, low, close, volume, close_time, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, `ignore`, download_settings_id, insert_ux_timestamp) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10], i[11], download_settings_id, str(int(datetime.datetime.utcnow().timestamp()))))
        # print(i)
    print("insert done")

    # update settings
    try:
        cursor.execute("UPDATE " + db_binance_schema_name + "." + db_binance_settings_table_name + " SET start_hist_download_ux_timestamp = %s, download_setting_status_id = %s where download_settings_id = %s", (str(int(str(open_time_max))/1000),  0, download_settings_id))
        print("update done")
    except Exception as e:
        print(e)
        cnxn.rollback()
        exit()

if __name__ == "__main__":
    create_temp_dir()
    delete_old_files()
    db_binance_schema_name, db_binance_klines_table_name, db_binance_settings_table_name = get_settings_json()
    cursor, cnxn = db_connect()

    # monthly files works
    download_settings_id, market, tick_interval, data_granulation, stock_type, stock_exchange, range_to_download, download_api_interval_sec, daily_update_from_files, monthly_update_from_files, start_hist_download_ux_timestamp = get_settings("monthly_hist")
    for k in get_filenames_to_download("monthly_hist"):
        try:
             file_path = TMP_PATH + k
             base_url = "https://data.binance.vision/data/"+stock_type+"/monthly/"+data_granulation+"/"+market+"/"+tick_interval+"/"+k+".zip"
             get_files_monthly()
             cnxn.commit()
        except Exception as e:
            print(e)
    # monthly_hist_complete = 1
    try:
        cursor.execute("UPDATE " + db_binance_schema_name + "." + db_binance_settings_table_name + " SET monthly_hist_complete = %s where download_settings_id = %s", (1, download_settings_id))
        print("update monthly hist complete done")
        cnxn.commit()
    except Exception as e:
        print(e)

    # daily
    delete_old_files()
    download_settings_id, market, tick_interval, data_granulation, stock_type, stock_exchange, range_to_download, download_api_interval_sec, daily_update_from_files, monthly_update_from_files, start_hist_download_ux_timestamp = get_settings("daily_hist")
    for k in get_filenames_to_download("daily_hist"):
        try:
             file_path = TMP_PATH + k
             base_url = "https://data.binance.vision/data/"+stock_type+"/daily/"+data_granulation+"/"+market+"/"+tick_interval+"/"+k+".zip"
             get_files_monthly()
             cnxn.commit()
        except Exception as e:
            print(e)
    try:
        cursor.execute("UPDATE " + db_binance_schema_name + "." + db_binance_settings_table_name + " SET daily_hist_complete = %s where download_settings_id = %s", (1, download_settings_id))
        print("update daily hist complete done")
        cnxn.commit()
    except Exception as e:
        print(e)

    cnxn.commit()
    cursor.close()
    cnxn.close()