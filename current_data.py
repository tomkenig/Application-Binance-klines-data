# todo: DONE: work with monthly interwal - 1mo < works with files or 1M < works with API.
# todo: check commits and rollbacks
# todo: montly 1m and 1w intervals doesn't insert last and next timestamps
# todo: change columns names: daily(monthly)_update_from_files > update_from_daily(monthly)_files
# todo: review tables names
# todo: pobieraby ma byc w pierwszej kolejnosci ten, który już wg. czasow powinien byc pobierany i jednoczesnie jego dane były pobierane najdawniej
# libs
import requests
import datetime
import json
from db_works import db_connect


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
    if interval_param_ == "current":
        cursor.execute(
            "SELECT download_settings_id, market, tick_interval, data_granulation, stock_type, stock_exchange, "
            "current_range_to_overwrite, download_api_interval_sec, daily_update_from_files, monthly_update_from_files, start_hist_download_ux_timestamp "
            "FROM " + db_binance_schema_name + "." + db_binance_settings_table_name + " WHERE current_update_from_api = 1 and "
                                                                     # "download_setting_status_id = 0 and "
                                                                     # "daily_hist_complete = 1 AND "
                                                                     # "monthly_hist_complete = 1 AND "
                                                                      "coalesce(last_download_ux_timestamp, 0) <= "
            + str(int(datetime.datetime.utcnow().timestamp())) + " order by last_download_ux_timestamp asc limit 1")
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


# get data from binance API
def get_binance_data_current():
    url = "https://api.binance.com/api/v3/" + data_granulation + "?symbol=" + market + "&interval=" + (tick_interval).replace('1mo', '1M')
    data = requests.get(url).json()
    print(data[-range_to_download:])
    return data[-range_to_download:]


# insert and overwrite fresh data
def insert_overwrite_data_current():
    short_data = get_binance_data_current()
    try:
        cursor.execute("DELETE FROM " + db_binance_schema_name+"."+db_binance_klines_table_name +" where open_time >= %s and download_settings_id = %s", (short_data[0][0], download_settings_id))
        print("delete done")
        for i in short_data:
            cursor.execute("INSERT INTO " + db_binance_schema_name+"."+db_binance_klines_table_name +"(open_time, open, high, low, close, volume, close_time,"
                                                                              " quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, "
                                                                              "taker_buy_quote_asset_volume, `ignore`,"
                                                                              "download_settings_id, insert_ux_timestamp) "
                                                                              "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                           (i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10], i[11],
                            download_settings_id, str(int(datetime.datetime.utcnow().timestamp()))))
        print("insert done")
        cnxn.commit()
    except:
        print("error_1")
        cnxn.rollback()
        exit()

def update_settings_queue_current():
    try:
        cursor.execute("UPDATE " + db_binance_schema_name + "." + db_binance_settings_table_name + " SET last_download_ux_timestamp = %s, next_download_ux_timestamp = %s,"
                                                                                   " download_setting_status_id = %s where download_settings_id = %s",
                       (str(int(datetime.datetime.utcnow().timestamp())),
                        str(int(str(int(datetime.datetime.utcnow().timestamp()))) + download_api_interval_sec),
                        0, download_settings_id))
        print("update done")
    except:
        print("error_2")
        cnxn.rollback()
        exit()


if __name__ == "__main__":
    db_binance_schema_name, db_binance_klines_table_name, db_binance_settings_table_name = get_settings_json()
    cursor, cnxn = db_connect()
    download_settings_id, market, tick_interval, data_granulation, stock_type, stock_exchange, range_to_download, download_api_interval_sec, daily_update_from_files, monthly_update_from_files, start_hist_download_ux_timestamp = get_settings("current")

    insert_overwrite_data_current()
    update_settings_queue_current()
    # close connection
    cnxn.commit()
    cursor.close()
    cnxn.close()
