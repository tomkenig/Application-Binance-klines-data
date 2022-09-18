from db_works import db_connect
import datetime
import json

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
def get_queue_settings(interval_param_):
    db_binance_schema_name, db_binance_klines_table_name, db_binance_settings_table_name = get_settings_json()
    cursor, cnxn = db_connect()

    # interval parameter: current - API data; daily_hist - data from daily files; monthly_hist - data from monthly files
    if interval_param_ == "api_update":
        cursor.execute(
            "SELECT download_settings_id, market, tick_interval, data_granulation, stock_type, stock_exchange, "
            "current_range_to_overwrite, download_api_interval_sec, daily_update_from_files, monthly_update_from_files, start_hist_download_ux_timestamp "
            "FROM " + db_binance_schema_name + "." + db_binance_settings_table_name + " WHERE current_update_from_api = 1 and "
                                                                      "download_setting_status_id = 0 and "
                                                                      "daily_hist_complete = 1 AND "
                                                                      "monthly_hist_complete = 1 AND "
                                                                      "coalesce(last_download_ux_timestamp, 0) <= "
            + str(int(datetime.datetime.utcnow().timestamp())) + " order by last_download_ux_timestamp asc limit 1")
    elif interval_param_ == "daily_hist":
        cursor.execute(
            "SELECT download_settings_id, market, tick_interval, data_granulation, stock_type, stock_exchange, "
            "current_range_to_overwrite, download_api_interval_sec, daily_update_from_files, monthly_update_from_files, start_hist_download_ux_timestamp "
            "FROM " + db_binance_schema_name + "." + db_binance_settings_table_name + " WHERE daily_update_from_files = 1 and "
                                                                      "download_setting_status_id = 0 and "
                                                                      "daily_hist_complete = 0 AND "
                                                                      "monthly_hist_complete = 1 AND "
                                                                      "coalesce(start_hist_download_ux_timestamp, 0) <= "
            + str(
                int(datetime.datetime.utcnow().timestamp())) + " order by last_download_ux_timestamp asc limit 1")

    elif interval_param_ == "monthly_hist":
        cursor.execute(
            "SELECT download_settings_id, market, tick_interval, data_granulation, stock_type, stock_exchange, "
            "current_range_to_overwrite, download_api_interval_sec, daily_update_from_files, monthly_update_from_files, start_hist_download_ux_timestamp "
            "FROM " + db_binance_schema_name + "." + db_binance_settings_table_name + " WHERE monthly_update_from_files = 1 and "
                                                                      "download_setting_status_id = 0 and "
                                                                      "monthly_hist_complete = 0 AND "
                                                                      "coalesce(start_hist_download_ux_timestamp, 0) <= "
            + str(
                int(datetime.datetime.utcnow().timestamp())) + " order by last_download_ux_timestamp asc limit 1")

    elif interval_param_ == "daily_file_update":
        cursor.execute(
            "SELECT download_settings_id, market, tick_interval, data_granulation, stock_type, stock_exchange, "
            "current_range_to_overwrite, download_api_interval_sec, daily_update_from_files, monthly_update_from_files, start_hist_download_ux_timestamp "
            "FROM " + db_binance_schema_name + "." + db_binance_settings_table_name + " WHERE daily_update_from_files = 1 and "
                                                                                      "download_setting_status_id = 0 and "
                                                                                      "daily_hist_complete = 1 AND "
                                                                                      "monthly_hist_complete = 1 AND "
                                                                                      "coalesce(start_hist_download_ux_timestamp, 0) <= "
            + str(
                int(datetime.datetime.utcnow().timestamp())) + " order by last_daily_update_files_ux_timestamp asc limit 1")

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
