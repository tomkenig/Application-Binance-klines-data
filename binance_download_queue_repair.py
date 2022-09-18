from db_works import db_connect
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
def repair_queue_status():
    print("queue repair process start")
    cursor, cnxn = db_connect()
    cursor.execute("UPDATE " + db_binance_schema_name + "." + db_binance_settings_table_name + " SET download_setting_status_id = %s where download_setting_status_id = %s", (0, 1))
    cnxn.commit()
    return print("queue repair process done")

if __name__ == "__main__":
    db_binance_schema_name, db_binance_klines_table_name, db_binance_settings_table_name = get_settings_json()
    repair_queue_status()