# Overview

Application downloads and overwrite OHLC data from binance sources (both API and files) and load data to RDBMS DB.

Application can downloads any klines data in any time interval for all pairs listed on binance.

Application can downloads data for all market pairs at te same time.

Application overwrites data if same data exist. There won’t be any duplicates of data

# Version

0.01 current

#Files

global_config.json – general configuration

db_credentials.json – database credentials file

db_works.py – functions for connection with database ( curent MySQL )

current_data_api_update.py – downloads current data in short intervals using API

current_data_file_update.py – downloads only last updated historical data from files. Corrections for closed days

hist_data.py – downloads historical data using files

binance_download_queue.py – queue

binance_download_queue_repair.py – queue status repair

current_data_api_update.py and current_data_file_update.py wont work unless hist_data wont be completed. It can be changed in settings.

# Database objects

binance_download_settings – settings table

binance_klines_data – klines table for downloaded data

vw_binance_klines_anl – view to analytical use

# Quick Install and deployment

Create Table on your database from binance_klines_data.sql script

Use your credentials and settings in files listed below:

global_config.json – general configuration
db_credentials.json – database credentials file
To automate use CRON or Task manager