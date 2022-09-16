CREATE TABLE `binance_klines_data` (
  `open_time` bigint DEFAULT NULL,
  `open` double DEFAULT NULL,
  `high` double DEFAULT NULL,
  `low` double DEFAULT NULL,
  `close` double DEFAULT NULL,
  `volume` double DEFAULT NULL,
  `close_time` bigint DEFAULT NULL,
  `quote_asset_volume` double DEFAULT NULL,
  `number_of_trades` bigint DEFAULT NULL,
  `taker_buy_base_asset_volume` double DEFAULT NULL,
  `taker_buy_quote_asset_volume` double DEFAULT NULL,
  `ignore` int DEFAULT NULL,
  `download_settings_id` int NOT NULL,
  `insert_ux_timestamp` int DEFAULT NULL
) ;




SELECT * FROM m1174_stock_dwh.binance_klines_data dat
join m1174_stock_dwh.binance_download_settings st on dat.download_settings_id = st.download_settings_id
-- where st.tick_interval = '4h'
;



SELECT * FROM m1174_stock_dwh.binance_download_settings
;


/*
update m1174_stock_dwh.binance_download_settings
set download_setting_status_id = 0
;
*/


SELECT 
    tick_interval,
    download_settings_id,
    MAX(FROM_UNIXTIME(insert_ux_timestamp)) AS last_insert_dtime
FROM
    m1174_stock_dwh.binance_klines_data
GROUP BY tick_interval , download_settings_id
ORDER BY 3 DESC
;