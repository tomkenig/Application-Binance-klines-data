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
  `insert_ux_timestamp` int DEFAULT NULL,
  KEY `idx_binance_klines_data_download_settings_id` (`download_settings_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;





SELECT * FROM m1174_stock_dwh.binance_klines_data dat
join m1174_stock_dwh.binance_download_settings st on dat.download_settings_id = st.download_settings_id
where st.tick_interval = '1h'
;



SELECT * FROM m1174_stock_dwh.binance_download_settings
;




update m1174_stock_dwh.binance_download_settings
set start_hist_download_ux_timestamp = 1483225200
;


update m1174_stock_dwh.binance_download_settings
set daily_update_from_files = 0, monthly_update_from_files = 0
where tick_interval = '1m'
;



truncate table m1174_stock_dwh.binance_klines_data;


select * from
 m1174_stock_dwh.binance_klines_data

;



/*
update m1174_stock_dwh.binance_download_settings
set download_setting_status_id = 0
;
*/


SELECT 
    st.tick_interval,
    f.download_settings_id,
    MAX(FROM_UNIXTIME(f.insert_ux_timestamp)) AS last_insert_dtime,
    count(0) as rec_cnt
FROM
	m1174_stock_dwh.binance_download_settings st 
    left join m1174_stock_dwh.binance_klines_data f on f.download_settings_id = st.download_settings_id
GROUP BY st.tick_interval , f.download_settings_id
ORDER BY 3 DESC
;