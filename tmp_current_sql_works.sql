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



SELECT * FROM m1174_stock_dwh.binance_download_settings;


update m1174_stock_dwh.binance_download_settings
set daily_hist_complete=1
where download_settings_id >1;

-- reset do factory defaults
update m1174_stock_dwh.binance_download_settings
set start_hist_download_ux_timestamp = 1483225200, monthly_hist_complete = 0, daily_hist_complete=0
;
-- reset do factory defaults and clean data
truncate table m1174_stock_dwh.binance_klines_data;



update m1174_stock_dwh.binance_download_settings
set daily_update_from_files = 0, monthly_update_from_files = 0
where tick_interval = '1m'
;


update m1174_stock_dwh.binance_download_settings
set download_setting_status_id = 0
where download_settings_id > 1
;



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
    st.download_settings_id,
    MAX(FROM_UNIXTIME(f.insert_ux_timestamp)) AS last_insert_dtime,
    count(f.open_time) as rec_cnt
FROM
	m1174_stock_dwh.binance_download_settings st 
    left join m1174_stock_dwh.binance_klines_data f on f.download_settings_id = st.download_settings_id
GROUP BY st.tick_interval , f.download_settings_id
ORDER BY 3 DESC
;



selecT 
	year(open_datetime) as yr,
	month(open_datetime) as mnt,
	a.tick_interval,
    a.download_settings_id,
    sum(number_of_trades) as trades,
    avg(open) as open_avg,
    avg(close) as close_avg,    
	sum(open) as open_s,
    sum(close) as close_s,    
    MAX(FROM_UNIXTIME(a.insert_timestamp)) AS last_insert_dtime,
    count(a.open_time) as rec_cnt
from vw_binance_klines_anl a
group by 
	year(open_datetime),
	month(open_datetime),
	a.tick_interval,
    a.download_settings_id
    ;





create view vw_binance_klines_anl as
    SELECT 
    `a`.`open_time` AS `open_time`,
    `a`.`open` AS `open`,
    `a`.`high` AS `high`,
    `a`.`low` AS `low`,
    `a`.`close` AS `close`,
    `a`.`volume` AS `volume`,
    `a`.`close_time` AS `close_time`,
    `a`.`quote_asset_volume` AS `quote_asset_volume`,
    `a`.`number_of_trades` AS `number_of_trades`,
    `a`.`taker_buy_base_asset_volume` AS `taker_buy_base_asset_volume`,
    `a`.`taker_buy_quote_asset_volume` AS `taker_buy_quote_asset_volume`,
    `a`.`ignore` AS `ignore`,
    `b`.`market` AS `market`,
    `b`.`tick_interval` AS `tick_interval`,
    `b`.`data_granulation` AS `data_granulation`,
    `b`.`stock_type` AS `stock_type`,
    `b`.`stock_exchange` AS `stock_exchange`,
    `a`.`download_settings_id` AS `download_settings_id`,
    `a`.`insert_ux_timestamp` AS `insert_timestamp`,
    CONVERT_TZ(FROM_UNIXTIME((`a`.`open_time` / 1000)),
            'SYSTEM',
            'UTC') AS `open_datetime`,
    CONVERT_TZ(FROM_UNIXTIME((`a`.`close_time` / 1000)),
            'SYSTEM',
            'UTC') AS `close_datetime`
FROM
    `binance_klines_data` `a`
        JOIN
    binance_download_settings b ON a.download_settings_id = b.download_settings_id
ORDER BY `a`.`open_time`;

select * from fear_and_greed_index_data order by 4 desc;


select * from tactics_tests;

-- DROP TABLE `tactics_groups`;
CREATE TABLE `tactics_groups` (
  `tactic_group_id` bigint NOT NULL AUTO_INCREMENT,
  `tactic_group_name` varchar(255) DEFAULT NULL,
  `tactic_group_category` varchar(255) DEFAULT NULL,
  `tactic_group_status_id` varchar(255) DEFAULT '0',
  `tactic_group_priority` bigint DEFAULT NULL,
  `tactic_group_data` varchar(65535)  DEFAULT NULL,
  `insert_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`tactic_group_id`)
);

truncate table tactics_groups;
truncate table tactics_tests;
truncate table tactics_tests_results;
truncate table tactics_workers;

selecT * from tactics_groups;
select * from tactics_tests;
select * from tactics_tests_results;
selecT * from tactics_workers;


select * from tactics_tests_results where worker_id is not null;

select * from tactics_tests where tactic_id = 1970;

update tactics_workers set worker_name = 'xDESKTOP-7OAKQVJ' where worker_id = 1;

delete from tactics_tests where tactic_group_id = 1;

DROP TABLE `tactics_workers`;
CREATE TABLE `tactics_workers` (
  `worker_id` int NOT NULL AUTO_INCREMENT,
  `worker_name` varchar(255) DEFAULT NULL,
  `last_run_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `insert_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`worker_id`),
  UNIQUE KEY `worker_name` (`worker_name`)
) ;


DROP TABLE `tactics_tests_results`;
CREATE TABLE `tactics_tests_results` (
  `tactics_tests_results_id` bigint NOT NULL AUTO_INCREMENT,
  `download_settings_id` int DEFAULT NULL,
  `tactic_id` bigint DEFAULT NULL,
  `result_string_1` varchar(4000) DEFAULT NULL,
  `result_string_2` varchar(4000) DEFAULT NULL,
  `result_string_3` varchar(4000) DEFAULT NULL,
  `score_1` double DEFAULT NULL,
  `score_2` double DEFAULT NULL,
  `score_3` double DEFAULT NULL,
  `score_4` double DEFAULT NULL,
  `worker_id` int NULL,
  `insert_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`tactics_tests_results_id`)
);
