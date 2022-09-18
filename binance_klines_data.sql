-- create settings table
CREATE TABLE `binance_download_settings` (
  `download_settings_id` int NOT NULL AUTO_INCREMENT,
  `market` varchar(10) DEFAULT NULL,
  `tick_interval` varchar(50) DEFAULT NULL,
  `data_granulation` varchar(50) DEFAULT NULL,
  `stock_type` varchar(255) DEFAULT NULL,
  `stock_exchange` varchar(255) DEFAULT NULL,
  `current_range_to_overwrite` int DEFAULT NULL,
  `download_priority` int NOT NULL,
  `download_api_interval_sec` int NOT NULL,
  `download_setting_status_id` int NOT NULL DEFAULT '0',
  `download_settings_desc` varchar(255) DEFAULT NULL,
  `current_update_from_api` int DEFAULT '1',
  `daily_update_from_files` int DEFAULT '1',
  `monthly_update_from_files` int DEFAULT '1',
  `daily_hist_complete` int DEFAULT '0',
  `monthly_hist_complete` int DEFAULT '0',
  `start_hist_download_ux_timestamp` int DEFAULT '1483225200',
  `last_download_ux_timestamp` int DEFAULT NULL,
  `next_download_ux_timestamp` int DEFAULT NULL,
  `insert_ux_timestamp` int DEFAULT NULL,
  PRIMARY KEY (`download_settings_id`)
);

-- create klines table for downloaded data
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
);

-- create view to analytical use
CREATE VIEW `vw_binance_klines_anl` AS
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
        (`binance_klines_data` `a`
        JOIN `binance_download_settings` `b` ON ((`a`.`download_settings_id` = `b`.`download_settings_id`)))
    ORDER BY `a`.`open_time`;