ADD JAR hdfs:///user/oracle/H2O/UDFtest/GBMAvgBitRateM17lib/v2.0/h2o-genmodel.jar;
ADD JAR hdfs:///user/oracle/H2O/UDFtest/GBMAvgBitRateM17lib/v2.0/ScoreDataUDFGBMAVGM17-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION scoredatavg AS 'ai.h2o.hive.udf.ScoreDataUDFGBMAVGM17';
USE iptv;
SHOW TABLES;
DROP TABLE IF EXISTS iptv.avgbitrate_predict;
CREATE TABLE iptv.avgbitrate_predict AS SELECT asset, deviceos, country, state, city, asn, isp, start_time_unix_time,startup_time_ms, playing_time_ms, buffering_time_ms, interrupts, startup_error, sessiontags, ipaddress, cdn, browser, convivasessionid, streamurl, errorlist, percentage_complete, average_bitrate_kbps,
 scoredatavg(asn, start_time_unix_time, startup_time_ms, playing_time_ms, buffering_time_ms, interrupts, startup_error, percentage_complete) 
 as predict_average_bitrate_kbps FROM iptv.avgbitrate
  LIMIT 10000;
