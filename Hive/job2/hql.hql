drop table prices;
drop table stocks_noparse;
drop table ticker_settore;
drop table first_join;
drop table settore_ticker_anno;
drop table result_job2;



create table IF  NOT EXISTS prices (ticker STRING, open DOUBLE, close DOUBLE, adj_close DOUBLE, low DOUBLE, high DOUBLE, volume DOUBLE, data DATE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "," LINES TERMINATED BY '\n'
tblproperties ("skip.header.line.count"="1");
--LOAD DATA LOCAL INPATH '/home/alex/Scrivania/Primo_Homework/daily-historical-stock-prices-1970-2018/historical_stock_prices.csv' OVERWRITE INTO TABLE prices;
LOAD DATA LOCAL INPATH '/home/alex/Scrivania/Primo_Homework/Hive/job2/SecondTable' OVERWRITE INTO TABLE prices;  


create table if not exists stocks_noparse(riga STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\n" 
tblproperties ("skip.header.line.count"="1");
--LOAD DATA LOCAL INPATH '/home/alex/Scrivania/Primo_Homework/daily-historical-stock-prices-1970-2018/historical_stocks.csv' OVERWRITE INTO TABLE stocks_noparse;
LOAD DATA LOCAL INPATH '/home/alex/Scrivania/Primo_Homework/Hive/job2/FirstTable' OVERWRITE INTO TABLE stocks_noparse; 



add jar /home/alex/Scrivania/Primo_Homework/Hive/job2/ParseRow.jar;
CREATE TEMPORARY FUNCTION ParseRow AS 'hive.ParseRow'; 

create table if not exists ticker_settore as
select ParseRow(riga) as (ticker, sector)
from stocks_noparse;


create table if not exists first_join as
select t2.sector as settore, t1.data as data, t1.ticker as ticker, t1.close as close, t1.volume as volume
from prices t1, ticker_settore t2
where (t1.ticker = t2.ticker) and (year(t1.data)>=2004 AND year(t1.data)<=2018);  --AND perchè è condizione sulla stessa variabile


 create table if not exists result_job2 as

select distinct  p.settore, p.anno,
p.somma_volumi, SUM(p.incremento_perc) over (partition by p.settore,p.anno), p.media_close
from
 (
 select distinct settore as settore, year(data) as anno,
  ((((first_value(close) over (partition by settore, year(data), ticker order by data desc) / first_value(close) over (partition by settore, year(data), ticker order by data asc)  ) )*100)-100) as incremento_perc,
  (SUM(volume) over (partition by settore, year(data))) as somma_volumi,
  (AVG(close) over (partition by settore, year(data))) as media_close
 from first_join
) p;



 select * from result_job2;


 