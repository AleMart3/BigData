drop table prices;
drop table stocks_noparse;
drop table ticker_name_sector;
drop table join_job3;
drop table nome_anno_settore;
drop table result_job3;




create table IF  NOT EXISTS prices (ticker STRING, open DOUBLE, close DOUBLE, adj_close DOUBLE, low DOUBLE, high DOUBLE, volume DOUBLE, data DATE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "," LINES TERMINATED BY '\n'
tblproperties ("skip.header.line.count"="1");
--LOAD DATA LOCAL INPATH '/home/alex/Scrivania/Primo_Homework/daily-historical-stock-prices-1970-2018/historical_stock_prices.csv' OVERWRITE INTO TABLE prices;
LOAD DATA LOCAL INPATH '/home/alex/Scrivania/Primo_Homework/Hive/job3/SecondTable' OVERWRITE INTO TABLE prices;  


create table if not exists stocks_noparse(riga STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\n" 
tblproperties ("skip.header.line.count"="1");
--LOAD DATA LOCAL INPATH '/home/alex/Scrivania/Primo_Homework/daily-historical-stock-prices-1970-2018/historical_stocks.csv' OVERWRITE INTO TABLE stocks_noparse;
LOAD DATA LOCAL INPATH '/home/alex/Scrivania/Primo_Homework/Hive/job3/FirstTable' OVERWRITE INTO TABLE stocks_noparse; 



add jar /home/alex/Scrivania/Primo_Homework/Hive/job3/ParseRowJob3.jar;
CREATE TEMPORARY FUNCTION ParseRowJob3 AS 'hive.ParseRowJob3'; 

create table if not exists ticker_name_sector as
select ParseRowJob3(riga) as (ticker, name, sector)
from stocks_noparse;


create table if not exists join_job3 as
select t2.name as name,t1.data as data, t2.sector as sector ,  t1.close as close
from prices t1, ticker_name_sector t2
where t1.ticker = t2.ticker AND (year(t1.data)>=2016 AND year(t1.data)<=2018);


create table if not exists nome_anno_settore as
select distinct name as name , year(data) as anno, sector as sector,
 cast( ((((first_value(close) over (partition by name, year(data), sector order by data desc) / first_value(close) over (partition by name, year(data), sector order by data asc)  ) )*100)-100) as int ) as incremento_perc
from join_job3;

select * from nome_anno_settore;


create table if not exists result_job3 as
select t1.name as nome1, t2.name as nome2, t1.anno, t1.incremento_perc
from nome_anno_settore t1, nome_anno_settore t2
where t1.incremento_perc = t2.incremento_perc AND t1.name <> t2.name AND t1.sector <> t2.sector;



select * from result_job3;

