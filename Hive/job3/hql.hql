drop table prices;
drop table stocks_noparse;
drop table ticker_name_sector;
drop table join_job3;
drop table nome_anno_settore;
drop table concat;
drop table result_job3;


create table IF  NOT EXISTS prices (ticker STRING, open DOUBLE, close DOUBLE, adj_close DOUBLE, low DOUBLE, high DOUBLE, volume DOUBLE, data DATE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "," LINES TERMINATED BY '\n'
tblproperties ("skip.header.line.count"="1");
LOAD DATA LOCAL INPATH './Scrivania/Primo_Homework/daily-historical-stock-prices-1970-2018/historical_stock_prices.csv' OVERWRITE INTO TABLE prices;


create table if not exists stocks_noparse(riga STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\n" 
tblproperties ("skip.header.line.count"="1");
LOAD DATA LOCAL INPATH './Scrivania/Primo_Homework/daily-historical-stock-prices-1970-2018/historical_stocks.csv' OVERWRITE INTO TABLE stocks_noparse;


add jar ./Scrivania/Primo_Homework/Hive/job3/ParseRowJob3.jar;
CREATE TEMPORARY FUNCTION ParseRowJob3 AS 'hive.ParseRowJob3'; 

create table if not exists ticker_name_sector as
select ParseRowJob3(riga) as (ticker, name, sector)
from stocks_noparse;


create table if not exists join_job3 as
select t2.name as name,t1.data as data, t2.sector as sector , t1.ticker as ticker,  t1.close as close
from prices t1, ticker_name_sector t2
where t1.ticker = t2.ticker AND (year(t1.data)>=2016 AND year(t1.data)<=2018);



create table if not exists nome_anno_settore as

select distinct t.name as name , t.anno as anno, t.sector as sector, 
(SUM(t.incremento_perc) over (partition by t.name, t.anno,t.sector))  as incremento_perc
from
(
select distinct name as name , year(data) as anno, sector as sector, ticker as ticker,
 cast( ((((first_value(close) over (partition by name, year(data), sector,ticker order by data desc) / first_value(close) over (partition by name, year(data), sector,ticker order by data asc)  ) )*100)-100) as int ) 
 as incremento_perc
from join_job3
order by anno
) t;




create table if not exists concat as 
	select name, sector, collect_list(incremento_perc) as incrementi_percentuali
	from nome_anno_settore
	group by name,sector
	having count(1) =3;



create table if not exists result_job3 as 
	select distinct t1.name as nome_prima_azienda, t2.name as nome_seconda_azienda, t1.incrementi_percentuali as incrementi_percentuali
	from concat t1, concat t2
	where 	t1.incrementi_percentuali[0] = t2.incrementi_percentuali[0] AND
				t1.incrementi_percentuali[1] = t2.incrementi_percentuali[1] AND
				t1.incrementi_percentuali[2] = t2.incrementi_percentuali[2] AND
				t1.sector <> t2.sector 																   AND 
				t1.name < t2.name;


select * from result_job3;




