drop table job1;
drop table ticker_anno_job1;
drop table ticker_job1;
drop table first_job1;
drop table result_job1;

create table IF  NOT EXISTS job1 (ticker STRING, open DOUBLE, close DOUBLE, adj_close DOUBLE, low DOUBLE, high DOUBLE, volume DOUBLE, data DATE)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY "," LINES TERMINATED BY '\n'
tblproperties ("skip.header.line.count"="1");
LOAD DATA LOCAL INPATH '/home/alex/Scrivania/Primo_Homework/Esercizi_Hive/job1/prices_50mila.csv' OVERWRITE INTO TABLE job1; 



-- group by ticker e anno, prende solo anni tra 1998 e 2018 e calcola media chiusura, min_low, max_high    (PER ANNO)
CREATE TABLE IF  NOT EXISTS ticker_anno_job1 AS
select ticker as ticker , year(data) as anno, AVG(close) as media_close, min(low) as min_low, max(high) as max_high, 
SUM(volume) as sum_volume, count(1) as count_volume
from job1
where year(data) >= 1998 AND year(data) <=2018
group by ticker, year(data);

--group by ticker, prende il minimo, massimo e media volume di tutti gli anni e salva anno minimo e anno massimo
CREATE TABLE IF  NOT EXISTS  ticker_job1 AS
select ticker, min(min_low) as min_low, max(max_high) as max_high, SUM(sum_volume) / SUM(count_volume) as media_volume, min(anno) as min_anno, max(anno) as max_anno
from ticker_anno_job1
group by ticker;

-- per ogni riga della  tabella ticker_job1 aggiunge 2 righe, che contengono la media_close del primo anno e dell'ultimo anno
CREATE TABLE IF  NOT EXISTS first_job1 AS
select distinct t2.ticker as ticker , t1.anno as anno, t1.media_close as media_close, (t2.min_low) as min_low, t2.max_high as max_high, t2.media_volume as media_volume
from ticker_anno_job1 t1, ticker_job1 t2
where t1.ticker = t2.ticker AND (( t1.anno = t2.min_anno) OR ( t1.anno=t2.max_anno))
order by t2.ticker;

-- partition by ticker serve per fare la differenza tra la media di chiusura dell'ultimo anno e del primo anno, ordino in base all'anno
CREATE TABLE IF NOT EXISTS result_job1 AS
select distinct ticker, 
(first_value(media_close) over (partition by ticker order by anno desc) - first_value(media_close) over (partition by ticker order by anno asc))  as diff ,
min_low as min_low,
max_high as max_high,
media_volume as media_volume,
((((first_value(media_close) over (partition by ticker order by anno desc) / first_value(media_close) over (partition by ticker order by anno asc)  ) )*100)-100) as incremento_perc        
from first_job1
order by diff desc limit 10;


select * from result_job1;

