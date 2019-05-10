--drop table stocks_noparse;


--create table IF  NOT EXISTS prices (ticker STRING, open DOUBLE, close DOUBLE, adj_close DOUBLE, low DOUBLE, high DOUBLE, volume DOUBLE, data DATE)
--ROW FORMAT DELIMITED FIELDS TERMINATED BY "," LINES TERMINATED BY '\n'
--tblproperties ("skip.header.line.count"="1");
--LOAD DATA LOCAL INPATH '/home/alex/Scrivania/Primo_Homework/Esercizi_Hive/job1/prices_50mila.csv' OVERWRITE INTO TABLE job1; 



--create table IF NOT EXISTS stocks (ticker STRING, exchange STRING, name STRING, sector STRING, industry STRING)
create table if not exists stocks_noparse(riga STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\n" 
tblproperties ("skip.header.line.count"="1");
LOAD DATA LOCAL INPATH '/home/alex/Scrivania/Primo_Homework/Esercizi_Hive/job2/prova.csv' OVERWRITE INTO TABLE stocks_noparse; 

add jar /home/alex/Scrivania/Primo_Homework/Esercizi_Hive/job2/ParseRow.jar;
CREATE TEMPORARY FUNCTION ParseRow AS 'hive.ParseRow'; 

add jar /home/alex/Scrivania/Primo_Homework/Esercizi_Hive/job2/ParseRow2.jar;
CREATE TEMPORARY FUNCTION ParseRow2 AS 'hive.ParseRow2'; 

select ParseRow(riga)
from stocks_noparse;

create table if not exists stocks as
select ParseRow2(riga) as (ticker, sector)
from stocks_noparse;
