set hive.cli.print.header=true;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.ptf.enabled=true;
set hive.fetch.task.conversion=none;

drop table over10k_n1;

create table over10k_n1(
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
	   ts timestamp, 
           `dec` decimal,  
           bin binary)
       row format delimited
       fields terminated by '|';

load data local inpath '../../data/files/over10k' into table over10k_n1;

explain vectorization detail
select s, sum(b) over (partition by i order by s,b rows unbounded preceding) from over10k_n1 limit 100;
select s, sum(b) over (partition by i order by s,b rows unbounded preceding) from over10k_n1 limit 100;

explain vectorization detail
select s, sum(f) over (partition by d order by s,f rows unbounded preceding) from over10k_n1 limit 100;
select s, sum(f) over (partition by d order by s,f rows unbounded preceding) from over10k_n1 limit 100;

explain vectorization detail
select s, sum(f) over (partition by ts order by f range between current row and unbounded following) from over10k_n1 limit 100;
select s, sum(f) over (partition by ts order by f range between current row and unbounded following) from over10k_n1 limit 100;

explain vectorization detail
select s, avg(f) over (partition by ts order by s,f rows between current row and 5 following) from over10k_n1 limit 100;
select s, avg(f) over (partition by ts order by s,f rows between current row and 5 following) from over10k_n1 limit 100;

explain vectorization detail
select s, avg(d) over (partition by t order by s,d desc rows between 5 preceding and 5 following) from over10k_n1 limit 100;
select s, avg(d) over (partition by t order by s,d desc rows between 5 preceding and 5 following) from over10k_n1 limit 100;

explain vectorization detail
select s, sum(i) over(partition by ts order by s) from over10k_n1 limit 100;
select s, sum(i) over(partition by ts order by s) from over10k_n1 limit 100;

explain vectorization detail
select f, sum(f) over (partition by ts order by f range between unbounded preceding and current row) from over10k_n1 limit 100;
select f, sum(f) over (partition by ts order by f range between unbounded preceding and current row) from over10k_n1 limit 100;

explain vectorization detail
select f, sum(f) over (partition by ts order by f rows between 2 preceding and 1 preceding) from over10k_n1 limit 100;
select f, sum(f) over (partition by ts order by f rows between 2 preceding and 1 preceding) from over10k_n1 limit 100;

explain vectorization detail
select s, i, round(avg(d) over (partition by s order by i) / 10.0 , 2) from over10k_n1 limit 7;
select s, i, round(avg(d) over (partition by s order by i) / 10.0 , 2) from over10k_n1 limit 7;

explain vectorization detail
select s, i, round((avg(d) over  w1 + 10.0) - (avg(d) over w1 - 10.0),2) from over10k_n1 window w1 as (partition by s order by i) limit 7;
select s, i, round((avg(d) over  w1 + 10.0) - (avg(d) over w1 - 10.0),2) from over10k_n1 window w1 as (partition by s order by i) limit 7;

set hive.cbo.enable=false;
-- HIVE-9228 
explain vectorization detail
select s, i from ( select s, i, round((avg(d) over  w1 + 10.0) - (avg(d) over w1 - 10.0),2) from over10k_n1 window w1 as (partition by s order by i)) X limit 7;
select s, i from ( select s, i, round((avg(d) over  w1 + 10.0) - (avg(d) over w1 - 10.0),2) from over10k_n1 window w1 as (partition by s order by i)) X limit 7;
