--
-- heaviest user , limit to find top X
--
with sel as 
(
    select email , sum(tot_dur) tot_dur, sum(sum_tot_e) sum_tot_e , sum(sum_tot_q) sum_tot_q
, sum(blk_sum) blk_sum,  sum(cpu_sum) cpu_sum, sum(sch_sum) sch_sum
, sum(frag_cnt) frag_cnt
, sum(difval_sum) difval_sum, sum(inval_sum) inval_sum, avg(difpct_max) difpct_max
,count(*)
from imp.hrs_vw.eval90_query_info
group by 1
order by 2 desc
)
select * from sel limit 10;

--
-- heaviest role
--
with sel as 
(
select role_name , sum(tot_dur) tot_dur, sum(sum_tot_e) sum_tot_e , sum(sum_tot_q) sum_tot_q
, sum(blk_sum) blk_sum,  sum(cpu_sum) cpu_sum, sum(sch_sum) sch_sum
, sum(frag_cnt) frag_cnt
, sum(difval_sum) difval_sum, sum(inval_sum) inval_sum, avg(difpct_max) difpct_max
,count(*)
from imp.hrs_vw.eval90_query_info
group by 1
order by 2 desc
)
select * from sel limit 10;

--
-- Find table that is 
-- most used with no stats
-- TODO 
select tbl , count(*)  from imp.hrs_vw.eval91_tbl_info where missing = 1 group by 1 order by 2 desc;

--
-- Find top 100 queries that are blocked
-- 
with sel as 
(
select * 
from imp.hrs_vw.eval90_query_info
group by 1
order by blk_sum desc
)
select * from sel limit 100;


--
-- Find queries that would benefit from WARP SPEED , who is the top user
-- 
with sel as 
(
select * 
from imp.hrs_vw.eval90_query_info
group by 1
order by difpct_sum , inval_sum desc
)
select * from sel limit 100;


