--
-- Output 1 line per fragment
--
use galaxy_telemetry.public;
drop table if exists sandbox01.exp.frag; 
create table sandbox01.exp.frag as 
with
  qh as (
    select
      query_id,
      split(query_plan, chr(10)) query_plan_parts
    from
      query_history
  ),
  qp1 as (
    select
      query_plan_parts,
      query_id,
      query_plan,
      ord,
      length(query_plan) ql,
      length(ltrim(query_plan)) qlt,
      ltrim(query_plan) lqp
    from
      qh,
      UNNEST (query_plan_parts)
    with
      ordinality AS t (query_plan, ord)
    order by
      1,
      3
  ),
  prv as (
    select
      query_plan_parts,
      query_id,
      lqp,
      ord,
      ql - qlt indent
    from
      qp1
    where
      ql > 0
      -- find CPU lines , find the input and output  
      --  and ( substring(lqp,1,3) = 'CPU' or substring (lqp,1,7) = '│   CPU') 
      -- Find the large breakdown to look for big queries 
      and lqp like ('Fragment%')
  )
SELECT
  query_id,
  lqp,
  ord,
  SPLIT(lqp, ' ') [2] frgno,
  regexp_replace(SPLIT(lqp, ' ')[3], '[\[\]]', '') frgtyp,
  COALESCE(
    LEAD(ord, 1) OVER (
      partition by
        query_id
      order by
        ord
    ),
    1000000
  ) -1 as end_ord
from
  prv;
   
