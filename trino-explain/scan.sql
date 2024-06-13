-- 
-- Get Table Scan operation 
-- 
USE galaxy_telemetry.public;
drop table if exists sandbox01.exp.scan;
create table sandbox01.exp.scan as 
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
  )
select
  query_id,
  lqp,
  TRY(REGEXP_EXTRACT(
    ltrim(split(split(lqp, '[') [2], '=') [2]),
    '^[a-zA-Z0-9:._]+'
  )) tbl,
  regexp_replace(split(lqp, '[') [1], '[^a-zA-Z]', '') scntyp,
  array_join(
    reverse(trim_array(reverse(split(lqp, '[')), 1)),
    '['
  ) aftscn,
  ord,
  ql - qlt indent
from
  qp1
where
  ql > 0
  -- find CPU lines , find the input and output  
  --  and ( substring(lqp,1,3) = 'CPU' or substring (lqp,1,7) = '│   CPU') 
  -- Find the large breakdown to look for big queries 
  and (lqp like '%Scan%')
order by
  1,3;