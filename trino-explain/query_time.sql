-- SQL to get out times for the complete query
DROP TABLE IF EXISTS sandbox01.exp.query_time;

CREATE TABLE sandbox01.exp.query_time as 
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
  ),
  kvp_inp as (
    select
      query_id,
      lqp,
      ord,
      split(lqp, ',') lqp_arr
    from
      qp1
    where
      ql > 0
      -- find CPU lines , find the input and output  
      --  and ( substring(lqp,1,3) = 'CPU' or substring (lqp,1,7) = '│   CPU') 
      -- Find the large breakdown to look for big queries 
      and lqp like ('Queued%')
  ),
  key_value_pairs AS (
    SELECT
      query_id,
      ord,
      key_value
    FROM
      kvp_inp,
      UNNEST (lqp_arr) AS t (key_value)
  ),
  split_pairs AS (
    SELECT
      query_id,
      ord,
      TRIM(SPLIT(key_value, ':') [1]) AS key,
      TRIM(SPLIT(key_value, ':') [2]) AS value
    FROM
      key_value_pairs
  )
SELECT
  query_id,
  ord,
  key,
  value,
  regexp_replace(value, '[^0-9.]', '') dur,
  regexp_replace(value, '[^a-zA-Z]', '') dur_unit
FROM
  split_pairs;