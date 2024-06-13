--
-- Work out the indentation of each line , 
--

USE galaxy_telemetry.public;

DROP TABLE IF EXISTS sandbox01.exp.indent;

CREATE TABLE sandbox01.exp.indent as 
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
      length(ltrim(query_plan)) qlt
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
  *,
  ql - qlt indent
from
  qp1
where
  ql > 0
order by
  1,
  3;