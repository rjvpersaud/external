use galaxy_telemetry.public;
drop table if exists sandbox01.exp.estimates; 
create table sandbox01.exp.estimates as 
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
      split(
        try(ltrim(split(split(query_plan, '{') [2], '}') [1])),
        ','
      ) ests
    from
      qh,
      UNNEST (query_plan_parts)
    with
      ordinality AS t (query_plan, ord)
    where
      query_plan like '%Estimates%' 
    order by
      1,
      3
  ), 
  dec as (
select
  query_id,
  ord, ests,
  try(replace(split(split(ests[1],':')[2],'(')[1],'?','')) num_rows,
  case when try(trim(split(split(ests[1],':')[2],'(')[1]))='?' then 1 else 0 end missing,
  try(replace(split(split(split(ests[1],':')[2],'(')[2],')')[1],'?')) size_data,
  try(replace(split(ests[2],':')[2],'?')) cpu,
  try(replace(split(ests[3],':')[2],'?')) mem,
  try(replace(split(ests[4],':')[2],'?')) ntw
from
  qp1
  where ests is not null)
  select 
   query_id
  ,ord
  ,cast(case when trim(num_rows) = '' then null else regexp_replace(num_rows, '[^0-9.]', '')  end as bigint)  num_rows_val
  ,missing
  ,cast(case when trim(size_data) = '' then null else regexp_replace(size_data, '[^0-9.]', '')  end as decimal)  size_data_val
  ,cast(case when trim(size_data) = '' then null else regexp_replace(size_data, '[^a-zA-Z]', '')  end as varchar)  size_data_unt
  ,cast(case when trim(cpu) = '' then null else regexp_replace(cpu, '[^0-9.]', '')  end as decimal)  cpu_val
  ,cast(case when trim(cpu) = '' then null else regexp_replace(cpu, '[^a-zA-Z]', '')  end as varchar)  cpu_unt
  ,cast(case when trim(mem) = '' then null else regexp_replace(mem, '[^0-9.]', '')  end as decimal)  mem_val
  ,cast(case when trim(mem) = '' then null else regexp_replace(mem, '[^a-zA-Z]', '')  end as varchar)  mem_unt
  ,cast(case when trim(ntw) = '' then null else regexp_replace(ntw, '[^0-9.]', '')  end as decimal)  ntw_val
  ,cast(case when trim(ntw) = '' then null else regexp_replace(ntw, '[^a-zA-Z]', '')  end as varchar)  ntw_unt
   from dec ;