--
-- where ord is 4 this is the total
--
use galaxy_telemetry.public;
DROP TABLE IF EXISTS sandbox01.exp.all_time; 
CREATE TABLE sandbox01.exp.all_time as 
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
      ltrim(replace(lqp, '│', '')) lqp,
      ord,
      split(ltrim(replace(lqp, '│', '')), ',') lqp_arr
    from
      qp1
    where
      ql > 0
      -- find CPU lines , find the input and output  
      and (
        substring(lqp, 1, 3) = 'CPU'
        or substring(lqp, 1, 7) = '│   CPU'
      )
      -- Find the large breakdown to look for big queries 
      -- and lqp like ('Queued%') 
  ),
  key_value_pairs AS (
    SELECT
      lqp,
      query_id,
      ord,
      key_value
    FROM
      kvp_inp,
      UNNEST (lqp_arr) AS t (key_value)
  ),
  split_pairs AS (
    SELECT
      lqp,
      query_id,
      ord,
      key_value,
      -- key
      TRY(
      CASE WHEN SUBSTRING(TRIM(SPLIT(key_value, ':') [1]),1,7) = 'Blocked' THEN SPLIT(TRIM(SPLIT(key_value, ':') [1]),' ')[1] 
      ELSE TRY(TRIM(SPLIT(key_value, ':') [1])) END 
      ) AS key,
      -- value
      CASE  WHEN SUBSTRING(TRIM(SPLIT(key_value, ':') [1]),1,3) IN ('CPU','Sch','Blo','Out','Inp') THEN SPLIT(TRIM(SPLIT(key_value, ':') [2]),'(')[1] 
      ELSE TRIM(SPLIT(key_value, ':') [2]) END AS value,
      -- key2
      TRY(
      CASE WHEN SUBSTRING(TRIM(SPLIT(key_value, ':') [1]),1,7) = 'Blocked' AND TRY(SPLIT(TRIM(SPLIT(key_value, ':') [1]),' ')[2]) is not null THEN 'InputBlocked'
           WHEN SUBSTRING(TRIM(SPLIT(key_value, ':') [1]),1,3) IN ('CPU','Sch','Blo') AND TRY(SPLIT(TRIM(SPLIT(key_value, ':') [2]),'(')[2]) is not null THEN 'pct' 
           WHEN SUBSTRING(TRIM(SPLIT(key_value, ':') [1]),1,3) IN ('Inp','Out') THEN 'Bytes'
      ELSE TRY(TRIM(SPLIT(key_value, ':') [3])) END 
      ) AS key2,
      -- value2
      CASE WHEN SUBSTRING(TRIM(SPLIT(key_value, ':') [1]),1,7) = 'Blocked' AND TRY(SPLIT(TRIM(SPLIT(key_value, ':') [1]),' ')[2]) is not null THEN TRY(SPLIT(TRIM(SPLIT(key_value, ':') [1]),' ')[2])
           WHEN SUBSTRING(TRIM(SPLIT(key_value, ':') [1]),1,3) IN ('CPU','Sch','Out','Blo') THEN TRY(SPLIT(TRIM(SPLIT(key_value, ':') [2]),'(')[2]) 
            WHEN SUBSTRING(TRIM(SPLIT(key_value, ':') [1]),1,3) IN ('Inp') THEN TRY(SPLIT(SPLIT(key_value, '(') [2],')')[1])
      ELSE TRY(TRIM(SPLIT(key_value, ':') [4])) 
      END AS value2,
      -- key3
      CASE  WHEN SUBSTRING(TRIM(SPLIT(key_value, ':') [1]),1,3) IN ('Inp') THEN 'avg'
      ELSE TRY(TRIM(SPLIT(key_value, ':') [5])) 
      END AS key3,
      -- value3
      CASE  WHEN SUBSTRING(TRIM(SPLIT(key_value, ':') [1]),1,3) IN ('Inp') THEN TRY(replace(TRIM(SPLIT(key_value, ':') [4]),'std.dev.',''))    
      ELSE TRY(TRIM(SPLIT(key_value, ':') [6])) 
      END AS value3,
      -- key4
      CASE  WHEN SUBSTRING(TRIM(SPLIT(key_value, ':') [1]),1,3) IN ('Inp') THEN 'stddev'
      ELSE NULL
      END AS key4,
      -- value4
      CASE  WHEN SUBSTRING(TRIM(SPLIT(key_value, ':') [1]),1,3) IN ('Inp') THEN TRY(replace(TRIM(SPLIT(key_value, ':') [5]),'std.dev.',''))  
      ELSE NULL
      END AS value4

    FROM
      key_value_pairs
  ),
  fin as (
SELECT
  lqp,
  key_value,
  query_id,
  ord,
  key,
  regexp_replace(value, '[^0-9.]', '') value,
  CASE WHEN value like ('%row%') then 'rows' 
       WHEN value like ('%ns%') then 'ns' 
       WHEN value like ('%ms%') then 'ms' 
       WHEN value like ('%us%') then 'us' 
       WHEN value like ('%hr%') then 'hr'
       WHEN value like ('%m%') then 'm'
       WHEN value like ('%s%') then 's'
       WHEN value like ('%kB%') then 'KB'
       WHEN value like ('%MB%') then 'MB'
       WHEN value like ('%B%') then 'B'
       ELSE NULL
       END unit , 
  key2,
  regexp_replace(value2, '[^0-9.]', '') value2,
  CASE WHEN value2 like ('%rows%') then 'rows' 
        WHEN value2 like ('%ns%') then 'ns' 
       WHEN value2 like ('%ms%') then 'ms' 
       WHEN value2 like ('%us%') then 'us' 
       WHEN value2 like ('%hr%') then 'hr'
       WHEN value2 like ('%m%') then 'm'
       WHEN value2 like ('%s%') then 's' 
        WHEN value2 like ('%kB%') then 'KB'
        WHEN value2 like ('%MB%') then 'MB'
        WHEN value2 like ('%B%') then 'B'
        ELSE NULL
        END unit2 ,
  key3,
  regexp_replace(value3, '[^0-9.]', '') value3,
  key4,
  regexp_replace(value4, '[^0-9.]', '') value4
FROM
  split_pairs)
  select *  from fin;
