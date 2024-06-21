CREATE OR REPLACE VIEW imp.hrs_vw.eval90_query_info as
with
  qry as (
    select
      *
    from
      galaxy_telemetry.public.query_history
  ),
  all as (
    select
      query_id,
      sum(dur) tot_dur,
      sum(
        case
          when key = 'Queued' then dur
          else 0
        end
      ) sum_tot_q,
      sum(
        case
          when key = 'Analysis' then dur
          else 0
        end
      ) sum_tot_a,
      sum(
        case
          when key = 'Planning' then dur
          else 0
        end
      ) sum_tot_p,
      sum(
        case
          when key = 'Execution' then dur
          else 0
        end
      ) sum_tot_e
    from
      query_time
    group by
      1
  ),
  frgtyp as (
    select
      query_id,
      count(*) frag_cnt,
      sum(
        case
          when frgtyp = 'SINGLE' then 1
          else 0
        end
      ) frag_single_cnt,
      sum(
        case
          when frgtyp = 'COORDINATOR_ONLY' then 1
          else 0
        end
      ) frag_conly_cnt,
      sum(
        case
          when frgtyp = 'SOURCE' then 1
          else 0
        end
      ) frag_source_cnt,
      sum(
        case
          when frgtyp = 'ROUND_ROBIN' then 1
          else 0
        end
      ) frag_rr_cnt,
      sum(
        case
          when frgtyp = 'HASH' then 1
          else 0
        end
      ) frag_hash_cnt,
      sum(
        case
          when frgtyp = 'MERGE' then 1
          else 0
        end
      ) frag_merge_cnt
    from
      frag
    group by
      1
  ),
  srcest as (
    select
      e.query_id,
      min(e.num_rows_val) max_num_rows_val,
      max(e.num_rows_val) min_num_rows_val,
      sum(e.missing) miss_cnt,
      sum(
        case
          when e.size_data_unt = 'B' then e.size_data_val
          when e.size_data_unt = 'kB' then e.size_data_val * 1024
          when e.size_data_unt = 'MB' then e.size_data_val * 1024 * 1024
          when e.size_data_unt = 'GB' then e.size_data_val * 1024 * 1024 * 1024
          when e.size_data_unt = 'TB' then e.size_data_val * 1024 * 1024 * 1024 * 1024
          else 0
        end
      ) / (1024 * 1024 * 1024),
      count(*) tot_cnt
    from
      estimates e
      left join frag on frag.query_id = e.query_id
      and e.ord between frag.ord and frag.end_ord
    where
      frag.frgtyp = 'SOURCE'
    group by
      1
  ),
  scntyp as (
    select
      query_id,
      sum(
        case
          when scntyp = 'ScanFilter' then 1
          else 0
        end
      ) cnt_sf,
      sum(
        case
          when scntyp = 'FilterProject' then 1
          else 0
        end
      ) cnt_fp,
      sum(
        case
          when scntyp = 'TableScan' then 1
          else 0
        end
      ) cnt_ts,
      sum(
        case
          when scntyp = 'ScanFilterProject' then 1
          else 0
        end
      ) cnt_sfp,
      sum(
        case
          when scntyp = 'ScanProject' then 1
          else 0
        end
      ) cnt_sp,
      count(*) scn_cnt
    from
      scan
    group by
      1
  ),
  qind as (
    select
      query_id,
      max(indent) max_indent,
      count(distinct indent) cnt_indent
    from
      indent
    group by
      1
  ),
alluntcnv as ( select
 query_id 
,key 
,case when unit = 's' then value 
      when unit = 'ms' then value/1000
      when unit = 'us' then value/(1000 * 1000) 
      when unit = 'us' then value/(1000 * 1000 * 1000)
      when unit = 'm' then value * 60
      when unit = 'm' then value * 60 * 60
end value
from all_time
where Key in ('CPU' , 'Blocked' , 'Scheduled'))
,alltme as (
    select 
    query_id
   ,sum(case when key = 'CPU' then value else 0 end) cpu_sum 
   ,sum(case when key = 'Blocked' then value else 0 end) blk_sum 
   ,sum(case when key = 'Scheduled' then value else 0 end) sch_sum
   ,count(*) cnt 
from alluntcnv
group by 1
)
select
  qry.query_id,
  qry.email,
  qry.role_name,
  qry.create_time,
  qry.execution_start_time,
  qry.end_time,
  all.tot_dur,
  all.sum_tot_q,
  all.sum_tot_a,
  all.sum_tot_p,
  all.sum_tot_e,
  frgtyp.frag_cnt,
  frgtyp.frag_single_cnt,
  frgtyp.frag_conly_cnt,
  frgtyp.frag_source_cnt,
  frgtyp.frag_rr_cnt,
  frgtyp.frag_hash_cnt,
  frgtyp.frag_merge_cnt,
  srcest.max_num_rows_val,
  srcest.min_num_rows_val,
  srcest.miss_cnt,
  srcest.tot_cnt,
  scntyp.cnt_sf,
  scntyp.cnt_fp,
  scntyp.cnt_ts,
  scntyp.cnt_sfp,
  scntyp.cnt_sp,
  scntyp.scn_cnt,
  qind.max_indent,
  qind.cnt_indent,
  alltme.cpu_sum
,alltme.blk_sum
,alltme.sch_sum
,alltme.cnt
from
  qry
  left join all on all.query_id = qry.query_id
  left join frgtyp on frgtyp.query_id = qry.query_id
  left join srcest on srcest.query_id = qry.query_id
  left join scntyp on scntyp.query_id = qry.query_id
  left join qind on qind.query_id = qry.query_id
  left join alltme on alltme.query_id = qry.query_id;
;
