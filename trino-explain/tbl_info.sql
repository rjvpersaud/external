USE SANDBOX01.exp;
CREATE OR REPLACE VIEW imp.hrs_vw.eval91_tbl_info as
with tbl as ( SELECT e.query_id , e.tbl,e.scntyp,e.ord, frgno
    from
      -- Table
      SANDBOX01.exp.scan e
      -- Fragment
      left join SANDBOX01.exp.frag on frag.query_id = e.query_id
      and e.ord between frag.ord and frag.end_ord
      where indent =4 and frgtyp = 'SOURCE') 
, est as(
select frag.query_id , frag.frgno , e.ord , missing , size_data_val , size_data_unt from 
estimates e 
      left join SANDBOX01.exp.frag on frag.query_id = e.query_id
      and e.ord between frag.ord and frag.end_ord
where frgtyp = 'SOURCE')
select tbl.query_id
,tbl
,scntyp
,tbl.frgno
,missing
,size_data_val
,size_data_unt
from est,tbl
where est.query_id = tbl.query_id
  and est.frgno = tbl.frgno;
