####
#### There are several components
#### ----------------------------
frag.sql - This works out the information for each fragement <br />
indent.sql - for each line work out what the identation is this will allow understanding which part of the <br />
query_time.sql - work out the query components , allows you to decided which query to concentrate on <br />
scan.sql - How is the source data being accessed.<br />
estimates.sql - breakdown the estimate lines. The important ones are the ones from the SOURCE fragments<br />
time_rows.sql - how long fragments take , and input and output rows.<br />

query_info - This is a summary record for the query , from this create aggregate reports <br />
tbl_info - This is a summary record for the query tbl record <br />

example_reports_01.sql - This is an example of reports.


#### At Query Level
####
#### Calculate the 
#### - Total run time
#### - Time by  Analysis , Execution , Planning , Queued
#### - Total number of Fragements : HASH , SINGLE , SOURCE , ROUND_ROBIN , COORDINATOR_ONLY , MERGE , total
#### - Number of Scan Fragments by Type : ScanFilter , FilterProject , TableScan , ScanFilterProject , ScanProject
#### - sum of input and output by fragment type: 
#### - sum of input and output by total
#### - deepest level of indentation
#### - SOURCE fragments with no estimates.
#### - sum of time for source fragments with no time.
