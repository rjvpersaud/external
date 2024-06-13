####
#### There are several components
#### ----------------------------
frag.sql - This works out the information for each fragement
indent.sql - for each line work out what the identation is this will allow understanding which part of the 
query_time.sql - work out the query components , allows you to decided which query to concentrate on 
scan.sql - How is the source data being accessed.
estimates.sql - breakdown the estimate lines. The important ones are the ones from the SOURCE fragments
time_rows.sql - how long fragments take , and input and output rows.

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