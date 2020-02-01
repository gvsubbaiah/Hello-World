# Hello-World
test 


spark.sql(""" select 
AT.rcvd_dt as bdf_load_dt
,AT.src_prcs_dt
,'CLM' as subj_area_nm
,'WGS' as src_sys_nm
,AT.tbl_nm as segment_nm
,count(KT.ws_seg_name) as src_cnt
,case when AT.match_flag = 'Yes' and AT.processed_staus = 'INITIAL_MATCH' then count() over(partition by AT.src_prcs_dt,AT.tbl_nm ,AT.src_prcs_dt) end as tgt_count
from source-table KT inner join target-table AT
ON 
KT.ws_seg_name = AT.tbl_nm
where AT.rcvd_dt = AT.src_prcs_dt 
group by AT.rcvd_dt,KT.ws_seg_name,AT.tbl_nm,AT.src_prcs_dt,AT.match_flag,AT.processed_staus """).show(1000,false)
