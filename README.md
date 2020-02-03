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





Step: 1 (Get Date,segment details where source and target are matching (match percent 100)) {Temp View name:IIDR_SUCCESS_LOADS}
====================================================================================================================================
select 
iidr_summ.src_prcs_dt,
iidr_summ.tbl_nm 
from clm_wgs_summary_table iidr_summ 
where match_percentage = 100
group by iidr_summ.src_prcs_dt, iidr_summ.tbl_nm;


Step 2: (Get Source/Key count from KEY table for each day, segment wise) {Temp View name: KEY_DATA_COUNTS}
==============================================================================================================
select  
KT.load_ingstn_id as bdf_load_dt,
KT.ws_seg_name as segment_name,
count(*) as src_cnt 
from dv_bdfrawzph_nogbd_r000_wh.clm_wgs_key_data KT 
where NOT EXISTS ( SELECT 1 FROM  IIDR_SUCCESS_LOADS suc_loads
Where KT.load_ingstn_id = suc_loads.load_ingstn_id
and KT.ws_seg_name = suc_loads.tbl_nm )
group by KT.load_ingstn_id, KT.ws_seg_name


Step 3: (Get Target count from AUDIT table for MATCHED KEYS for each day, segment wise) {Temp View name: AUDIT_DATA_COUNTS}
============================================================================================================================
Select 
src_prcs_dt,
tbl_nm,
rcvd_dt,
count
from (select 
AT.src_prcs_dt,
AT.tbl_nm,
max(AT.rcvd_dt) as rcvd_dt,
AT.match_flag as match_flag,
count(*) as count
from dv_bdfrawzph_nogbd_r000_qc.audit_bdf_clm_wgs_key_data AT
group by AT.src_prcs_dt, AT.tbl_nm, AT.match_flag ) A
Where trim(upper(A.match_flag)) = 'YES';

Step 4: (Join Step 2 and Step 3) (TO get actual summary table results)
================================================
select 
KC.bdf_load_dt
,AC.tbl_nm
,AC.rcvd_dt
,'CLM' as subj_area_nm
,'WGS' as src_sys_nm
,KC.src_cnt
,AC.target_count
,AC.target_count/KC.src_cnt*100 as  match_percentage
,'IIDR Streaming' as LD_PTRN
,current_timestamp() as load_end_dtm
from KEY_DATA_COUNTS KC inner join AUDIT_DATA_COUNTS AC 
on KC.bdf_load_dt  = AC.src_prcs_dt 
and KC.ws_seg_name = AC.tbl_nm;
 
