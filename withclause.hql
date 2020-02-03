with KEY_DATA_COUNTS 
as 
(
select
KT.load_ingstn_id as bdf_load_dt, 
KT.ws_seg_name as segment_name, 
count(*) as src_cnt from 
dv_bdfrawzph_nogbd_r000_wh.clm_wgs_key_data KT 
where NOT EXISTS 
( SELECT 1 FROM IIDR_SUCCESS_LOADS suc_loads 
Where KT.load_ingstn_id = suc_loads.load_ingstn_id 
and KT.ws_seg_name = suc_loads.tbl_nm )
 group by KT.load_ingstn_id, KT.ws_seg_name
 ),
 AUDIT_DATA_COUNTS as (
 Select src_prcs_dt, tbl_nm, rcvd_dt, 
 count from (select AT.src_prcs_dt,
 AT.tbl_nm, max(AT.rcvd_dt) as rcvd_dt,
 AT.match_flag as match_flag, count(*) as count 
 from dv_bdfrawzph_nogbd_r000_qc.audit_bdf_clm_wgs_key_data AT 
 group by AT.src_prcs_dt, AT.tbl_nm, AT.match_flag 
 ) A Where trim(upper(A.match_flag)) = 'YES')
 select KC.bdf_load_dt ,AC.tbl_nm ,AC.rcvd_dt ,'CLM' as subj_area_nm ,
 'WGS' as src_sys_nm ,KC.src_cnt ,AC.target_count ,
 AC.target_count/KC.src_cnt*100 as match_percentage ,
 'IIDR Streaming' as LD_PTRN ,current_timestamp() as load_end_dtm 
 from KEY_DATA_COUNTS KC inner join AUDIT_DATA_COUNTS AC 
 on KC.bdf_load_dt = AC.src_prcs_dt and KC.ws_seg_name = AC.tbl_nm;
