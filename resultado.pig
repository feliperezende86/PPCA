REGISTER '/dados/bigrco04/pig/mtdl_msd.py' USING org.apache.pig.scripting.streaming.python.PythonScriptEngine AS mtdl_msd
REGISTER '/dados/bigrco04/pig/mtdl_eaopt.py' USING org.apache.pig.scripting.streaming.python.PythonScriptEngine AS mtdl_eaopt

--LOAD DA FAMILIA DE CONTRATO
CTR = 	LOAD 'hbase://dirco_gecig:rst506' 
USING org.apache.pig.backend.hadoop.hbase.HBaseStorage( 'ctr_opr:*, sctr_opr:*, sd_sctr:*') 
AS (ctr:map[], sctr:map[], saldo:map[]);

--LOAD DA FAMILIA DE SUB CONTRATO
CTR_OPR = foreach CTR generate (long)$0#'NR_UNCO_CTR_OPR' AS NR_UNCO_CTR_OPR,(int)$0#'NR_EPRD_FNCD' AS NR_EPRD_FNCD,(int)$0#'NR_SCTR_OPR' AS NR_SCTR_OPR
,(chararray)$0#'SG_SIS_RSP_CDU' AS SG_SIS_RSP_CDU,(int)$0#'CD_PRF_DEPE_CDU' AS CD_PRF_DEPE_CDU,(int)$0#'NR_CTR_OPR' AS NR_CTR_OPR,(int)$0#'CD_EST_CDU' AS CD_EST_CDU
,(datetime)$0#'DT_EST_CDU' AS DT_EST_CDU,(int)$0#'CD_CNL_FRMZ_CTR' AS CD_CNL_FRMZ_CTR,(int)$0#'CD_PRF_DEPE_NEGR' AS CD_PRF_DEPE_NEGR,(int)$0#'CD_PRF_DEPE_EXEC' AS CD_PRF_DEPE_EXEC
,(int)$0#'CD_PRD' AS CD_PRD,(int)$0#'CD_MDLD' AS CD_MDLD,(int)$0#'CD_IEC_MOE' AS CD_IEC_MOE,(int)$0#'CD_CIA_RSP_CTR' AS CD_CIA_RSP_CTR;

CTR_OPR = FILTER CTR_OPR BY CD_EST_CDU == 1;

--RESTRINGE SOMENTE OS CONTRATOS ATIVOS NO OPR (ESTADO DE CONDUÇÃO = 1)
CTR_OPR = FILTER CTR_OPR BY CD_EST_CDU == 1;

CTR_OPR = FOREACH CTR_OPR GENERATE SG_SIS_RSP_CDU,CD_PRD,CD_MDLD,CD_PRF_DEPE_CDU,CD_PRF_DEPE_NEGR,CD_PRF_DEPE_EXEC,NR_UNCO_CTR_OPR,NR_EPRD_FNCD,NR_SCTR_OPR,NR_CTR_OPR,CD_EST_CDU,ToString(DT_EST_CDU, 'dd/MM/yyyy') as dt_est_cdu,CD_CNL_FRMZ_CTR,CD_IEC_MOE,CD_CIA_RSP_CTR;

SCTR_OPR = foreach CTR generate (long)$1#'NR_UNCO_CTR_OPR' AS NR_UNCO_CTR_OPR,(int)$1#'NR_EPRD_FNCD' AS NR_EPRD_FNCD,(int)$1#'NR_SCTR_OPR' AS NR_SCTR_OPR,(int)$1#'CD_PRD' AS CD_PRD
,(int)$1#'CD_MDLD' AS CD_MDLD,(int)$1#'CD_PSS_RSCO_OPRL' AS CD_PSS_RSCO_OPRL,(int)$1#'CD_EST_CDU_SCTR' AS CD_EST_CDU_SCTR,(datetime)$1#'DT_EST_CDU_SCTR' AS DT_EST_CDU_SCTR
,(int)$1#'NR_UNCO_FON_RCS' AS NR_UNCO_FON_RCS,(datetime)$1#'DT_FRMZ_SCTR' AS DT_FRMZ_SCTR,(datetime)$1#'DT_VNCT_SCTR' AS DT_VNCT_SCTR,(int)$1#'CD_RBC_CTB' AS CD_RBC_CTB
,(int)$1#'CD_FMA_TAXA_TRNS' AS CD_FMA_TAXA_TRNS,(bigdecimal)$1#'PC_TAXA_TRNS_FNCR' AS PC_TAXA_TRNS_FNCR,(int)$1#'CD_TAXA_JUR_NMLD' AS CD_TAXA_JUR_NMLD
,(bigdecimal)$1#'PC_TAXA_JUR_NMLD' AS PC_TAXA_JUR_NMLD,(int)$1#'CD_IEC_CM_INDP' AS CD_IEC_CM_INDP,(bigdecimal)$1#'PC_APL_CM_INDP' AS PC_APL_CM_INDP
,(int)$1#'CD_TAXA_JUR_INDP' AS CD_TAXA_JUR_INDP,(bigdecimal)$1#'PC_TAXA_JUR_INDP' AS PC_TAXA_JUR_INDP,(int)$1#'CD_MTDL_CLC' AS CD_MTDL_CLC,(int)$1#'CD_IEC_CM' AS CD_IEC_CM
,(bigdecimal)$1#'PC_APL_CM' AS PC_APL_CM,(int)$1#'CD_IEC_TAXA_TRNS' AS CD_IEC_TAXA_TRNS,(bigdecimal)$1#'PC_IEC_TAXA_TRNS' AS PC_IEC_TAXA_TRNS
,(datetime)$1#'DT_PRMO_CRNG_ABTO' AS DT_PRMO_CRNG_ABTO,(bigdecimal)$1#'PC_PDA_EPRO' AS PC_PDA_EPRO,(int)$1#'QT_PZ_CTRD' AS QT_PZ_CTRD;

--RESTRINGE SOMENTE OS SUB CONTRATOS ATIVOS NO OPR (ESTADO DE CONDUÇÃO = 1)
SCTR_OPR = FILTER SCTR_OPR BY CD_EST_CDU_SCTR == 1;

SCTR_OPR = FOREACH SCTR_OPR GENERATE CD_PRD,CD_MDLD,NR_UNCO_CTR_OPR,NR_EPRD_FNCD,NR_SCTR_OPR,CD_PSS_RSCO_OPRL,ToString(DT_EST_CDU_SCTR, 'dd/MM/yyyy') AS dt_est_cdu_sctr
,CD_EST_CDU_SCTR,NR_UNCO_FON_RCS,ToString(DT_FRMZ_SCTR, 'dd/MM/yyyy') AS dt_frmz_sctr,ToString(DT_VNCT_SCTR, 'dd/MM/yyyy') AS dt_vnct_sctr,CD_RBC_CTB,CD_TAXA_JUR_NMLD
,PC_TAXA_JUR_NMLD,CD_IEC_CM_INDP,PC_APL_CM_INDP,CD_TAXA_JUR_INDP,PC_TAXA_JUR_INDP,CD_MTDL_CLC,CD_IEC_CM,PC_APL_CM,CD_IEC_TAXA_TRNS,PC_IEC_TAXA_TRNS
,ToString(DT_PRMO_CRNG_ABTO, 'dd/MM/yyyy') AS dt_prmo_crng_abto,CD_FMA_TAXA_TRNS,PC_TAXA_TRNS_FNCR,PC_PDA_EPRO,QT_PZ_CTRD;

--JUNÇÃO DA INFORMAÇÃO DE CONTRATO COM SUBCONTRATO POR NR_UNCO, PRD, MDLD
A = JOIN CTR_OPR BY (NR_UNCO_CTR_OPR, NR_EPRD_FNCD, NR_SCTR_OPR), SCTR_OPR BY (NR_UNCO_CTR_OPR, NR_EPRD_FNCD, NR_SCTR_OPR); 

OPER = FOREACH A GENERATE $0,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42;

SALDO_OPR = foreach CTR generate (bigdecimal)$2#'VL_CRC_CPTZ_SCTR' AS VL_CRC_CPTZ_SCTR,(bigdecimal)$2#'VL_SDO_SCTR_USD' AS VL_SDO_SCTR_USD,(int)$2#'NR_EPRD_FNCD' AS NR_EPRD_FNCD
,(bigdecimal)$2#'VL_CMB_MOEE' AS VL_CMB_MOEE,(bigdecimal)$2#'VL_ACSR_APPR_SCTR' AS VL_ACSR_APPR_SCTR,(datetime)$2#'DT_SDO_SCTR' AS DT_SDO_SCTR,(bigdecimal)$2#'VL_SDO_CRC' AS VL_SDO_CRC
,(bigdecimal)$2#'VL_TRND_PRJZ' AS VL_TRND_PRJZ,(bigdecimal)$2#'VL_CMB_MOEN' AS VL_CMB_MOEN,(bigdecimal)$2#'VL_REN_APPR_USD' AS VL_REN_APPR_USD,(bigdecimal)$2#'VL_SDO_CPTL' AS VL_SDO_CPTL
,(int)$2#'NR_SCTR_OPR' AS NR_SCTR_OPR,(bigdecimal)$2#'VL_SDO_SCTR' AS VL_SDO_SCTR,(int)$2#'CD_TIP_SDO' AS CD_TIP_SDO,(bigdecimal)$2#'VL_JUR_CPTZ_SCTR' AS VL_JUR_CPTZ_SCTR
,(bigdecimal)$2#'VL_ENCG_CPTZ_SCTR' AS VL_ENCG_CPTZ_SCTR,(datetime)$2#'DT_VL_ATU_SCTR' AS DT_VL_ATU_SCTR,(datetime)$2#'DT_TRNS_PRJZ' AS DT_TRNS_PRJZ
,(long)$2#'NR_UNCO_CTR_OPR' AS NR_UNCO_CTR_OPR,(bigdecimal)$2#'VL_SDO_MOEE' AS VL_SDO_MOEE,(bigdecimal)$2#'VL_SDO_ACSR' AS VL_SDO_ACSR,(bigdecimal)$2#'VL_JUR_APPR_SCTR' AS VL_JUR_APPR_SCTR
,(bigdecimal)$2#'VL_REN_APPR_SCTR' AS VL_REN_APPR_SCTR,(bigdecimal)$2#'VL_CRC_APPR_SCTR' AS VL_CRC_APPR_SCTR,(bigdecimal)$2#'VL_ATU_SCTR' AS VL_ATU_SCTR
,(bigdecimal)$2#'VL_ATR_SCTR' AS VL_ATR_SCTR;

SALDO_OPR = FOREACH SALDO_OPR GENERATE NR_UNCO_CTR_OPR,NR_EPRD_FNCD,NR_SCTR_OPR,ToString(DT_SDO_SCTR, 'dd/MM/yyyy') as dt_sdo_sctr,VL_SDO_SCTR,VL_REN_APPR_SCTR,VL_ENCG_CPTZ_SCTR
,VL_CRC_CPTZ_SCTR,VL_SDO_SCTR_USD,VL_CMB_MOEE,VL_ACSR_APPR_SCTR,VL_SDO_CRC,VL_TRND_PRJZ,VL_CMB_MOEN,VL_REN_APPR_USD,VL_SDO_CPTL,CD_TIP_SDO,VL_JUR_CPTZ_SCTR
,ToString(DT_VL_ATU_SCTR, 'dd/MM/yyyy') as dt_vl_atu_sctr,ToString(DT_TRNS_PRJZ, 'dd/MM/yyyy') as dt_trns_prjz,VL_SDO_MOEE,VL_SDO_ACSR,VL_JUR_APPR_SCTR,VL_CRC_APPR_SCTR,VL_ATU_SCTR
,VL_ATR_SCTR,(bigdecimal)((VL_SDO_SCTR - VL_REN_APPR_SCTR) + VL_ENCG_CPTZ_SCTR) AS saldo;

--JUNÇÃO DA TABELA ANTERIORMENTE CRIADA COM CONTRATO + SUBCONTRATO COM A TABELA DE SALDO, POR NR_UNCO, EMPREENDIMENTO E SUBCONTRATO (GERANDO O VALOR DO SALDO = SD - RENDAS + ENCARGOS)
B = JOIN OPER BY (NR_UNCO_CTR_OPR, NR_EPRD_FNCD, NR_SCTR_OPR), SALDO_OPR BY (NR_UNCO_CTR_OPR, NR_EPRD_FNCD, NR_SCTR_OPR);

OPER = FOREACH B GENERATE $0 AS sg_sis_rsp_cdu,$1 AS cd_prd,$2 AS cd_mdld,$3 AS cd_prf_depe_cdu,$4 AS cd_prd_depe_negr,$5 AS cd_prf_depe_exec,$6 AS nr_unco_ctr_opr,$7 AS nr_eprd_fncd
,$8 AS nr_sctr_opr,$9 AS nr_ctr_opr,$10 AS cd_est_cdu,ToDate($11, 'dd/mm/yyyy') AS dt_est_cdu,$12 AS cd_cnl_frmz_ctr,$13 AS cd_iec_moe,$14 AS cd_cia_rsp_ctr,$15 AS cd_pss_rsco_oprl
,ToDate($16, 'dd/mm/yyyy') AS dt_est_cdu_sctr,$17 AS cd_est_cdu_sctr,$18 AS nr_unco_fon_rcs,ToDate($19, 'dd/mm/yyyy') AS dt_frmz_sctr,ToDate($20, 'dd/mm/yyyy') AS dt_vnct_sctr
,$21 AS cd_rbc_ctb,$22 AS cd_taxa_jur_nmld,$23 AS pc_taxa_jur_nmld,$24 AS cd_iec_cm_indp,$25 AS pc_apl_cm_indp,$26 AS cd_taxa_jur_jndp,$27 AS pc_taxa_jur_indp,$28 AS cd_mtdl_clc
,$29 AS cd_iec_cm,$20 AS pc_apl_cm,$31 AS cd_iec_taxa_trns,$32 AS pc_iec_taxa_trns,ToDate($33, 'dd/mm/yyyy') AS dt_prmo_crng_abto,$34 AS cd_fma_taxa_trns
,$35 AS cd_taxa_trns_fncr,$36 AS pc_pda_epro,$37 AS qt_pz_ctrd,ToDate($41, 'dd/mm/yyyy')  AS dt_sdo_sctr,$42 AS vl_sdo_sctr,$43 AS vl_ren_appr_sctr,$44 AS vl_encg_cptz_sctr
,$45 AS vl_crc_cptz_sctr,$46 AS vl_sdo_sctr_usd,$47 AS vl_cmb_moee,$48 AS vl_acsr_appr_sctr,$49 AS vl_sdo_crc,$50 AS vl_trnd_prjz,$51 AS vl_cmb_moen,$52 AS vl_ren_appr_usd
,$53 AS vl_sdo_cptl,$54 AS cd_tip_sdo,$55 AS vl_jur_cptz_sctr,ToDate($56, 'dd/mm/yyyy')  AS dt_vl_atu_sctr,ToDate($57, 'dd/mm/yyyy')  AS dt_trns_prjz,$58 AS vl_sdo_moee
,$59 AS vl_sdo_acsr,$60 AS vl_jur_appr_sctr,$61 AS vl_crc_appr_sctr,$62 AS vl_atu_sctr,$63 AS vl_atr_sctr,$64 AS saldo;

MSD = FOREACH OPER GENERATE *, FLATTEN(mtdl_msd.msd(saldo)) as (msd, dt_processamento, aa_ref, mm_ref, dt_mvt, dc, dc_mes);

RST = FOREACH MSD GENERATE *, FLATTEN(mtdl_eaopt.eaopt(dc, dc_mes, cd_fma_taxa_trns, cd_taxa_trns_fncr, saldo)) as (eaopt);

RST = FOREACH RST GENERATE sg_sis_rsp_cdu,cd_prd,cd_mdld,cd_prf_depe_cdu,cd_prd_depe_negr,cd_prf_depe_exec,nr_unco_ctr_opr,nr_eprd_fncd,nr_sctr_opr,nr_ctr_opr,cd_est_cdu
,dt_est_cdu,cd_cnl_frmz_ctr,cd_iec_moe,cd_cia_rsp_ctr,cd_pss_rsco_oprl,dt_est_cdu_sctr,cd_est_cdu_sctr,nr_unco_fon_rcs,dt_frmz_sctr,dt_vnct_sctr,cd_rbc_ctb,cd_taxa_jur_nmld
,pc_taxa_jur_nmld,cd_iec_cm_indp,pc_apl_cm_indp,cd_taxa_jur_jndp,pc_taxa_jur_indp,cd_mtdl_clc,cd_iec_cm,pc_apl_cm,cd_iec_taxa_trns,pc_iec_taxa_trns,dt_prmo_crng_abto
,cd_fma_taxa_trns,cd_taxa_trns_fncr,pc_pda_epro,qt_pz_ctrd,dt_sdo_sctr,vl_sdo_sctr,vl_ren_appr_sctr,vl_encg_cptz_sctr,vl_crc_cptz_sctr,vl_sdo_sctr_usd,vl_cmb_moee,vl_acsr_appr_sctr
,vl_sdo_crc,vl_trnd_prjz,vl_cmb_moen,vl_ren_appr_usd,vl_sdo_cptl,cd_tip_sdo,vl_jur_cptz_sctr,dt_vl_atu_sctr,dt_trns_prjz,vl_sdo_moee,vl_sdo_acsr,vl_jur_appr_sctr,vl_crc_appr_sctr
,vl_atu_sctr,vl_atr_sctr,saldo,msd,ToDate(dt_processamento, 'dd/MM/yyyy') as dt_processamento,aa_ref,mm_ref,ToDate(dt_mvt, 'dd/MM/yyyy') as dt_mvt,dc,dc_mes,eaopt;

STORE RST INTO 'dirco_gecig.rst507' USING org.apache.hive.hcatalog.pig.HCatStorer('ano = aa_ref, mes = mm_ref');
