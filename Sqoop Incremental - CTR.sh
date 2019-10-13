sqoop import -Dhadoop.security.credential.provider.path=jceks://hdfs/user/bigrco04/db2.db2mis01.jceks \
--connect jdbc:db2://bdb2p04.plexbsb.bb.com.br:50446/BDB2P04 \
--username db2mis01 -password-alias db2.db2mis01.alias \
--query "SELECT CHAR(DIGITS(T1.NR_UNCO_CTR_OPR) || DIGITS(T2.NR_EPRD_FNCD) || DIGITS(T2.NR_SCTR_OPR)) AS ROW_ID, \
T1.NR_UNCO_CTR_OPR, T1.SG_SIS_RSP_CDU, T1.CD_PRF_DEPE_CDU, T1.NR_CTR_OPR, T1.CD_EST_CDU, T1.DT_EST_CDU, T1.CD_CNL_FRMZ_CTR, T1.CD_PRF_DEPE_NEGR, \
T1.CD_PRF_DEPE_EXEC, T1.CD_PRD, T1.CD_MDLD, T1.CD_IEC_MOE, T1.CD_CIA_RSP_CTR \
FROM DB2OPR.CTR_OPR T1, DB2OPR.SCTR_OPR T2, DB2MIS.IADD_MAPA_DT T3 \
WHERE (t1.NR_UNCO_CTR_OPR = t2.NR_UNCO_CTR_OPR) AND DATA_REF = CURRENT_DATE AND T1.DT_EST_CDU >= T3.DATA_ANT_1 and \$CONDITIONS" \
--hbase-table 'dirco_gecig:rst_entrada' --column-family 'ctr_opr' \
--hbase-row-key ROW_ID \
--split-by T1.NR_UNCO_CTR_OPR -m 128
