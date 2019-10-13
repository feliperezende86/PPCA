sqoop import -Dhadoop.security.credential.provider.path=jceks://hdfs/user/bigrco04/db2.db2mis01.jceks \
--connect jdbc:db2://bdb2p04.plexbsb.bb.com.br:50446/BDB2P04 \
--username db2mis01 -password-alias db2.db2mis01.alias \
--query "SELECT CHAR(DIGITS(NR_UNCO_CTR_OPR) || DIGITS(NR_EPRD_FNCD) || DIGITS(NR_SCTR_OPR)) AS ROW_ID, \
NR_UNCO_CTR_OPR, NR_SCTR_OPR, NR_EPRD_FNCD, CD_RBC_CTB, CD_IEC_CM, PC_APL_CM, CD_IEC_CM_INDP, \
PC_APL_CM_INDP, CD_TAXA_JUR_NMLD, PC_TAXA_JUR_NMLD, CD_TAXA_JUR_INDP, PC_TAXA_JUR_INDP, CD_PRD, CD_MDLD, \
DT_FRMZ_SCTR, CD_MTDL_CLC, CD_PSS_RSCO_OPRL, CD_EST_CDU_SCTR, DT_EST_CDU_SCTR, DT_PRMO_CRNG_ABTO, DT_VNCT_SCTR, \
NR_UNCO_FON_RCS, CD_IEC_TAXA_TRNS, PC_IEC_TAXA_TRNS, CD_FMA_TAXA_TRNS, PC_PDA_EPRO, QT_PZ_CTRD \
FROM db2opr.sctr_opr WHERE \$CONDITIONS" \
--hbase-table 'dirco_gecig:rst506' --column-family 'sctr_opr' --hbase-row-key ROW_ID \
--split-by NR_UNCO_CTR_OPR -m 128
