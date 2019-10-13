sqoop import -Dhadoop.security.credential.provider.path=jceks://hdfs/user/bigrco04/db2.db2mis01.jceks \
--connect jdbc:db2://bdb2p04.plexbsb.bb.com.br:50446/BDB2P04 \
--username db2mis01 -password-alias db2.db2mis01.alias \
--query "SELECT CHAR(DIGITS(NR_UNCO_CTR_OPR) || DIGITS(NR_EPRD_FNCD) || DIGITS(NR_SCTR_OPR)) AS ROW_ID, \
NR_UNCO_CTR_OPR, NR_EPRD_FNCD, NR_SCTR_OPR, T3.DATA_ANT_1 AS DT_BASE, VL_SDO_SCTR, VL_ENCG_CPTZ_SCTR, VL_REN_APPR_SCTR, \
VL_SDO_MOEE, VL_ATR_SCTR, VL_ATU_SCTR, VL_TRND_PRJZ, VL_CMB_MOEN, VL_CMB_MOEE, VL_SDO_CPTL, VL_SDO_JUR, \
VL_SDO_CRC, VL_SDO_ACSR, DT_SDO_SCTR, DT_VL_ATU_SCTR, VL_JUR_CPTZ_SCTR, VL_CRC_CPTZ_SCTR, VL_JUR_APPR_SCTR, \
VL_CRC_APPR_SCTR, VL_ACSR_APPR_SCTR, DT_TRNS_PRJZ, VL_SDO_SCTR_USD, VL_REN_APPR_USD, CD_TIP_SDO \
FROM db2opr.sdo_sctr_opr, DB2MIS.IADD_MAPA_DT T3 WHERE DATA_REF = CURRENT_DATE AND \$CONDITIONS" \
--hbase-table 'dirco_gecig:rst_entrada' --column-family 'sd_sctr' --hbase-row-key ROW_ID \
--split-by NR_UNCO_CTR_OPR -m 128
