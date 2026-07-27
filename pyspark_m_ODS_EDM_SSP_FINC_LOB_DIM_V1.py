# Mapping: m_ODS_EDM_SSP_FINC_LOB_DIM_V1
# Generated from Informatica Knowledge Graph — PySpark
# Parameters: --schema, --target-schema
#
# Source: SSP_STG.SSP_STG_OSF_L2_CUST_MSTR
# Target: SSP_FINC_LOB_DIM_V1

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType,
    DecimalType, DateType, TimestampType, StructType, StructField,
)


def main():
    parser = argparse.ArgumentParser(description="m_ODS_EDM_SSP_FINC_LOB_DIM_V1")
    parser.add_argument("--schema", required=True, help="Source schema name")
    parser.add_argument("--target-schema", required=True, help="Target schema name")
    args = parser.parse_args()
    params = vars(args)

    spark = (
        SparkSession.builder
        .appName("m_ODS_EDM_SSP_FINC_LOB_DIM_V1")
        .enableHiveSupport()
        .getOrCreate()
    )

    # --- Source reads ---
    # Only FNCL_LOB_ID flows downstream; source_filter applied, select_distinct=true
    df_SQ_SSP_STG_OSF_L2_CUST_MSTR = (
        spark.read.table(f"{params['schema']}.SSP_STG_OSF_L2_CUST_MSTR")
        .select("FNCL_LOB_ID")
        .filter("FNCL_LOB_ID IS NOT NULL")
        .distinct()
    )

    # --- Transformation pipeline ---

    # Expression: EXP_SSP_STG_OSF_L2_CUST_MSTR
    df_EXP_SSP_STG_OSF_L2_CUST_MSTR = (
        df_SQ_SSP_STG_OSF_L2_CUST_MSTR
        .withColumn("IsCurrent",          F.lit(1))
        .withColumn("SRCSYSID",           F.lit(1))
        .withColumn("FINC_LOB_ARBR_CD",   F.lit("ARBOR"))
        .withColumn("FINC_LOB_NM",        F.lit("UNKNOWN"))
        .withColumn("FINC_LOB_BUS_NM",    F.lit("UNKNOWN"))
        .withColumn("FINC_LOB_BUS_NM_UAT",F.lit("UNKNOWN"))
        .withColumn("FINC_LOB_HRCHY_ID",  F.lit("UNKNOWN"))
        .withColumn("ACTV_DT",            F.current_timestamp())
        .withColumn("INACTV_DT",          F.lit("9999-12-31").cast(DateType()))
        .withColumn("IS_ACTV_IND",        F.lit("Y"))
        .withColumn("DIM_SRC_CD",         F.lit("ODS"))
        .withColumn("DIM_LAST_MDFD_DT",   F.current_timestamp())
        .withColumn("FINC_LOB_HRCHY_LVL", F.lit("UNKNOWN"))
    )

    # Lookup: LKP_SSP_FINC_LOB_DIM_V1 — check if FNCL_LOB_ID already exists as FINC_LOB_CD in target
    df_lkp_SSP_FINC_LOB_DIM_V1 = spark.read.table(
        f"{params['target_schema']}.SSP_FINC_LOB_DIM_V1"
    ).select(
        F.col("FINC_LOB_CD").alias("lkp_FINC_LOB_CD"),
        F.col("SSP_FINC_LOB_DIM_Key"),
    )

    df_LKP_SSP_FINC_LOB_DIM_V1 = (
        df_EXP_SSP_STG_OSF_L2_CUST_MSTR
        .join(
            df_lkp_SSP_FINC_LOB_DIM_V1,
            on=F.col("FNCL_LOB_ID") == F.col("lkp_FINC_LOB_CD"),
            how="left",
        )
        .drop("lkp_FINC_LOB_CD")
        .withColumnRenamed("SSP_FINC_LOB_DIM_Key", "SSP_FINC_LOB_CD_DIM_Key")
    )

    # Router: RTR_INSERT_UPDATE
    # INSGRP: ISNULL(SSP_FINC_LOB_CD_DIM_Key) — new records not yet in target
    df_RTR_INSGRP_RTR_INSERT_UPDATE = df_LKP_SSP_FINC_LOB_DIM_V1.filter(
        F.col("SSP_FINC_LOB_CD_DIM_Key").isNull()
    )

    # TODO: UPDGRP condition IIF(ISNULL(SSP_FINC_LOB_CD_DIM_Key),TRUE,FALSE) also resolves to
    # TRUE when key IS NULL — same as INSGRP. Likely intended as NOT ISNULL; translated faithfully,
    # manual review required.
    df_RTR_UPDGRP_RTR_INSERT_UPDATE = df_LKP_SSP_FINC_LOB_DIM_V1.filter(
        F.col("SSP_FINC_LOB_CD_DIM_Key").isNull()
    )

    # DEFAULT1: rows not matched by INSGRP or UPDGRP (empty in practice)
    df_RTR_DEFAULT1_RTR_INSERT_UPDATE = df_LKP_SSP_FINC_LOB_DIM_V1.filter(
        ~(F.col("SSP_FINC_LOB_CD_DIM_Key").isNull() | F.col("SSP_FINC_LOB_CD_DIM_Key").isNotNull())
    )

    # --- Write to target ---

    # INSERT path: INSGRP → SSP_FINC_LOB_DIM_V1 (append new rows)
    # SSP_FINC_LOB_DIM_Key excluded — auto-generated identity on target
    # FINC_LOB_HRCHY_LVL1/2/3 all derive from single FINC_LOB_HRCHY_LVL per field lineage
    df_insert = df_RTR_INSGRP_RTR_INSERT_UPDATE.select(
        F.col("IsCurrent"),
        F.col("SRCSYSID").alias("SourceSystemID"),
        F.col("FNCL_LOB_ID").alias("FINC_LOB_CD"),
        F.col("FINC_LOB_ARBR_CD"),
        F.col("FINC_LOB_NM"),
        F.col("FINC_LOB_BUS_NM"),
        F.col("FINC_LOB_BUS_NM_UAT"),
        F.col("FINC_LOB_HRCHY_ID"),
        F.col("ACTV_DT"),
        F.col("INACTV_DT"),
        F.col("IS_ACTV_IND"),
        F.col("DIM_SRC_CD"),
        F.col("DIM_LAST_MDFD_DT"),
        F.col("FINC_LOB_HRCHY_LVL").alias("FINC_LOB_HRCHY_LVL1"),
        F.col("FINC_LOB_HRCHY_LVL").alias("FINC_LOB_HRCHY_LVL2"),
        F.col("FINC_LOB_HRCHY_LVL").alias("FINC_LOB_HRCHY_LVL3"),
    )
    df_insert.write.mode("append").saveAsTable(
        f"{params['target_schema']}.SSP_FINC_LOB_DIM_V1"
    )

    # UPDATE path: UPDGRP → UPDT_FINC_LOB_DIM_V1 (DD_UPDATE) → SSP_FINC_LOB_DIM_V1 via MERGE
    # Join key: tgt.SSP_FINC_LOB_DIM_Key = src.SSP_FINC_LOB_CD_DIM_Key
    df_RTR_UPDGRP_RTR_INSERT_UPDATE.createOrReplaceTempView(
        "src_m_ODS_EDM_SSP_FINC_LOB_DIM_V1"
    )
    spark.sql(f"""
        MERGE INTO {params['target_schema']}.SSP_FINC_LOB_DIM_V1 AS tgt
        USING src_m_ODS_EDM_SSP_FINC_LOB_DIM_V1 AS src
        ON tgt.SSP_FINC_LOB_DIM_Key = src.SSP_FINC_LOB_CD_DIM_Key
        WHEN MATCHED THEN UPDATE SET
            tgt.IsCurrent           = src.IsCurrent,
            tgt.SourceSystemID      = src.SRCSYSID,
            tgt.FINC_LOB_ARBR_CD    = src.FINC_LOB_ARBR_CD,
            tgt.FINC_LOB_NM         = src.FINC_LOB_NM,
            tgt.FINC_LOB_BUS_NM     = src.FINC_LOB_BUS_NM,
            tgt.FINC_LOB_BUS_NM_UAT = src.FINC_LOB_BUS_NM_UAT,
            tgt.FINC_LOB_HRCHY_ID   = src.FINC_LOB_HRCHY_ID,
            tgt.ACTV_DT             = src.ACTV_DT,
            tgt.INACTV_DT           = src.INACTV_DT,
            tgt.IS_ACTV_IND         = src.IS_ACTV_IND,
            tgt.DIM_SRC_CD          = src.DIM_SRC_CD,
            tgt.DIM_LAST_MDFD_DT    = src.DIM_LAST_MDFD_DT,
            tgt.FINC_LOB_HRCHY_LVL1 = src.FINC_LOB_HRCHY_LVL,
            tgt.FINC_LOB_HRCHY_LVL2 = src.FINC_LOB_HRCHY_LVL,
            tgt.FINC_LOB_HRCHY_LVL3 = src.FINC_LOB_HRCHY_LVL
    """)


if __name__ == "__main__":
    main()
