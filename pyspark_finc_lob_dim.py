# Mapping: m_EDM_DW_SSP_FINC_LOB_DIM
# Generated from Informatica Knowledge Graph — PySpark
# Parameters: --schema, --target-schema
#
# Source: EDM.SSP_FINC_LOB_DIM_V1
# Target: SSP_FINC_LOB_DIM

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType,
    DecimalType, DateType, TimestampType, StructType, StructField,
)


def main():
    parser = argparse.ArgumentParser(description="m_EDM_DW_SSP_FINC_LOB_DIM")
    parser.add_argument("--schema", required=True, help="Source schema name")
    parser.add_argument("--target-schema", required=True, help="Target schema name")
    args = parser.parse_args()
    params = vars(args)

    spark = (
        SparkSession.builder
        .appName("m_EDM_DW_SSP_FINC_LOB_DIM")
        .enableHiveSupport()
        .getOrCreate()
    )

    # --- Source reads ---
    df_SQ_SSP_FINC_LOB_DIM_V1 = spark.read.table(
        f"{params['schema']}.SSP_FINC_LOB_DIM_V1"
    ).select(
        "SSP_FINC_LOB_DIM_Key", "StartDate", "EndDate", "IsCurrent",
        "SourceSystemKey", "SourceSystemID", "FINC_LOB_CD", "FINC_LOB_ARBR_CD",
        "FINC_LOB_NM", "FINC_LOB_BUS_NM", "FINC_LOB_BUS_NM_UAT",
        "FINC_LOB_HRCHY_ID", "ACTV_DT", "INACTV_DT", "IS_ACTV_IND",
        "DIM_SRC_CD", "DIM_LAST_MDFD_DT", "FINC_LOB_HRCHY_LVL1",
        "FINC_LOB_HRCHY_LVL2", "FINC_LOB_HRCHY_LVL3",
    )

    # --- Transformation pipeline ---

    # Lookup: LKP_SSP_FINC_LOB_DIM — left join on FINC_LOB_CD to retrieve existing FINC_LOB_CD_SK
    df_lkp_SSP_FINC_LOB_DIM = spark.read.table(
        f"{params['target_schema']}.SSP_FINC_LOB_DIM"
    ).select(
        F.col("FINC_LOB_CD").alias("lkp_FINC_LOB_CD"),
        F.col("FINC_LOB_CD_SK"),
    )

    df_LKP_SSP_FINC_LOB_DIM = df_SQ_SSP_FINC_LOB_DIM_V1.join(
        df_lkp_SSP_FINC_LOB_DIM,
        on=F.col("FINC_LOB_CD") == F.col("lkp_FINC_LOB_CD"),
        how="left",
    ).drop("lkp_FINC_LOB_CD")

    # Router: RTR_INSERT_UPDATE
    # INSGRP: ISNULL(FINC_LOB_CD_SK) — new records not yet in target
    df_RTR_INSGRP_RTR_INSERT_UPDATE = df_LKP_SSP_FINC_LOB_DIM.filter(
        F.col("FINC_LOB_CD_SK").isNull()
    )

    # UPDGRP: NOT ISNULL(FINC_LOB_CD_SK) — existing records to update
    df_RTR_UPDGRP_RTR_INSERT_UPDATE = df_LKP_SSP_FINC_LOB_DIM.filter(
        F.col("FINC_LOB_CD_SK").isNotNull()
    )

    # DEFAULT1: rows not matched by INSGRP or UPDGRP (empty in practice — all rows are NULL or NOT NULL)
    df_RTR_DEFAULT1_RTR_INSERT_UPDATE = df_LKP_SSP_FINC_LOB_DIM.filter(
        ~(F.col("FINC_LOB_CD_SK").isNull() | F.col("FINC_LOB_CD_SK").isNotNull())
    )

    # --- Write to target ---

    # INSERT path: INSGRP → SSP_FINC_LOB_DIM (append new rows)
    # SSP_FINC_LOB_DIM_Key from source is mapped to FINC_LOB_CD_SK per field lineage
    df_insert = df_RTR_INSGRP_RTR_INSERT_UPDATE.select(
        F.col("SSP_FINC_LOB_DIM_Key").cast(DecimalType(12, 0)).alias("FINC_LOB_CD_SK"),
        F.col("FINC_LOB_CD"),
        F.col("FINC_LOB_ARBR_CD"),
        F.col("FINC_LOB_NM"),
        F.col("FINC_LOB_BUS_NM"),
        F.col("FINC_LOB_BUS_NM_UAT"),
        F.col("FINC_LOB_HRCHY_ID"),
        F.col("ACTV_DT").cast(TimestampType()),
        F.col("INACTV_DT").cast(TimestampType()),
        F.col("IS_ACTV_IND"),
        F.col("DIM_SRC_CD"),
        F.col("DIM_LAST_MDFD_DT").cast(TimestampType()),
        F.col("FINC_LOB_HRCHY_LVL1"),
        F.col("FINC_LOB_HRCHY_LVL2"),
        F.col("FINC_LOB_HRCHY_LVL3"),
    )
    df_insert.write.mode("append").saveAsTable(
        f"{params['target_schema']}.SSP_FINC_LOB_DIM"
    )

    # UPDATE path: UPDGRP → UPD_UPDATE (DD_UPDATE) → SSP_FINC_LOB_DIM via MERGE
    df_RTR_UPDGRP_RTR_INSERT_UPDATE.createOrReplaceTempView(
        "src_m_EDM_DW_SSP_FINC_LOB_DIM"
    )
    spark.sql(f"""
        MERGE INTO {params['target_schema']}.SSP_FINC_LOB_DIM AS tgt
        USING src_m_EDM_DW_SSP_FINC_LOB_DIM AS src
        ON tgt.FINC_LOB_CD_SK = src.FINC_LOB_CD_SK
        WHEN MATCHED THEN UPDATE SET
            tgt.FINC_LOB_CD          = src.FINC_LOB_CD,
            tgt.FINC_LOB_ARBR_CD     = src.FINC_LOB_ARBR_CD,
            tgt.FINC_LOB_NM          = src.FINC_LOB_NM,
            tgt.FINC_LOB_BUS_NM      = src.FINC_LOB_BUS_NM,
            tgt.FINC_LOB_BUS_NM_UAT  = src.FINC_LOB_BUS_NM_UAT,
            tgt.FINC_LOB_HRCHY_ID    = src.FINC_LOB_HRCHY_ID,
            tgt.ACTV_DT              = src.ACTV_DT,
            tgt.INACTV_DT            = src.INACTV_DT,
            tgt.IS_ACTV_IND          = src.IS_ACTV_IND,
            tgt.DIM_SRC_CD           = src.DIM_SRC_CD,
            tgt.DIM_LAST_MDFD_DT     = src.DIM_LAST_MDFD_DT,
            tgt.FINC_LOB_HRCHY_LVL1  = src.FINC_LOB_HRCHY_LVL1,
            tgt.FINC_LOB_HRCHY_LVL2  = src.FINC_LOB_HRCHY_LVL2,
            tgt.FINC_LOB_HRCHY_LVL3  = src.FINC_LOB_HRCHY_LVL3
    """)


if __name__ == "__main__":
    main()
