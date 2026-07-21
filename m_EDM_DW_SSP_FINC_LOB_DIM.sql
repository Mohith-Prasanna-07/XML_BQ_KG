-- Mapping: m_EDM_DW_SSP_FINC_LOB_DIM
-- Generated from Informatica Knowledge Graph
-- Parameters: none
--
-- Source: SSP_FINC_LOB_DIM_V1 (Microsoft SQL Server, owner: EDM)
-- Target: SSP_FINC_LOB_DIM

WITH cte_SQ_SSP_FINC_LOB_DIM_V1 AS (
  -- Source Qualifier: SQ_SSP_FINC_LOB_DIM_V1
  -- No sql_override, no source_filter, no DISTINCT
  SELECT
    SSP_FINC_LOB_DIM_Key,
    StartDate,
    EndDate,
    IsCurrent,
    SourceSystemKey,
    SourceSystemID,
    FINC_LOB_CD,
    FINC_LOB_ARBR_CD,
    FINC_LOB_NM,
    FINC_LOB_BUS_NM,
    FINC_LOB_BUS_NM_UAT,
    FINC_LOB_HRCHY_ID,
    ACTV_DT,
    INACTV_DT,
    IS_ACTV_IND,
    DIM_SRC_CD,
    DIM_LAST_MDFD_DT,
    FINC_LOB_HRCHY_LVL1,
    FINC_LOB_HRCHY_LVL2,
    FINC_LOB_HRCHY_LVL3
  FROM `@{PROJECT_ID}.@{DATASET}.SSP_FINC_LOB_DIM_V1`
),

cte_LKP_SSP_FINC_LOB_DIM AS (
  -- Lookup Procedure: LKP_SSP_FINC_LOB_DIM
  -- LEFT JOIN on SSP_FINC_LOB_DIM WHERE lkp.FINC_LOB_CD = src.FINC_LOB_CD
  -- LOOKUP/OUTPUT ports: FINC_LOB_CD_SK, FINC_LOB_CD
  SELECT
    src.SSP_FINC_LOB_DIM_Key,
    src.StartDate,
    src.EndDate,
    src.IsCurrent,
    src.SourceSystemKey,
    src.SourceSystemID,
    src.FINC_LOB_CD,
    src.FINC_LOB_ARBR_CD,
    src.FINC_LOB_NM,
    src.FINC_LOB_BUS_NM,
    src.FINC_LOB_BUS_NM_UAT,
    src.FINC_LOB_HRCHY_ID,
    src.ACTV_DT,
    src.INACTV_DT,
    src.IS_ACTV_IND,
    src.DIM_SRC_CD,
    src.DIM_LAST_MDFD_DT,
    src.FINC_LOB_HRCHY_LVL1,
    src.FINC_LOB_HRCHY_LVL2,
    src.FINC_LOB_HRCHY_LVL3,
    lkp.FINC_LOB_CD_SK
  FROM cte_SQ_SSP_FINC_LOB_DIM_V1 src
  LEFT JOIN `@{PROJECT_ID}.@{DATASET}.SSP_FINC_LOB_DIM` lkp
    ON lkp.FINC_LOB_CD = src.FINC_LOB_CD
),

cte_RTR_INSERT_UPDATE_update AS (
  -- Router: RTR_INSERT_UPDATE — UPDATE group (suffix-3 output ports → UPD_UPDATE / DD_UPDATE)
  -- Condition: lookup matched → FINC_LOB_CD_SK IS NOT NULL
  SELECT
    FINC_LOB_CD_SK,
    SSP_FINC_LOB_DIM_Key,
    StartDate,
    EndDate,
    IsCurrent,
    SourceSystemKey,
    SourceSystemID,
    FINC_LOB_CD,
    FINC_LOB_ARBR_CD,
    FINC_LOB_NM,
    FINC_LOB_BUS_NM,
    FINC_LOB_BUS_NM_UAT,
    FINC_LOB_HRCHY_ID,
    ACTV_DT,
    INACTV_DT,
    IS_ACTV_IND,
    DIM_SRC_CD,
    DIM_LAST_MDFD_DT,
    FINC_LOB_HRCHY_LVL1,
    FINC_LOB_HRCHY_LVL2,
    FINC_LOB_HRCHY_LVL3
  FROM cte_LKP_SSP_FINC_LOB_DIM
  WHERE FINC_LOB_CD_SK IS NOT NULL
)

-- TODO: RTR_INSERT_UPDATE suffix-1 output group (INSERT branch, FINC_LOB_CD_SK IS NULL)
--       has no downstream UpdateStrategy/target in this subgraph — manual review required.
-- TODO: RTR_INSERT_UPDATE suffix-2 output group has no downstream connection in this subgraph
--       — manual review required.

-- Update Strategy: UPD_UPDATE (DD_UPDATE)
-- WHEN MATCHED → UPDATE all non-key dimension attributes
MERGE `@{PROJECT_ID}.@{DATASET}.SSP_FINC_LOB_DIM` T
USING cte_RTR_INSERT_UPDATE_update S
  ON T.FINC_LOB_CD_SK = S.FINC_LOB_CD_SK
WHEN MATCHED THEN UPDATE SET
  T.FINC_LOB_CD         = S.FINC_LOB_CD,
  T.FINC_LOB_ARBR_CD    = S.FINC_LOB_ARBR_CD,
  T.FINC_LOB_NM         = S.FINC_LOB_NM,
  T.FINC_LOB_BUS_NM     = S.FINC_LOB_BUS_NM,
  T.FINC_LOB_BUS_NM_UAT = S.FINC_LOB_BUS_NM_UAT,
  T.FINC_LOB_HRCHY_ID   = S.FINC_LOB_HRCHY_ID,
  T.ACTV_DT             = S.ACTV_DT,
  T.INACTV_DT           = S.INACTV_DT,
  T.IS_ACTV_IND         = S.IS_ACTV_IND,
  T.DIM_SRC_CD          = S.DIM_SRC_CD,
  T.DIM_LAST_MDFD_DT    = S.DIM_LAST_MDFD_DT,
  T.FINC_LOB_HRCHY_LVL1 = S.FINC_LOB_HRCHY_LVL1,
  T.FINC_LOB_HRCHY_LVL2 = S.FINC_LOB_HRCHY_LVL2,
  T.FINC_LOB_HRCHY_LVL3 = S.FINC_LOB_HRCHY_LVL3;
