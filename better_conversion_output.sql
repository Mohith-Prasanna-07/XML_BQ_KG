-- Mapping: m_EDM_DW_SSP_FINC_LOB_DIM
-- Generated from Informatica Knowledge Graph
-- Parameters: none
--
-- Source:  EDM.dbo.SSP_FINC_LOB_DIM_V1  (Microsoft SQL Server)
-- Target:  SSP_FINC_LOB_DIM              (BigQuery via Teradata target)
--
-- Logic:
--   1. Read all rows from SSP_FINC_LOB_DIM_V1.
--   2. Lookup existing surrogate key (FINC_LOB_CD_SK) in target by FINC_LOB_CD.
--   3. Router INSGRP  (FINC_LOB_CD_SK IS NULL)         → WHEN NOT MATCHED → INSERT
--      Router UPDGRP  (FINC_LOB_CD_SK IS NOT NULL)     → WHEN MATCHED     → UPDATE
--      Router DEFAULT1 (catch-all — no rows expected;
--                       INSGRP + UPDGRP are exhaustive) → no-op, see TODO below

WITH cte_SQ_SSP_FINC_LOB_DIM_V1 AS (
    -- Source Qualifier: no sql_override, no source_filter, no DISTINCT
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
    -- Lookup Procedure: LEFT JOIN target table to resolve existing surrogate key
    -- Condition: target.FINC_LOB_CD = source.FINC_LOB_CD
    SELECT
        sq.SSP_FINC_LOB_DIM_Key,
        sq.StartDate,
        sq.EndDate,
        sq.IsCurrent,
        sq.SourceSystemKey,
        sq.SourceSystemID,
        sq.FINC_LOB_CD,
        sq.FINC_LOB_ARBR_CD,
        sq.FINC_LOB_NM,
        sq.FINC_LOB_BUS_NM,
        sq.FINC_LOB_BUS_NM_UAT,
        sq.FINC_LOB_HRCHY_ID,
        sq.ACTV_DT,
        sq.INACTV_DT,
        sq.IS_ACTV_IND,
        sq.DIM_SRC_CD,
        sq.DIM_LAST_MDFD_DT,
        sq.FINC_LOB_HRCHY_LVL1,
        sq.FINC_LOB_HRCHY_LVL2,
        sq.FINC_LOB_HRCHY_LVL3,
        -- LOOKUP/OUTPUT ports
        lkp.FINC_LOB_CD_SK          AS FINC_LOB_CD_SK   -- NULL when no match → new row
    FROM cte_SQ_SSP_FINC_LOB_DIM_V1 sq
    LEFT JOIN `@{PROJECT_ID}.@{DATASET}.SSP_FINC_LOB_DIM` lkp
        ON lkp.FINC_LOB_CD = sq.FINC_LOB_CD
)

-- Router RTR_INSERT_UPDATE + Update Strategy UPD_UPDATE (DD_UPDATE)
-- → collapsed into a single MERGE statement
--
-- INSGRP  filter : FINC_LOB_CD_SK IS NULL          → WHEN NOT MATCHED → INSERT
-- UPDGRP  filter : FINC_LOB_CD_SK IS NOT NULL       → WHEN MATCHED     → UPDATE SET
-- DEFAULT1 filter: catch-all (no rows reach here;
--                  INSGRP ∪ UPDGRP is exhaustive over IS NULL / IS NOT NULL)
-- TODO: DEFAULT1 group — verify no downstream target is connected; manual review required

MERGE `@{PROJECT_ID}.@{DATASET}.SSP_FINC_LOB_DIM` T
USING cte_LKP_SSP_FINC_LOB_DIM S
    ON T.FINC_LOB_CD = S.FINC_LOB_CD

-- ── INSGRP: new rows (lookup returned no match) ──────────────────────────────
WHEN NOT MATCHED
    -- Router condition: FINC_LOB_CD_SK IS NULL
    AND S.FINC_LOB_CD_SK IS NULL
THEN INSERT (
    FINC_LOB_CD_SK,
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
)
VALUES (
    -- FINC_LOB_CD_SK: no Sequence Generator in pipeline; use source natural key
    -- TODO: confirm surrogate key generation strategy — using SSP_FINC_LOB_DIM_Key as SK for inserts
    S.SSP_FINC_LOB_DIM_Key,
    S.FINC_LOB_CD,
    S.FINC_LOB_ARBR_CD,
    S.FINC_LOB_NM,
    S.FINC_LOB_BUS_NM,
    S.FINC_LOB_BUS_NM_UAT,
    S.FINC_LOB_HRCHY_ID,
    TIMESTAMP(S.ACTV_DT, 'America/New_York'),
    TIMESTAMP(S.INACTV_DT, 'America/New_York'),
    S.IS_ACTV_IND,
    S.DIM_SRC_CD,
    TIMESTAMP(S.DIM_LAST_MDFD_DT, 'America/New_York'),
    S.FINC_LOB_HRCHY_LVL1,
    S.FINC_LOB_HRCHY_LVL2,
    S.FINC_LOB_HRCHY_LVL3
)

-- ── UPDGRP → UPD_UPDATE (DD_UPDATE): existing rows ───────────────────────────
WHEN MATCHED
    -- Router condition: FINC_LOB_CD_SK IS NOT NULL
    AND S.FINC_LOB_CD_SK IS NOT NULL
THEN UPDATE SET
    -- UPD_UPDATE fields (INPUT/OUTPUT from UPDGRP via Update Strategy DD_UPDATE)
    -- Note: FINC_LOB_CD_SK (PK) is not updated; it is the match key
    T.FINC_LOB_CD           = S.FINC_LOB_CD,
    T.FINC_LOB_ARBR_CD      = S.FINC_LOB_ARBR_CD,
    T.FINC_LOB_NM           = S.FINC_LOB_NM,
    T.FINC_LOB_BUS_NM       = S.FINC_LOB_BUS_NM,
    T.FINC_LOB_BUS_NM_UAT   = S.FINC_LOB_BUS_NM_UAT,
    T.FINC_LOB_HRCHY_ID     = S.FINC_LOB_HRCHY_ID,
    T.ACTV_DT               = TIMESTAMP(S.ACTV_DT, 'America/New_York'),
    T.INACTV_DT             = TIMESTAMP(S.INACTV_DT, 'America/New_York'),
    T.IS_ACTV_IND           = S.IS_ACTV_IND,
    T.DIM_SRC_CD            = S.DIM_SRC_CD,
    T.DIM_LAST_MDFD_DT      = TIMESTAMP(S.DIM_LAST_MDFD_DT, 'America/New_York'),
    T.FINC_LOB_HRCHY_LVL1   = S.FINC_LOB_HRCHY_LVL1,
    T.FINC_LOB_HRCHY_LVL2   = S.FINC_LOB_HRCHY_LVL2,
    T.FINC_LOB_HRCHY_LVL3   = S.FINC_LOB_HRCHY_LVL3
;
-- TODO: StartDate, EndDate, IsCurrent, SourceSystemKey, SourceSystemID are present in
--       the UPD_UPDATE transformation but have no corresponding columns in the target
--       definition (SSP_FINC_LOB_DIM).  Verify whether these fields should be added to
--       the target or are intentionally excluded — manual review required.