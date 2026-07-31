"""
kg_extractor.py — Query Neo4j for a mapping or workflow subgraph and emit JSON
matching the input structure expected by prompts in prompts.py.

Usage:
    # Single mapping (existing)
    python kg_extractor.py --mapping M_MY_MAPPING --password <pw>
    python kg_extractor.py --mapping M_MY_MAPPING --folder MY_FOLDER --password <pw> --output subgraph.json

    # Entire workflow → multi-mapping JSON for pyspark-workflow target
    python kg_extractor.py --workflow W_MY_WORKFLOW --folder MY_FOLDER --password <pw> --output workflow.json

    # Two workflows combined into one multi-mapping JSON
    python kg_extractor.py --workflow W_WF1 W_WF2 --folder MY_FOLDER --password <pw> --output combined.json
"""

import argparse
import json
import sys
from collections import defaultdict, deque

try:
    from neo4j import GraphDatabase
    _NEO4J_AVAILABLE = True
except ImportError:
    _NEO4J_AVAILABLE = False

# ---------------------------------------------------------------------------
# Informatica property name → JSON key
# ---------------------------------------------------------------------------

_PROP_KEY = {
    "Sql Query":                    "sql_override",
    "SQL Query":                    "sql_override",
    "User Defined Join":            "sql_override",
    "Source Filter":                "source_filter",
    "Filter":                       "filter_condition",
    "Filter Condition":             "filter_condition",
    "Lookup table name":            "lookup_table",
    "Lookup Table Name":            "lookup_table",
    "Lookup Condition":             "lookup_condition",
    "Lookup condition":             "lookup_condition",
    "Update Strategy Expression":   "update_strategy_expr",
    "Update Strategy":              "update_strategy_expr",
    "Select Distinct":              "select_distinct",
}

# ---------------------------------------------------------------------------
# Context layer — Informatica → BigQuery translation reference
# ---------------------------------------------------------------------------

_FUNCTION_MAP_BQ = {
    "IIF":              "IF(cond, a, b)",
    "ISNULL":           "x IS NULL",
    "DECODE":           "CASE WHEN ... THEN ... ELSE ... END",
    "NVL":              "COALESCE(x, fallback)",
    "NVL2":             "IF(x IS NOT NULL, a, b)",
    "TO_DATE":          "DATE '9999-12-31'  (or PARSE_DATE)",
    "TO_CHAR":          "FORMAT_DATETIME(fmt, d)",
    "DATE_DIFF":        "TIMESTAMP_DIFF(a, b, MONTH)",
    "ADD_MONTHS":       "DATE_ADD(d, INTERVAL n MONTH)",
    "SESSSTARTTIME":    "CURRENT_TIMESTAMP()",
    "SYSDATE":          "CURRENT_DATETIME()",
    "LAST_DAY":         "DATE_SUB(DATE_TRUNC(DATE_ADD(d,INTERVAL 1 MONTH),MONTH),INTERVAL 1 DAY)",
    "TRUNC":            "TRUNC(x) or DATE_TRUNC(d, MONTH)",
    "INSTR":            "STRPOS(s, sub)",
    "SUBSTR":           "SUBSTR(s, start, length)",
    "LENGTH":           "LENGTH(x)",
    "LTRIM":            "LTRIM(x)",
    "RTRIM":            "RTRIM(x)",
    "TRIM":             "TRIM(x)",
    "UPPER":            "UPPER(x)",
    "LOWER":            "LOWER(x)",
    "LPAD":             "LPAD(s, len, pad)",
    "RPAD":             "RPAD(s, len, pad)",
    "CONCAT":           "CONCAT(a, b)",
    "MOD":              "MOD(a, b)",
    "ABS":              "ABS(x)",
    "GREATEST":         "GREATEST(a, b, ...)",
    "LEAST":            "LEAST(a, b, ...)",
    "ROUND":            "ROUND(x, d)",
    "FLOOR":            "FLOOR(x)",
    "CEIL":             "CEIL(x)",
    "POWER":            "POWER(b, e)",
    "SQRT":             "SQRT(x)",
    "SIGN":             "SIGN(x)",
    "REG_EXTRACT":      "REGEXP_EXTRACT(s, r)",
    "REG_REPLACE":      "REGEXP_REPLACE(s, r, repl)",
    "IN":               "x IN (a, b, ...)",
    "LIKE":             "x LIKE 'pattern'",
}

_DATATYPE_MAP_BQ = {
    "string":     "STRING",
    "nstring":    "STRING",
    "char":       "STRING",
    "nchar":      "STRING",
    "varchar":    "STRING",
    "nvarchar":   "STRING",
    "text":       "STRING",
    "integer":    "INT64",
    "int":        "INT64",
    "smallint":   "INT64",
    "bigint":     "INT64",
    "number":     "NUMERIC",
    "decimal":    "NUMERIC",
    "float":      "FLOAT64",
    "double":     "FLOAT64",
    "real":       "FLOAT64",
    "date/time":  "TIMESTAMP",
    "date":       "DATE",
    "time":       "TIME",
    "timestamp":  "TIMESTAMP",
    "binary":     "BYTES",
    "bit":        "BOOL",
}

_TRANSFORM_PATTERNS_BQ = {
    "Source Qualifier": {
        "cte_prefix": "cte_SQ_",
        "base_table": "`@{PROJECT_ID}.@{DATASET}.<table_name>`",
        "distinct_support": True,
        "filter_support": True,
        "sql_override_support": True,
    },
    "Expression": {
        "cte_prefix": "cte_EXP_",
        "output_ports": "computed SELECT aliases",
        "input_ports": "pass-through columns",
        "variable_ports": "intermediate CTE columns",
    },
    "Lookup Procedure": {
        "cte_prefix": "cte_LKP_",
        "join_type": "LEFT JOIN",
        "lookup_table": "`@{PROJECT_ID}.@{DATASET}.<lookup_table>`",
    },
    "Router": {
        "cte_prefix": "cte_RTR_",
        "generates_multiple_output_groups": True,
        "combine_with_UpdateStrategy": "MERGE statement",
    },
    "Update Strategy": {
        "cte_prefix": "cte_UPD_",
        "DD_INSERT": "INSERT INTO",
        "DD_UPDATE": "WHEN MATCHED THEN UPDATE SET",
        "DD_DELETE": "WHEN MATCHED THEN DELETE",
        "DD_REJECT": "-- TODO: rejected rows",
    },
    "Aggregator": {
        "cte_prefix": "cte_AGG_",
        "group_by_ports": "fields with expression_type=GROUPBY",
        "aggregate_ports": "fields with aggregate expressions",
    },
    "Filter": {
        "cte_prefix": "cte_FIL_",
        "adds_where_clause": True,
    },
    "Joiner": {
        "cte_prefix": "cte_JNR_",
        "master_side": "left / FROM",
        "detail_side": "right / JOIN",
    },
    "Sequence Generator": {
        "cte_prefix": "cte_SEQ_",
        "NEXTVAL": "ROW_NUMBER() OVER (ORDER BY (SELECT NULL))",
        "CURRVAL": "ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1",
    },
}

# ---------------------------------------------------------------------------
# Context layer — Informatica → PySpark translation reference
# ---------------------------------------------------------------------------

_FUNCTION_MAP_PYSPARK = {
    "IIF":              "F.when(F.expr('cond'), a).otherwise(b)",
    "ISNULL":           "F.col('x').isNull()",
    "DECODE":           "F.when(F.col('x')==v1, r1)...otherwise(default)",
    "NVL":              "F.coalesce(F.col('x'), F.lit(fallback))",
    "NVL2":             "F.when(F.col('x').isNotNull(), a).otherwise(b)",
    "TO_DATE":          "F.lit('9999-12-31').cast(DateType())",
    "TO_CHAR":          "F.date_format(F.col('d'), fmt)",
    "DATE_DIFF":        "F.months_between(F.col('a'), F.col('b'))",
    "ADD_MONTHS":       "F.add_months(F.col('d'), n)",
    "SESSSTARTTIME":    "F.current_timestamp()",
    "SYSDATE":          "F.current_date()",
    "LAST_DAY":         "F.last_day(F.col('d'))",
    "TRUNC":            "F.trunc(F.col('d'), 'MM')  or  F.floor(F.col('x'))",
    "INSTR":            "F.locate(sub, F.col('s'))",
    "SUBSTR":           "F.substring(F.col('s'), start, length)",
    "LENGTH":           "F.length(F.col('x'))",
    "LTRIM":            "F.ltrim(F.col('x'))",
    "RTRIM":            "F.rtrim(F.col('x'))",
    "TRIM":             "F.trim(F.col('x'))",
    "UPPER":            "F.upper(F.col('x'))",
    "LOWER":            "F.lower(F.col('x'))",
    "LPAD":             "F.lpad(F.col('s'), len, pad)",
    "RPAD":             "F.rpad(F.col('s'), len, pad)",
    "CONCAT":           "F.concat(F.col('a'), F.col('b'))",
    "MOD":              "F.col('a') % b",
    "ABS":              "F.abs(F.col('x'))",
    "GREATEST":         "F.greatest(F.col('a'), F.col('b'), ...)",
    "LEAST":            "F.least(F.col('a'), F.col('b'), ...)",
    "ROUND":            "F.round(F.col('x'), d)",
    "FLOOR":            "F.floor(F.col('x'))",
    "CEIL":             "F.ceil(F.col('x'))",
    "POWER":            "F.pow(F.col('b'), e)",
    "SQRT":             "F.sqrt(F.col('x'))",
    "SIGN":             "F.signum(F.col('x'))",
    "REG_EXTRACT":      "F.regexp_extract(F.col('s'), r, 0)",
    "REG_REPLACE":      "F.regexp_replace(F.col('s'), r, repl)",
    "IN":               "F.col('x').isin([a, b, ...])",
    "LIKE":             "F.col('x').like('pattern')",
}

_DATATYPE_MAP_PYSPARK = {
    "string":     "StringType()",
    "nstring":    "StringType()",
    "char":       "StringType()",
    "nchar":      "StringType()",
    "varchar":    "StringType()",
    "nvarchar":   "StringType()",
    "text":       "StringType()",
    "integer":    "IntegerType()",
    "int":        "IntegerType()",
    "smallint":   "IntegerType()",
    "bigint":     "LongType()",
    "number":     "DecimalType(38, 10)",
    "decimal":    "DecimalType(p, s)",
    "float":      "FloatType()",
    "double":     "DoubleType()",
    "real":       "FloatType()",
    "date/time":  "TimestampType()",
    "date":       "DateType()",
    "time":       "StringType()  # no native TimeType; store as STRING",
    "timestamp":  "TimestampType()",
    "binary":     "BinaryType()",
    "bit":        "BooleanType()",
}

_TRANSFORM_PATTERNS_PYSPARK = {
    "Source Qualifier": {
        "df_prefix": "df_SQ_",
        "read": "spark.read.table(f\"{params['schema']}.{table_name}\")",
        "distinct_support": True,
        "filter_support": True,
        "sql_override_support": True,
    },
    "Expression": {
        "df_prefix": "df_EXP_",
        "output_ports": "df.withColumn('out', F.expr('...'))",
        "input_ports": "pass-through — no withColumn needed",
        "variable_ports": "intermediate withColumn referenced by subsequent expressions",
    },
    "Lookup Procedure": {
        "df_prefix": "df_LKP_",
        "join_type": "left",
        "read": "spark.read.table(f\"{params['schema']}.{lookup_table}\")",
    },
    "Router": {
        "df_prefix": "df_RTR_<group>_",
        "generates_multiple_output_dataframes": True,
        "default_group": "rows not matching any named group condition",
    },
    "Update Strategy": {
        "df_prefix": "df_UPD_",
        "DD_INSERT": "df.write.mode('append').saveAsTable(...)",
        "DD_UPDATE": "createOrReplaceTempView + spark.sql(MERGE ...)",
        "DD_DELETE": "createOrReplaceTempView + spark.sql(MERGE ... WHEN MATCHED THEN DELETE)",
        "DD_REJECT": "# TODO: rejected rows — manual review required",
    },
    "Aggregator": {
        "df_prefix": "df_AGG_",
        "group_by_ports": "fields with expression_type=GROUPBY",
        "aggregate_ports": "fields with aggregate expressions (count/sum/avg/max/min)",
    },
    "Filter": {
        "df_prefix": "df_FLT_",
        "api": "df.filter(F.expr('<condition>'))",
    },
    "Joiner": {
        "df_prefix": "df_JNR_",
        "master_side": "left DataFrame (already assigned)",
        "detail_side": "right DataFrame loaded via spark.read.table()",
        "default_join": "inner",
    },
    "Sequence Generator": {
        "df_prefix": "df_SEQ_",
        "NEXTVAL": "F.row_number().over(Window.orderBy(F.lit(1)))",
        "CURRVAL": "F.row_number().over(Window.orderBy(F.lit(1))) - 1",
    },
}

# ---------------------------------------------------------------------------
# Exported context layers — keyed by converter target
# ---------------------------------------------------------------------------

CONTEXT_LAYERS = {
    "bq": {
        "function_map":       _FUNCTION_MAP_BQ,
        "datatype_map":       _DATATYPE_MAP_BQ,
        "transform_patterns": _TRANSFORM_PATTERNS_BQ,
    },
    "pyspark": {
        "function_map":       _FUNCTION_MAP_PYSPARK,
        "datatype_map":       _DATATYPE_MAP_PYSPARK,
        "transform_patterns": _TRANSFORM_PATTERNS_PYSPARK,
    },
}

# ---------------------------------------------------------------------------
# Extractor class
# ---------------------------------------------------------------------------

class KGExtractor:

    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract(self, mapping_name: str, folder_name: str = None) -> dict:
        with self.driver.session() as db:
            mapping_rows = self._find_mapping(db, mapping_name, folder_name)
            if not mapping_rows:
                hint = f" in folder '{folder_name}'" if folder_name else ""
                raise ValueError(f"Mapping '{mapping_name}' not found{hint}.")
            if len(mapping_rows) > 1:
                folders = [r["folder"] for r in mapping_rows]
                raise ValueError(
                    f"Ambiguous: mapping '{mapping_name}' exists in folders {folders}. "
                    "Use --folder to select one."
                )
            info = mapping_rows[0]
            mid  = info["mapping_id"]
            subgraph = self._extract_mapping_subgraph(db, mid)
            subgraph["mapping_name"] = info["name"]

        return {
            "subgraph": subgraph,
            "context_layer": {
                "function_map":       _FUNCTION_MAP_BQ,
                "datatype_map":       _DATATYPE_MAP_BQ,
                "transform_patterns": _TRANSFORM_PATTERNS_BQ,
            },
        }

    def extract_workflow(self, workflow_names: list, folder_name: str = None) -> dict:
        """Extract all session mappings from one or more workflows into a combined structure."""
        all_mappings = []

        with self.driver.session() as db:
            for wf_name in workflow_names:
                wf_rows = self._find_workflow(db, wf_name, folder_name)
                if not wf_rows:
                    hint = f" in folder '{folder_name}'" if folder_name else ""
                    raise ValueError(f"Workflow '{wf_name}' not found{hint}.")

                info = wf_rows[0]
                wid  = info["workflow_id"]

                sessions  = self._get_workflow_sessions(db, wid)
                dep_edges = self._get_session_order(db, wid)

                session_by_id  = {s["session_id"]: s for s in sessions}
                ordered_ids    = self._topo_sort_sessions(sessions, dep_edges)
                ordered_sessions = [session_by_id[sid] for sid in ordered_ids if sid in session_by_id]

                for order_idx, session in enumerate(ordered_sessions):
                    mapping_name = session.get("mapping_name", "")
                    if not mapping_name:
                        continue

                    mapping_rows = self._find_mapping(db, mapping_name, folder_name)
                    if not mapping_rows:
                        continue

                    m_info   = mapping_rows[0]
                    subgraph = self._extract_mapping_subgraph(db, m_info["mapping_id"])
                    subgraph["mapping_name"]  = m_info["name"]
                    subgraph["session_name"]  = session["name"]
                    subgraph["session_order"] = order_idx
                    subgraph["workflow_name"] = wf_name
                    all_mappings.append(subgraph)

        return {
            "workflow": {
                "workflow_names": workflow_names,
                "folder":         folder_name,
                "mappings":       all_mappings,
            },
            "context_layer": {
                "function_map":       _FUNCTION_MAP_BQ,
                "datatype_map":       _DATATYPE_MAP_BQ,
                "transform_patterns": _TRANSFORM_PATTERNS_BQ,
            },
        }

    # ------------------------------------------------------------------
    # Neo4j queries
    # ------------------------------------------------------------------

    def _extract_mapping_subgraph(self, db, mid) -> dict:
        """Extract all data for one mapping; returns the subgraph dict (no context_layer or mapping_name)."""
        transformations   = self._get_transformations(db, mid)
        ports_by_trans    = self._get_ports(db, mid)
        props_by_trans    = self._get_properties(db, mid)
        groups_by_trans   = self._get_router_groups(db, mid)
        lkp_by_trans      = self._get_lookup_conditions(db, mid)
        inter_edges       = self._get_inter_flows(db, mid)
        intra_flows       = self._get_intra_flows(db, mid)
        target_flows      = self._get_target_flows(db, mid)
        sources           = self._get_sources(db, mid)
        targets           = self._get_targets(db, mid)
        parameters        = self._get_parameters(db, mid)

        ordered  = self._topo_sort(transformations, inter_edges)
        pipeline = self._build_pipeline(ordered, ports_by_trans, props_by_trans,
                                        groups_by_trans, lkp_by_trans, intra_flows)

        field_lineage = [
            {
                "from_field":    e["from_field"],
                "from_instance": e["from_instance"],
                "from_type":     e["from_type"],
                "to_field":      e["to_field"],
                "to_instance":   e["to_instance"],
                "to_type":       e["to_type"],
            }
            for e in (inter_edges + target_flows)
        ]

        return {
            "description":   "",
            "sources":       sources,
            "targets":       targets,
            "pipeline":      pipeline,
            "field_lineage": field_lineage,
            "parameters":    parameters,
        }

    def _find_mapping(self, db, mapping_name, folder_name):
        q = (
            "MATCH (r:Repository)-[:HAS_FOLDER]->(f:Folder)"
            "-[:HAS_MAPPING]->(m:Mapping) "
            "WHERE m.name = $name"
        )
        params = {"name": mapping_name}
        if folder_name:
            q += " AND f.name = $folder"
            params["folder"] = folder_name
        q += " RETURN m.mappingId AS mapping_id, m.name AS name, f.name AS folder"
        return [dict(r) for r in db.run(q, **params)]

    def _get_transformations(self, db, mid):
        result = db.run(
            "MATCH (m:Mapping {mappingId: $mid})-[:HAS_TRANSFORMATION]->(t:Transformation) "
            "RETURN t.transformationId AS id, t.name AS name, t.type AS type",
            mid=mid,
        )
        return [dict(r) for r in result]

    def _get_ports(self, db, mid):
        result = db.run(
            """
            MATCH (m:Mapping {mappingId: $mid})-[:HAS_TRANSFORMATION]->(t:Transformation)
                  -[:HAS_PORT]->(p:Column:Port)
            OPTIONAL MATCH (p)-[:USES_EXPRESSION]->(e:Expression)
            RETURN t.transformationId          AS trans_id,
                   p.portId                   AS port_id,
                   p.name                     AS name,
                   p.portType                 AS port_type,
                   p.datatype                 AS datatype,
                   p.precision                AS precision,
                   p.scale                    AS scale,
                   p.defaultValue             AS default_value,
                   coalesce(p.ordinalPosition, 0) AS ord_pos,
                   coalesce(p.group, '')       AS group,
                   coalesce(p.refField, '')    AS ref_field,
                   e.rawExpression            AS expression
            ORDER BY t.transformationId, p.ordinalPosition
            """,
            mid=mid,
        )
        by_trans = defaultdict(list)
        for row in result:
            by_trans[row["trans_id"]].append(dict(row))
        return by_trans

    def _get_router_groups(self, db, mid):
        result = db.run(
            """
            MATCH (m:Mapping {mappingId: $mid})-[:HAS_TRANSFORMATION]->(t:Transformation)
                  -[:HAS_GROUP]->(rg:RouterGroup)
            RETURN t.transformationId          AS trans_id,
                   rg.name                    AS name,
                   coalesce(rg.groupType, '') AS group_type,
                   coalesce(rg.condition, '') AS condition,
                   toInteger(coalesce(rg.order, '0')) AS order
            ORDER BY t.transformationId, toInteger(coalesce(rg.order, '0'))
            """,
            mid=mid,
        )
        by_trans = defaultdict(list)
        for row in result:
            by_trans[row["trans_id"]].append({
                "name":       row["name"],
                "group_type": row["group_type"],
                "condition":  row["condition"],
                "order":      row["order"],
            })
        return by_trans

    def _get_lookup_conditions(self, db, mid):
        """LookupCondition nodes store the canonical table name and join predicate."""
        result = db.run(
            """
            MATCH (m:Mapping {mappingId: $mid})-[:HAS_TRANSFORMATION]->(t:Transformation)
                  -[:HAS_LOOKUP]->(lc:LookupCondition)
            RETURN t.transformationId              AS trans_id,
                   coalesce(lc.tableName, '')      AS table_name,
                   coalesce(lc.joinCondition, '')  AS join_condition
            """,
            mid=mid,
        )
        by_trans = {}
        for row in result:
            by_trans[row["trans_id"]] = {
                "table_name":     row["table_name"],
                "join_condition": row["join_condition"],
            }
        return by_trans

    def _get_target_flows(self, db, mid):
        """BOUND_TO_FIELD edges — ports that write directly to target fields.
        Captures mappings where a transformation connects to a target without
        an intervening Update Strategy."""
        result = db.run(
            """
            MATCH (m:Mapping {mappingId: $mid})-[:HAS_TRANSFORMATION]->(t:Transformation)
                  -[:HAS_PORT]->(p:Column:Port)-[:BOUND_TO_FIELD]->(f:Column:Field)
                  <-[:HAS_FIELD]-(td:TargetDefinition)
            RETURN t.name               AS from_instance,
                   t.type               AS from_type,
                   p.name               AS from_field,
                   td.name              AS to_instance,
                   'Target Definition'  AS to_type,
                   f.name               AS to_field
            ORDER BY from_instance, from_field
            """,
            mid=mid,
        )
        return [dict(r) for r in result]

    def _get_properties(self, db, mid):
        result = db.run(
            """
            MATCH (m:Mapping {mappingId: $mid})-[:HAS_TRANSFORMATION]->(t:Transformation)
                  -[:HAS_PROPERTY]->(prop:TransformationProperty)
            RETURN t.transformationId AS trans_id, prop.name AS name, prop.value AS value
            """,
            mid=mid,
        )
        by_trans = defaultdict(dict)
        for row in result:
            by_trans[row["trans_id"]][row["name"]] = row["value"]
        return by_trans

    def _get_inter_flows(self, db, mid):
        """FLOWS_TO edges that cross transformation boundaries."""
        result = db.run(
            """
            MATCH (m:Mapping {mappingId: $mid})-[:HAS_TRANSFORMATION]->(t1:Transformation)
                  -[:HAS_PORT]->(p1:Column:Port)-[:FLOWS_TO]->(p2:Column:Port)
                  <-[:HAS_PORT]-(t2:Transformation)<-[:HAS_TRANSFORMATION]-(m)
            WHERE t1.transformationId <> t2.transformationId
            RETURN DISTINCT
                   t1.transformationId AS from_id,
                   t1.name             AS from_instance,
                   t1.type             AS from_type,
                   p1.name             AS from_field,
                   t2.transformationId AS to_id,
                   t2.name             AS to_instance,
                   t2.type             AS to_type,
                   p2.name             AS to_field
            """,
            mid=mid,
        )
        return [dict(r) for r in result]

    def _get_intra_flows(self, db, mid):
        """FLOWS_TO edges within the same transformation (Router / Expression)."""
        result = db.run(
            """
            MATCH (m:Mapping {mappingId: $mid})-[:HAS_TRANSFORMATION]->(t:Transformation)
                  -[:HAS_PORT]->(p1:Column:Port)-[:FLOWS_TO]->(p2:Column:Port)<-[:HAS_PORT]-(t)
            RETURN t.transformationId AS trans_id,
                   p1.portId          AS from_port_id,
                   p1.name            AS from_port_name,
                   p2.portId          AS to_port_id,
                   p2.name            AS to_port_name
            """,
            mid=mid,
        )
        by_trans = defaultdict(list)
        for row in result:
            by_trans[row["trans_id"]].append(dict(row))
        return by_trans

    def _get_sources(self, db, mid):
        result = db.run(
            """
            MATCH (m:Mapping {mappingId: $mid})-[:HAS_TRANSFORMATION]->(t:Transformation)
                  -[:HAS_PORT]->(p:Column:Port)<-[:BOUND_TO_PORT]-(f:Column:Field)
                  <-[:HAS_FIELD]-(sd:SourceDefinition)
            OPTIONAL MATCH (tbl:Table)-[:HAS_DEFINITION]->(sd)
            OPTIONAL MATCH (d:Database)-[:HAS_TABLE]->(tbl)
            RETURN DISTINCT
                   sd.name                          AS src_name,
                   coalesce(d.databaseType, '')      AS db_type,
                   coalesce(d.DBName, '')            AS db_name,
                   f.name                           AS field_name,
                   coalesce(f.datatype, '')          AS datatype,
                   coalesce(f.precision, '')         AS precision,
                   coalesce(f.scale, '')             AS scale,
                   coalesce(f.nullable, '')          AS nullable,
                   coalesce(f.ordinalPosition, 0)    AS ord_pos
            ORDER BY src_name, ord_pos
            """,
            mid=mid,
        )
        src_map = {}
        for row in result:
            name = row["src_name"]
            if name not in src_map:
                src_map[name] = {
                    "name":   name,
                    "db_type": row["db_type"],
                    "owner":  row["db_name"],
                    "fields": [],
                }
            src_map[name]["fields"].append({
                "name":      row["field_name"],
                "datatype":  row["datatype"],
                "precision": row["precision"],
                "scale":     row["scale"],
                "nullable":  row["nullable"],
            })
        return list(src_map.values())

    def _get_targets(self, db, mid):
        result = db.run(
            """
            MATCH (m:Mapping {mappingId: $mid})-[:HAS_TRANSFORMATION]->(t:Transformation)
                  -[:HAS_PORT]->(p:Column:Port)-[:BOUND_TO_FIELD]->(f:Column:Field)
                  <-[:HAS_FIELD]-(td:TargetDefinition)
            OPTIONAL MATCH (tbl:Table)-[:HAS_DEFINITION]->(td)
            OPTIONAL MATCH (d:Database)-[:HAS_TABLE]->(tbl)
            RETURN DISTINCT
                   td.name                          AS tgt_name,
                   coalesce(d.databaseType, '')      AS db_type,
                   f.name                           AS field_name,
                   coalesce(f.datatype, '')          AS datatype,
                   coalesce(f.precision, '')         AS precision,
                   coalesce(f.scale, '')             AS scale,
                   coalesce(f.ordinalPosition, 0)    AS ord_pos
            ORDER BY tgt_name, ord_pos
            """,
            mid=mid,
        )
        tgt_map = {}
        for row in result:
            name = row["tgt_name"]
            if name not in tgt_map:
                tgt_map[name] = {
                    "name":    name,
                    "db_type": row["db_type"],
                    "fields":  [],
                }
            tgt_map[name]["fields"].append({
                "name":      row["field_name"],
                "datatype":  row["datatype"],
                "precision": row["precision"],
                "scale":     row["scale"],
            })
        return list(tgt_map.values())

    def _get_parameters(self, db, mid):
        result = db.run(
            """
            MATCH (m:Mapping {mappingId: $mid})-[:HAS_TRANSFORMATION]->(p:Parameter)
            RETURN p.name AS name, p.datatype AS datatype, p.value AS default_value
            """,
            mid=mid,
        )
        return [
            {"name": r["name"], "datatype": r["datatype"] or "", "default_value": r["default_value"] or ""}
            for r in result
        ]

    def _find_workflow(self, db, workflow_name, folder_name):
        q = (
            "MATCH (r:Repository)-[:HAS_FOLDER]->(f:Folder)"
            "-[:HAS_WORKFLOW]->(w:Workflow) "
            "WHERE w.name = $name"
        )
        params = {"name": workflow_name}
        if folder_name:
            q += " AND f.name = $folder"
            params["folder"] = folder_name
        q += " RETURN w.workflowId AS workflow_id, w.name AS name, f.name AS folder"
        return [dict(r) for r in db.run(q, **params)]

    def _get_workflow_sessions(self, db, wid):
        result = db.run(
            """
            MATCH (w:Workflow {workflowId: $wid})-[:HAS_SESSION]->(s:Session)
            OPTIONAL MATCH (s)-[:RUNS_MAPPING]->(m:Mapping)
            RETURN s.sessionId                     AS session_id,
                   s.name                          AS name,
                   coalesce(m.name, '')             AS mapping_name
            """,
            wid=wid,
        )
        return [dict(r) for r in result]

    def _get_session_order(self, db, wid):
        """Returns DEPENDS_ON edges between sessions in the same workflow for ordering."""
        result = db.run(
            """
            MATCH (w:Workflow {workflowId: $wid})-[:HAS_SESSION]->(s2:Session)
                  -[:DEPENDS_ON]->(s1:Session)<-[:HAS_SESSION]-(w)
            RETURN s1.sessionId AS from_id, s2.sessionId AS to_id
            """,
            wid=wid,
        )
        return [dict(r) for r in result]

    # ------------------------------------------------------------------
    # Topological sort (Kahn's algorithm)
    # ------------------------------------------------------------------

    def _topo_sort(self, transformations, inter_edges):
        by_id = {t["id"]: t for t in transformations}
        all_ids = set(by_id)

        # Build in-degree map and adjacency list
        in_degree = {tid: 0 for tid in all_ids}
        adj = defaultdict(set)
        for e in inter_edges:
            src, dst = e["from_id"], e["to_id"]
            if dst not in adj[src]:
                adj[src].add(dst)
                in_degree[dst] += 1

        # Kahn's BFS
        queue = deque(sorted(tid for tid in all_ids if in_degree[tid] == 0))
        order = []
        while queue:
            tid = queue.popleft()
            order.append(tid)
            for neighbour in sorted(adj[tid]):
                in_degree[neighbour] -= 1
                if in_degree[neighbour] == 0:
                    queue.append(neighbour)

        # Append any remaining nodes (cycle fallback — should not occur in valid mappings)
        for tid in sorted(all_ids - set(order)):
            order.append(tid)

        return [by_id[tid] for tid in order if tid in by_id]

    def _topo_sort_sessions(self, sessions, dep_edges):
        """Topological sort of workflow sessions using DEPENDS_ON edges."""
        all_ids   = {s["session_id"] for s in sessions}
        in_degree = {sid: 0 for sid in all_ids}
        adj       = defaultdict(set)

        for e in dep_edges:
            src, dst = e["from_id"], e["to_id"]
            if src in all_ids and dst in all_ids and dst not in adj[src]:
                adj[src].add(dst)
                in_degree[dst] += 1

        queue = deque(sorted(sid for sid in all_ids if in_degree[sid] == 0))
        order = []
        while queue:
            sid = queue.popleft()
            order.append(sid)
            for neighbour in sorted(adj[sid]):
                in_degree[neighbour] -= 1
                if in_degree[neighbour] == 0:
                    queue.append(neighbour)

        for sid in sorted(all_ids - set(order)):
            order.append(sid)

        return order

    # ------------------------------------------------------------------
    # Pipeline assembly
    # ------------------------------------------------------------------

    def _build_pipeline(self, ordered_trans, ports_by_trans, props_by_trans,
                        groups_by_trans, lkp_by_trans, intra_flows):
        pipeline = []
        for trans in ordered_trans:
            tid    = trans["id"]
            ports  = sorted(ports_by_trans.get(tid, []), key=lambda p: p.get("ord_pos") or 0)
            props  = props_by_trans.get(tid, {})
            groups = groups_by_trans.get(tid, [])
            lkp    = lkp_by_trans.get(tid, {})
            intra  = intra_flows.get(tid, [])

            # Fallback ref_field from intra-flows when port.refField is not set
            intra_ref = {f["to_port_id"]: f["from_port_name"] for f in intra}

            fields = [
                {
                    "name":            p["name"],
                    "datatype":        p["datatype"] or "",
                    "expression":      p["expression"] or "",
                    "expression_type": self._infer_expr_type(p, trans["type"]),
                    "port_type":       p["port_type"] or "",
                    "group":           p.get("group") or "",
                    "ref_field":       (p.get("ref_field") or ""
                                        or intra_ref.get(p["port_id"], "")),
                }
                for p in ports
            ]

            # Map raw TABLEATTRIBUTE property names to JSON keys
            mapped = {}
            for prop_name, prop_value in props.items():
                key = _PROP_KEY.get(prop_name)
                if not key:
                    continue
                if key == "select_distinct":
                    mapped[key] = prop_value.strip().upper() in ("YES", "TRUE", "1")
                else:
                    mapped[key] = prop_value.strip() if prop_value else None

            # LookupCondition node takes precedence over TABLEATTRIBUTE-derived values
            lookup_table     = lkp.get("table_name") or mapped.get("lookup_table")
            lookup_condition = lkp.get("join_condition") or mapped.get("lookup_condition")

            pipeline.append({
                "name":                trans["name"],
                "type":                trans["type"],
                "fields":              fields,
                "groups":              groups,
                "sql_override":        mapped.get("sql_override"),
                "source_filter":       mapped.get("source_filter"),
                "lookup_table":        lookup_table,
                "lookup_condition":    lookup_condition,
                "filter_condition":    mapped.get("filter_condition"),
                "update_strategy_expr": mapped.get("update_strategy_expr"),
                "select_distinct":     mapped.get("select_distinct", False),
            })
        return pipeline

    @staticmethod
    def _infer_expr_type(port, trans_type):
        if trans_type == "Aggregator":
            ptype = (port.get("port_type") or "").upper()
            if "INPUT" in ptype:
                return "GROUPBY"
            expr = (port.get("expression") or "").upper()
            for agg in ("COUNT(", "SUM(", "AVG(", "MAX(", "MIN("):
                if agg in expr:
                    return "AGGREGATE"
        return ""


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Extract a mapping or workflow subgraph from Neo4j and output JSON for LLM conversion."
    )
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--mapping",  metavar="MAPPING_NAME",
                      help="Single mapping name (exact, case-sensitive).")
    mode.add_argument("--workflow", nargs="+", metavar="WORKFLOW_NAME",
                      help="One or more workflow names to extract and combine into a single multi-mapping JSON.")
    ap.add_argument("--folder",   default=None,                     help="Folder name — required when name is ambiguous.")
    ap.add_argument("--uri",      default="bolt://localhost:7687",   help="Neo4j bolt URI.")
    ap.add_argument("--user",     default="neo4j",                  help="Neo4j username.")
    ap.add_argument("--password", required=True,                    help="Neo4j password.")
    ap.add_argument("--output",   default=None,                     help="Output JSON file (default: stdout).")
    args = ap.parse_args()

    if not _NEO4J_AVAILABLE:
        print("ERROR: neo4j package not installed. Run: pip install neo4j", file=sys.stderr)
        sys.exit(1)

    extractor = KGExtractor(args.uri, args.user, args.password)
    try:
        if args.mapping:
            payload = extractor.extract(args.mapping, args.folder)
        else:
            payload = extractor.extract_workflow(args.workflow, args.folder)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        extractor.close()

    out = json.dumps(payload, indent=2, ensure_ascii=False)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(out)
        print(f"Subgraph written to: {args.output}")
    else:
        print(out)


if __name__ == "__main__":
    main()
