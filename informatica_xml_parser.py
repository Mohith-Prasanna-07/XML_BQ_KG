"""
Informatica PowerCenter XML → Neo4j Knowledge Graph Parser

Parses an Informatica XML export and loads it into Neo4j following the
graph model defined in informatica_to_bigquery_graph_model_simplified.md.

Usage:
    # Dry-run (no Neo4j needed — just prints what would be loaded)
    python informatica_xml_parser.py <xml_file> --dry-run

    # Load into Neo4j
    python informatica_xml_parser.py <xml_file> --password <neo4j_password>
    python informatica_xml_parser.py <xml_file> --uri bolt://localhost:7687 --user neo4j --password secret
"""

import argparse
import collections
import logging
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

try:
    from neo4j import GraphDatabase
    from neo4j.exceptions import ServiceUnavailable
    _NEO4J_AVAILABLE = True
except ImportError:
    _NEO4J_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Port type label mapping (Informatica PORTTYPE → Neo4j label)
# ---------------------------------------------------------------------------
_PORT_LABEL = {
    "INPUT": "InputPort",
    "OUTPUT": "OutputPort",
    "INPUT/OUTPUT": "InputOutputPort",
    "LOCAL VARIABLE": "VariablePort",
}


def _port_label(porttype_raw: str) -> str:
    return _PORT_LABEL.get(porttype_raw.upper().strip(), "InputOutputPort")


# ---------------------------------------------------------------------------
# ID builders — must match the strategy in the markdown
# ---------------------------------------------------------------------------

def repo_id(repo_name):
    return repo_name

def folder_id(repo_name, folder_name):
    return f"{repo_name}.{folder_name}"

def workflow_id(repo_name, folder_name, workflow_name):
    return f"{repo_name}.{folder_name}.{workflow_name}"

def session_id(repo_name, folder_name, workflow_name, session_name):
    return f"{repo_name}.{folder_name}.{workflow_name}.{session_name}"

def mapping_id(repo_name, folder_name, mapping_name):
    return f"{repo_name}.{folder_name}.{mapping_name}"

def transformation_id(repo_name, folder_name, mapping_name, trans_name):
    return f"{repo_name}.{folder_name}.{mapping_name}.{trans_name}"

def port_id(repo_name, folder_name, mapping_name, trans_name, port_name):
    return f"{repo_name}.{folder_name}.{mapping_name}.{trans_name}.{port_name}"

def expression_id(p_id):
    return f"{p_id}.expression"

def property_id(repo_name, folder_name, mapping_name, trans_name, prop_name):
    return f"{repo_name}.{folder_name}.{mapping_name}.{trans_name}.{prop_name}"

def field_id(repo_name, folder_name, obj_type, obj_name, field_name):
    # obj_type: "SOURCE" or "TARGET"
    return f"{repo_name}.{folder_name}.{obj_type}.{obj_name}.{field_name}"

def parameter_id(repo_name, folder_name, scope, param_name):
    return f"{repo_name}.{folder_name}.{scope}.{param_name}"


# ---------------------------------------------------------------------------
# Dry-run session stub — counts and prints what would be written
# ---------------------------------------------------------------------------

class _DryRunSession:
    """Drop-in replacement for a neo4j.Session in dry-run mode."""

    def __init__(self):
        self._counts = collections.Counter()

    def run(self, query, **params):
        # Detect the operation type from the first keyword
        q = query.strip().upper()
        if "MERGE (r:Repository" in query:
            self._counts["Repository"] += 1
        elif "MERGE (f:Folder" in query:
            self._counts["Folder"] += 1
        elif "MERGE (m:Mapping" in query:
            self._counts["Mapping"] += 1
        elif "MERGE (w:Workflow" in query:
            self._counts["Workflow"] += 1
        elif "MERGE (wl:Worklet" in query:
            self._counts["Worklet"] += 1
        elif "MERGE (s:Session" in query:
            self._counts["Session"] += 1
        elif "MERGE (t:Transformation" in query:
            self._counts["Transformation"] += 1
        elif ":Column:Port:" in query and "MERGE" in query:
            self._counts["Port"] += 1
        elif "MERGE (e:Expression" in query:
            self._counts["Expression"] += 1
        elif "MERGE (p:TransformationProperty" in query:
            self._counts["TransformationProperty"] += 1
        elif "MERGE (p:Parameter" in query:
            self._counts["Parameter"] += 1
        elif "MERGE (f:Column:Field" in query:
            self._counts["Field"] += 1
        elif "MERGE (from)-[:FLOWS_TO]" in query:
            self._counts["rel:FLOWS_TO"] += 1
        elif "MERGE (f)-[:BOUND_TO_PORT]" in query:
            self._counts["rel:BOUND_TO_PORT"] += 1
        elif "MERGE (p)-[:BOUND_TO_FIELD]" in query:
            self._counts["rel:BOUND_TO_FIELD"] += 1

    def report(self):
        print("\n--- Dry-run summary (nothing written to Neo4j) ---")
        nodes = {k: v for k, v in self._counts.items() if not k.startswith("rel:")}
        rels  = {k[4:]: v for k, v in self._counts.items() if k.startswith("rel:")}
        print("Nodes:")
        for label, count in sorted(nodes.items()):
            print(f"  {label:30s} {count:>6}")
        print("Relationships:")
        for rel, count in sorted(rels.items()):
            print(f"  {rel:30s} {count:>6}")
        total = sum(nodes.values())
        print(f"\nTotal nodes: {total}  |  Total relationships: {sum(rels.values())}")


# ---------------------------------------------------------------------------
# Parser / loader
# ---------------------------------------------------------------------------

class InformaticaXMLParser:

    def __init__(self, uri: str = None, user: str = None, password: str = None,
                 dry_run: bool = False):
        self._dry_run = dry_run
        if not dry_run:
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
        else:
            self.driver = None

    def close(self):
        if self.driver:
            self.driver.close()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def parse_and_load(self, xml_path: str) -> None:
        logger.info("Parsing XML: %s", xml_path)
        tree = ET.parse(xml_path)
        root = tree.getroot()

        # Root may be POWERMART or REPOSITORY depending on export format
        repo_elem = (
            root.find("REPOSITORY") if root.tag == "POWERMART" else root
        )
        if repo_elem is None:
            raise ValueError("Cannot find <REPOSITORY> element in XML.")

        if self._dry_run:
            dry_session = _DryRunSession()
            self._create_constraints(dry_session)
            self._load_repository(dry_session, repo_elem)
            dry_session.report()
        else:
            with self.driver.session() as db:
                self._create_constraints(db)
                self._load_repository(db, repo_elem)
            logger.info("Load complete.")

    # ------------------------------------------------------------------
    # Schema constraints
    # ------------------------------------------------------------------

    def _create_constraints(self, db) -> None:
        stmts = [
            "CREATE CONSTRAINT repository_id IF NOT EXISTS FOR (r:Repository) REQUIRE r.repositoryId IS UNIQUE",
            "CREATE CONSTRAINT folder_id IF NOT EXISTS FOR (f:Folder) REQUIRE f.folderId IS UNIQUE",
            "CREATE CONSTRAINT workflow_id IF NOT EXISTS FOR (w:Workflow) REQUIRE w.workflowId IS UNIQUE",
            "CREATE CONSTRAINT session_id IF NOT EXISTS FOR (s:Session) REQUIRE s.sessionId IS UNIQUE",
            "CREATE CONSTRAINT mapping_id IF NOT EXISTS FOR (m:Mapping) REQUIRE m.mappingId IS UNIQUE",
            "CREATE CONSTRAINT transformation_id IF NOT EXISTS FOR (t:Transformation) REQUIRE t.transformationId IS UNIQUE",
            "CREATE CONSTRAINT port_id IF NOT EXISTS FOR (p:Port) REQUIRE p.portId IS UNIQUE",
            "CREATE CONSTRAINT expression_id IF NOT EXISTS FOR (e:Expression) REQUIRE e.expressionId IS UNIQUE",
            "CREATE CONSTRAINT property_id IF NOT EXISTS FOR (tp:TransformationProperty) REQUIRE tp.propertyId IS UNIQUE",
            "CREATE CONSTRAINT parameter_id IF NOT EXISTS FOR (p:Parameter) REQUIRE p.parameterId IS UNIQUE",
        ]
        for stmt in stmts:
            try:
                db.run(stmt)
            except Exception as exc:
                logger.debug("Constraint note: %s", exc)

    # ------------------------------------------------------------------
    # Repository
    # ------------------------------------------------------------------

    def _load_repository(self, db, repo_elem) -> None:
        r_name = repo_elem.get("NAME", "Unknown")
        r_id = repo_id(r_name)

        db.run(
            """
            MERGE (r:Repository {repositoryId: $id})
            SET r.name        = $name,
                r.version     = $version,
                r.databaseType = $dbType
            """,
            id=r_id,
            name=r_name,
            version=repo_elem.get("VERSION", ""),
            dbType=repo_elem.get("DATABASETYPE", ""),
        )
        logger.info("Repository: %s", r_name)

        for folder_elem in repo_elem.findall("FOLDER"):
            self._load_folder(db, folder_elem, r_name)

    # ------------------------------------------------------------------
    # Folder
    # ------------------------------------------------------------------

    def _load_folder(self, db, folder_elem, r_name: str) -> None:
        f_name = folder_elem.get("NAME", "")
        f_id = folder_id(r_name, f_name)

        db.run(
            """
            MERGE (f:Folder {folderId: $id})
            SET f.name  = $name,
                f.owner = $owner
            WITH f
            MATCH (r:Repository {repositoryId: $repoId})
            MERGE (r)-[:HAS_FOLDER]->(f)
            """,
            id=f_id,
            name=f_name,
            owner=folder_elem.get("OWNER", ""),
            repoId=repo_id(r_name),
        )
        logger.info("  Folder: %s", f_name)

        # Folder-level source/target definitions (shared across mappings)
        folder_sources = self._index_sources(folder_elem, r_name, f_name)
        folder_targets = self._index_targets(folder_elem, r_name, f_name)

        for source_elem in folder_elem.findall("SOURCE"):
            self._load_fields_from_source(db, source_elem, r_name, f_name)

        for target_elem in folder_elem.findall("TARGET"):
            self._load_fields_from_target(db, target_elem, r_name, f_name)

        for mapping_elem in folder_elem.findall("MAPPING"):
            self._load_mapping(db, mapping_elem, r_name, f_name, f_id,
                               folder_sources, folder_targets)

        for workflow_elem in folder_elem.findall("WORKFLOW"):
            self._load_workflow(db, workflow_elem, r_name, f_name, f_id)

    # ------------------------------------------------------------------
    # Source / Target field registries
    # ------------------------------------------------------------------

    def _index_sources(self, parent_elem, r_name, f_name) -> dict:
        """Returns {source_name: {field_name: field_id}} for lookup."""
        index = {}
        for src in parent_elem.findall("SOURCE"):
            name = src.get("NAME", "")
            index[name] = {
                sf.get("NAME", ""): field_id(r_name, f_name, "SOURCE", name, sf.get("NAME", ""))
                for sf in src.findall("SOURCEFIELD")
            }
        return index

    def _index_targets(self, parent_elem, r_name, f_name) -> dict:
        index = {}
        for tgt in parent_elem.findall("TARGET"):
            name = tgt.get("NAME", "")
            index[name] = {
                tf.get("NAME", ""): field_id(r_name, f_name, "TARGET", name, tf.get("NAME", ""))
                for tf in tgt.findall("TARGETFIELD")
            }
        return index

    def _load_fields_from_source(self, db, src_elem, r_name, f_name) -> None:
        src_name = src_elem.get("NAME", "")
        for i, sf in enumerate(src_elem.findall("SOURCEFIELD")):
            f_name_col = sf.get("NAME", "")
            f_id = field_id(r_name, f_name, "SOURCE", src_name, f_name_col)
            db.run(
                """
                MERGE (f:Column:Field {fieldId: $id})
                SET f.name            = $name,
                    f.datatype        = $datatype,
                    f.precision       = $precision,
                    f.scale           = $scale,
                    f.nullable        = $nullable,
                    f.keyType         = $keyType,
                    f.ordinalPosition = $pos
                """,
                id=f_id,
                name=f_name_col,
                datatype=sf.get("DATATYPE", ""),
                precision=sf.get("PRECISION", ""),
                scale=sf.get("SCALE", ""),
                nullable=sf.get("NULLABLE", ""),
                keyType=sf.get("KEYTYPE", ""),
                pos=i,
            )

    def _load_fields_from_target(self, db, tgt_elem, r_name, f_name) -> None:
        tgt_name = tgt_elem.get("NAME", "")
        for i, tf in enumerate(tgt_elem.findall("TARGETFIELD")):
            f_name_col = tf.get("NAME", "")
            f_id = field_id(r_name, f_name, "TARGET", tgt_name, f_name_col)
            db.run(
                """
                MERGE (f:Column:Field {fieldId: $id})
                SET f.name            = $name,
                    f.datatype        = $datatype,
                    f.precision       = $precision,
                    f.scale           = $scale,
                    f.nullable        = $nullable,
                    f.keyType         = $keyType,
                    f.ordinalPosition = $pos
                """,
                id=f_id,
                name=f_name_col,
                datatype=tf.get("DATATYPE", ""),
                precision=tf.get("PRECISION", ""),
                scale=tf.get("SCALE", ""),
                nullable=tf.get("NULLABLE", ""),
                keyType=tf.get("KEYTYPE", ""),
                pos=i,
            )

    # ------------------------------------------------------------------
    # Mapping
    # ------------------------------------------------------------------

    def _load_mapping(self, db, mapping_elem, r_name, f_name, f_id,
                      folder_sources, folder_targets) -> None:
        m_name = mapping_elem.get("NAME", "")
        m_id = mapping_id(r_name, f_name, m_name)

        db.run(
            """
            MERGE (m:Mapping {mappingId: $id})
            SET m.name = $name
            WITH m
            MATCH (f:Folder {folderId: $folderId})
            MERGE (f)-[:HAS_MAPPING]->(m)
            """,
            id=m_id,
            name=m_name,
            folderId=f_id,
        )
        logger.info("    Mapping: %s", m_name)

        # Mapping-level source/target definitions (may override folder-level)
        local_sources = self._index_sources(mapping_elem, r_name, f_name)
        local_targets = self._index_targets(mapping_elem, r_name, f_name)

        for src_elem in mapping_elem.findall("SOURCE"):
            self._load_fields_from_source(db, src_elem, r_name, f_name)

        for tgt_elem in mapping_elem.findall("TARGET"):
            self._load_fields_from_target(db, tgt_elem, r_name, f_name)

        # Merge folder-level and local definitions; local wins on conflict
        sources = {**folder_sources, **local_sources}
        targets = {**folder_targets, **local_targets}

        # Build instance name → {type, name} map from INSTANCE elements
        instance_map = self._build_instance_map(mapping_elem)

        # Transformations and their ports
        for trans_elem in mapping_elem.findall("TRANSFORMATION"):
            self._load_transformation(db, trans_elem, r_name, f_name, m_name, m_id)

        # Mapping-level parameters (MAPPINGVARIABLE)
        for var_elem in mapping_elem.findall("MAPPINGVARIABLE"):
            self._load_mapping_variable(db, var_elem, r_name, f_name, m_name, m_id)

        # Connectors — must run after all ports exist
        for conn_elem in mapping_elem.findall("CONNECTOR"):
            self._load_connector(db, conn_elem, r_name, f_name, m_name,
                                 instance_map, sources, targets)

    def _build_instance_map(self, mapping_elem) -> dict:
        """
        Returns {instance_name: {'type': 'TRANSFORMATION'|'SOURCE'|'TARGET',
                                  'name': actual_object_name}}
        """
        result = {}
        for inst in mapping_elem.findall("INSTANCE"):
            inst_name = inst.get("NAME", "")
            inst_type = inst.get("TYPE", "TRANSFORMATION").upper()
            trans_name = inst.get("TRANSFORMATION_NAME", inst_name)
            result[inst_name] = {"type": inst_type, "name": trans_name}
        return result

    # ------------------------------------------------------------------
    # Transformation
    # ------------------------------------------------------------------

    def _load_transformation(self, db, trans_elem, r_name, f_name,
                              m_name, m_id) -> None:
        t_name = trans_elem.get("NAME", "")
        t_id = transformation_id(r_name, f_name, m_name, t_name)

        db.run(
            """
            MERGE (t:Transformation {transformationId: $id})
            SET t.name         = $name,
                t.type         = $type,
                t.reusable     = $reusable,
                t.businessName = $businessName
            WITH t
            MATCH (m:Mapping {mappingId: $mId})
            MERGE (m)-[:HAS_TRANSFORMATION]->(t)
            """,
            id=t_id,
            name=t_name,
            type=trans_elem.get("TYPE", ""),
            reusable=trans_elem.get("REUSABLE", "NO"),
            businessName=trans_elem.get("BUSINESSNAME", ""),
            mId=m_id,
        )

        for i, tf in enumerate(trans_elem.findall("TRANSFORMFIELD")):
            self._load_port(db, tf, r_name, f_name, m_name, t_name, t_id, i)

        for attr in trans_elem.findall("TABLEATTRIBUTE"):
            self._load_property(db, attr, r_name, f_name, m_name, t_name, t_id)

    # ------------------------------------------------------------------
    # Port (TRANSFORMFIELD)
    # ------------------------------------------------------------------

    def _load_port(self, db, tf_elem, r_name, f_name, m_name,
                   t_name, t_id, position: int) -> None:
        p_name = tf_elem.get("NAME", "")
        porttype_raw = tf_elem.get("PORTTYPE", tf_elem.get("TYPE", "INPUT/OUTPUT"))
        specific_label = _port_label(porttype_raw)
        p_id = port_id(r_name, f_name, m_name, t_name, p_name)
        expression = tf_elem.get("EXPRESSION", "")

        # Neo4j does not allow dynamic labels in plain Cypher parameters,
        # so we interpolate the label string (it is never user-supplied).
        db.run(
            f"""
            MERGE (p:Column:Port:{specific_label} {{portId: $id}})
            SET p.name            = $name,
                p.portType        = $portType,
                p.datatype        = $datatype,
                p.precision       = $precision,
                p.scale           = $scale,
                p.defaultValue    = $defaultValue,
                p.ordinalPosition = $pos,
                p.isVariable      = $isVar
            WITH p
            MATCH (t:Transformation {{transformationId: $tId}})
            MERGE (t)-[:HAS_PORT]->(p)
            """,
            id=p_id,
            name=p_name,
            portType=porttype_raw,
            datatype=tf_elem.get("DATATYPE", ""),
            precision=tf_elem.get("PRECISION", ""),
            scale=tf_elem.get("SCALE", ""),
            defaultValue=tf_elem.get("DEFAULTVALUE", ""),
            pos=position,
            isVar=specific_label == "VariablePort",
            tId=t_id,
        )

        # Create Expression node when a non-trivial expression is present
        if expression and expression.strip() and expression.strip() != p_name:
            e_id = expression_id(p_id)
            db.run(
                """
                MERGE (e:Expression {expressionId: $id})
                SET e.rawExpression = $expr,
                    e.language      = 'Informatica'
                WITH e
                MATCH (p:Column:Port {portId: $pId})
                MERGE (p)-[:USES_EXPRESSION]->(e)
                """,
                id=e_id,
                expr=expression,
                pId=p_id,
            )

    # ------------------------------------------------------------------
    # Transformation property (TABLEATTRIBUTE)
    # ------------------------------------------------------------------

    def _load_property(self, db, attr_elem, r_name, f_name, m_name,
                       t_name, t_id) -> None:
        prop_name = attr_elem.get("NAME", "")
        prop_value = attr_elem.get("VALUE", "")
        p_id = property_id(r_name, f_name, m_name, t_name, prop_name)

        db.run(
            """
            MERGE (p:TransformationProperty {propertyId: $id})
            SET p.name  = $name,
                p.value = $value
            WITH p
            MATCH (t:Transformation {transformationId: $tId})
            MERGE (t)-[:HAS_PROPERTY]->(p)
            """,
            id=p_id,
            name=prop_name,
            value=prop_value,
            tId=t_id,
        )

    # ------------------------------------------------------------------
    # Mapping variable / parameter
    # ------------------------------------------------------------------

    def _load_mapping_variable(self, db, var_elem, r_name, f_name, m_name, m_id) -> None:
        var_name = var_elem.get("NAME", "")
        p_id = parameter_id(r_name, f_name, m_name, var_name)

        db.run(
            """
            MERGE (p:Parameter {parameterId: $id})
            SET p.name     = $name,
                p.value    = $value,
                p.datatype = $datatype,
                p.scope    = $scope
            WITH p
            MATCH (m:Mapping {mappingId: $mId})
            MERGE (m)-[:HAS_TRANSFORMATION]->(p)
            """,
            id=p_id,
            name=var_name,
            value=var_elem.get("DEFAULTVALUE", ""),
            datatype=var_elem.get("DATATYPE", ""),
            scope=m_name,
            mId=m_id,
        )

    # ------------------------------------------------------------------
    # Connector → FLOWS_TO / BOUND_TO_PORT / BOUND_TO_FIELD
    # ------------------------------------------------------------------

    def _load_connector(self, db, conn_elem, r_name, f_name, m_name,
                        instance_map, sources, targets) -> None:
        from_inst_name = conn_elem.get("FROMINSTANCE", "")
        from_field_name = conn_elem.get("FROMFIELD", "")
        to_inst_name = conn_elem.get("TOINSTANCE", "")
        to_field_name = conn_elem.get("TOFIELD", "")

        from_info = instance_map.get(from_inst_name, {"type": "TRANSFORMATION", "name": from_inst_name})
        to_info   = instance_map.get(to_inst_name,   {"type": "TRANSFORMATION", "name": to_inst_name})

        from_type = from_info["type"]
        to_type   = to_info["type"]
        from_obj  = from_info["name"]
        to_obj    = to_info["name"]

        if from_type == "SOURCE" and to_type == "TRANSFORMATION":
            # Field → Port  (BOUND_TO_PORT)
            f_id = (sources.get(from_obj, {}).get(from_field_name)
                    or field_id(r_name, f_name, "SOURCE", from_obj, from_field_name))
            p_id = port_id(r_name, f_name, m_name, to_obj, to_field_name)
            db.run(
                """
                MATCH (f:Column:Field {fieldId: $fId})
                MATCH (p:Column:Port  {portId:  $pId})
                MERGE (f)-[:BOUND_TO_PORT]->(p)
                """,
                fId=f_id, pId=p_id,
            )

        elif from_type == "TRANSFORMATION" and to_type == "TARGET":
            # Port → Field  (BOUND_TO_FIELD)
            p_id = port_id(r_name, f_name, m_name, from_obj, from_field_name)
            f_id = (targets.get(to_obj, {}).get(to_field_name)
                    or field_id(r_name, f_name, "TARGET", to_obj, to_field_name))
            db.run(
                """
                MATCH (p:Column:Port  {portId:  $pId})
                MATCH (f:Column:Field {fieldId: $fId})
                MERGE (p)-[:BOUND_TO_FIELD]->(f)
                """,
                pId=p_id, fId=f_id,
            )

        elif from_type == "TRANSFORMATION" and to_type == "TRANSFORMATION":
            # Port → Port  (FLOWS_TO)
            from_p_id = port_id(r_name, f_name, m_name, from_obj, from_field_name)
            to_p_id   = port_id(r_name, f_name, m_name, to_obj,   to_field_name)
            db.run(
                """
                MATCH (from:Column:Port {portId: $fromId})
                MATCH (to:Column:Port   {portId: $toId})
                MERGE (from)-[:FLOWS_TO]->(to)
                """,
                fromId=from_p_id, toId=to_p_id,
            )

        else:
            logger.debug(
                "Skipping connector %s.%s → %s.%s (types: %s→%s)",
                from_inst_name, from_field_name,
                to_inst_name,   to_field_name,
                from_type, to_type,
            )

    # ------------------------------------------------------------------
    # Workflow
    # ------------------------------------------------------------------

    def _load_workflow(self, db, wf_elem, r_name, f_name, f_id) -> None:
        wf_name = wf_elem.get("NAME", "")
        wf_id = workflow_id(r_name, f_name, wf_name)

        db.run(
            """
            MERGE (w:Workflow {workflowId: $id})
            SET w.name = $name
            WITH w
            MATCH (f:Folder {folderId: $fId})
            MERGE (f)-[:HAS_WORKFLOW]->(w)
            """,
            id=wf_id,
            name=wf_name,
            fId=f_id,
        )
        logger.info("    Workflow: %s", wf_name)

        # Tasks directly under the workflow
        self._load_tasks(db, wf_elem, r_name, f_name, wf_name, wf_id)

        # Worklets nested inside the workflow
        for wl_elem in wf_elem.findall("WORKLET"):
            wl_name = wl_elem.get("NAME", "")
            db.run(
                """
                MERGE (wl:Worklet {workletId: $id})
                SET wl.name      = $name,
                    wl.IsEnabled = $enabled
                WITH wl
                MATCH (w:Workflow {workflowId: $wfId})
                MERGE (w)-[:HAS_SESSION]->(wl)
                """,
                id=f"{wf_id}.{wl_name}",
                name=wl_name,
                enabled=wl_elem.get("ISENABLED", "YES"),
                wfId=wf_id,
            )
            self._load_tasks(db, wl_elem, r_name, f_name, wf_name, wf_id)

    def _load_tasks(self, db, parent_elem, r_name, f_name,
                    wf_name, wf_id) -> None:
        for task_elem in parent_elem.findall("TASK"):
            if task_elem.get("TYPE", "").upper() != "SESSION":
                continue

            task_name = task_elem.get("NAME", "")
            s_id = session_id(r_name, f_name, wf_name, task_name)

            db.run(
                """
                MERGE (s:Session {sessionId: $id})
                SET s.name = $name
                WITH s
                MATCH (w:Workflow {workflowId: $wfId})
                MERGE (w)-[:HAS_SESSION]->(s)
                """,
                id=s_id,
                name=task_name,
                wfId=wf_id,
            )

            # Wire session → mapping via "Mapping name" attribute
            mapping_name = ""
            for attr in task_elem.findall("ATTRIBUTE"):
                if attr.get("NAME") == "Mapping name":
                    mapping_name = attr.get("VALUE", "")
                    break

            if mapping_name:
                m_id = mapping_id(r_name, f_name, mapping_name)
                db.run(
                    """
                    MATCH (s:Session {sessionId: $sId})
                    MATCH (m:Mapping  {mappingId: $mId})
                    MERGE (s)-[:RUNS_MAPPING]->(m)
                    """,
                    sId=s_id, mId=m_id,
                )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Parse an Informatica PowerCenter XML export into Neo4j."
    )
    ap.add_argument("xml_file", help="Path to the Informatica XML export file.")
    ap.add_argument("--uri",      default="bolt://localhost:7687", help="Neo4j bolt URI.")
    ap.add_argument("--user",     default="neo4j",                help="Neo4j username.")
    ap.add_argument("--password", default=None,                   help="Neo4j password.")
    ap.add_argument("--dry-run",  action="store_true",            help="Parse only — print counts without writing to Neo4j.")
    ap.add_argument("--debug",    action="store_true",            help="Enable debug logging.")
    args = ap.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    if not Path(args.xml_file).exists():
        logger.error("File not found: %s", args.xml_file)
        sys.exit(1)

    if args.dry_run:
        parser = InformaticaXMLParser(dry_run=True)
    else:
        if not args.password:
            ap.error("--password is required when not using --dry-run")
        if not _NEO4J_AVAILABLE:
            logger.error("neo4j package not installed. Run: pip install neo4j")
            sys.exit(1)
        try:
            parser = InformaticaXMLParser(args.uri, args.user, args.password)
        except Exception as exc:
            logger.error("Cannot connect to Neo4j at %s: %s", args.uri, exc)
            sys.exit(1)

    try:
        parser.parse_and_load(args.xml_file)
    finally:
        parser.close()


if __name__ == "__main__":
    main()
