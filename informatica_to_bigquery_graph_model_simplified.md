# Informatica to BigQuery Knowledge Graph Model

## Nodes

```text
(:Repository)
(:Folder)
(:Workflow)
(:Session)
(:Mapping)
(:Transformation)
(:Column:Field)
(:Column:Port)
(:Column:Port:InputPort)
(:Column:Port:OutputPort)
(:Column:Port:InputOutputPort)
(:Column:Port:VariablePort)
(:Expression)
(:TransformationProperty)
(:Parameter)
(:ConversionRule)
```

## Node Properties

### Repository

```text
repositoryId
name
version
databaseType
creationDate
```

### Folder

```text
folderId
name
owner
```

### Workflow

```text
workflowId
name
```

### Worklet

```text
workletId
name
IsEnabled
```

### Session

```text
sessionId
name
```

### Mapping

```text
mappingId
name
```

### Transformation

```text
transformationId
name
type
reusable
businessName
```

### Column:Field

```text
fieldId
name
datatype
precision
scale
nullable
keyType
ordinalPosition
```

### Column:Port

```text
portId
name
portType
datatype
precision
scale
defaultValue
ordinalPosition
isVariable
```

### Expression

```text
expressionId
rawExpression
intent
language
```

### TransformationProperty

```text
propertyId
name
value
```

### Parameter

```text
parameterId
name
value
datatype
scope
```

### ConversionRule

```text
ruleId
sourceTechnology
targetTechnology
sourceFunction
targetFunction
ruleType
```



## Relationships

```text
(:Repository)-[:HAS_FOLDER]->(:Folder)
(:Folder)-[:HAS_WORKFLOW]->(:Workflow)
(:Folder)-[:HAS_MAPPING]->(:Mapping)
(:Folder)-[:HAS_WORKLET]->(:Worklet)
(:Workflow)-[:HAS_SESSION]->(:Session)
(:Session)-[:RUNS_MAPPING]->(:Mapping)
(:Mapping)-[:HAS_TRANSFORMATION]->(:Transformation)
(:Transformation)-[:HAS_PORT]->(:Column:Port)
(:Transformation)-[:HAS_PROPERTY]->(:TransformationProperty)
(:Transformation)-[:USES_PARAMETER]->(:Parameter)
(:Column:Port)-[:FLOWS_TO]->(:Column:Port)
(:Column:Port)-[:DERIVES_FROM]->(:Column:Port)
(:Column:Port)-[:USES_EXPRESSION]->(:Expression)
(:Column:Port)-[:USES_PARAMETER]->(:Parameter)
(:Column:Field)-[:BOUND_TO_PORT]->(:Column:Port)
(:Column:Port)-[:BOUND_TO_FIELD]->(:Column:Field)
(:Column:Field)-[:FLOWS_TO_FIELD]->(:Column:Field)
(:ConversionRule)-[:APPLIES_TO_EXPRESSION]->(:Expression)
(:ConversionRule)-[:APPLIES_TO_TRANSFORMATION]->(:Transformation)
```

## Constraints

```cypher
CREATE CONSTRAINT repository_id IF NOT EXISTS
FOR (r:Repository)
REQUIRE r.repositoryId IS UNIQUE;

CREATE CONSTRAINT folder_id IF NOT EXISTS
FOR (f:Folder)
REQUIRE f.folderId IS UNIQUE;

CREATE CONSTRAINT workflow_id IF NOT EXISTS
FOR (w:Workflow)
REQUIRE w.workflowId IS UNIQUE;

CREATE CONSTRAINT session_id IF NOT EXISTS
FOR (s:Session)
REQUIRE s.sessionId IS UNIQUE;

CREATE CONSTRAINT mapping_id IF NOT EXISTS
FOR (m:Mapping)
REQUIRE m.mappingId IS UNIQUE;

CREATE CONSTRAINT transformation_id IF NOT EXISTS
FOR (t:Transformation)
REQUIRE t.transformationId IS UNIQUE;

CREATE CONSTRAINT field_id IF NOT EXISTS
FOR (f:Field)
REQUIRE f.fieldId IS UNIQUE;

CREATE CONSTRAINT port_id IF NOT EXISTS
FOR (p:Port)
REQUIRE p.portId IS UNIQUE;

CREATE CONSTRAINT expression_id IF NOT EXISTS
FOR (e:Expression)
REQUIRE e.expressionId IS UNIQUE;

CREATE CONSTRAINT property_id IF NOT EXISTS
FOR (p:TransformationProperty)
REQUIRE p.propertyId IS UNIQUE;

CREATE CONSTRAINT parameter_id IF NOT EXISTS
FOR (p:Parameter)
REQUIRE p.parameterId IS UNIQUE;

CREATE CONSTRAINT conversion_rule_id IF NOT EXISTS
FOR (r:ConversionRule)
REQUIRE r.ruleId IS UNIQUE;

CREATE CONSTRAINT bigquery_expression_id IF NOT EXISTS
FOR (b:BigQueryExpression)
REQUIRE b.bigqueryExpressionId IS UNIQUE;

CREATE CONSTRAINT unsupported_feature_id IF NOT EXISTS
FOR (u:UnsupportedFeature)
REQUIRE u.featureId IS UNIQUE;

CREATE CONSTRAINT conversion_issue_id IF NOT EXISTS
FOR (i:ConversionIssue)
REQUIRE i.issueId IS UNIQUE;
```

## Unique ID Strategy

```text
repositoryId = repositoryName
folderId = repositoryName + "." + folderName
workflowId = repositoryName + "." + folderName + "." + workflowName
sessionId = repositoryName + "." + folderName + "." + workflowName + "." + sessionName
mappingId = repositoryName + "." + folderName + "." + mappingName
transformationId = repositoryName + "." + folderName + "." + mappingName + "." + transformationName
fieldId = repositoryName + "." + folderName + "." + objectType + "." + objectName + "." + fieldName
portId = repositoryName + "." + folderName + "." + mappingName + "." + transformationName + "." + portName
expressionId = repositoryName + "." + folderName + "." + mappingName + "." + transformationName + "." + portName + ".expression"
propertyId = repositoryName + "." + folderName + "." + mappingName + "." + transformationName + "." + propertyName
parameterId = repositoryName + "." + folderName + "." + parameterScope + "." + parameterName
```

## Core Graph Paths

### Mapping Structure Path

```text
(:Repository)-[:HAS_FOLDER]->(:Folder)-[:HAS_MAPPING]->(:Mapping)-[:HAS_TRANSFORMATION]->(:Transformation)-[:HAS_PORT]->(:Column:Port)
```

### Port Lineage Path

```text
(:Column:Port)-[:FLOWS_TO]->(:Column:Port)
```

### Physical Source to Target Binding Path

```text
(:Column:Field)-[:BOUND_TO_PORT]->(:Column:Port)-[:FLOWS_TO|DERIVES_FROM*]->(:Column:Port)-[:BOUND_TO_FIELD]->(:Column:Field)
```

### Expression Dependency Path

```text
(:Column:Port:OutputPort)-[:DERIVES_FROM]->(:Column:Port:VariablePort)-[:DERIVES_FROM]->(:Column:Port:InputPort)
```

## Minimal MVP Graph

### MVP Nodes

```text
(:Repository)
(:Folder)
(:Mapping)
(:Transformation)
(:Column:Field)
(:Column:Port)
(:Column:Port:InputPort)
(:Column:Port:OutputPort)
(:Column:Port:VariablePort)
(:Expression)
```

### MVP Relationships

```text
(:Repository)-[:HAS_FOLDER]->(:Folder)
(:Folder)-[:HAS_MAPPING]->(:Mapping)
(:Mapping)-[:HAS_TRANSFORMATION]->(:Transformation)
(:Transformation)-[:HAS_PORT]->(:Column:Port)
(:Column:Port)-[:FLOWS_TO]->(:Column:Port)
(:Column:Port)-[:DERIVES_FROM]->(:Column:Port)
(:Column:Port)-[:USES_EXPRESSION]->(:Expression)
(:Column:Field)-[:BOUND_TO_PORT]->(:Column:Port)
(:Column:Port)-[:BOUND_TO_FIELD]->(:Column:Field)
```
