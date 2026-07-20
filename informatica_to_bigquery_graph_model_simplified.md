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
(:Table)
(:File)
(:Database)
(:SourceDefinition) 
(:TargetDefinition)

```

## Node Properties

### Database

```text
DatabaseId
DBName
databaseType
```

### Table

```text
tableId
TableName
```

### File

```text
fileId
name
Location
Encoding
Delimiters
EscapeCharacter
NullCharacter
PadBytes
QuoteCharacter
SkipRows
```


### SurceDefinition

```text
sourceId
name
```

### TargetDefinition

```text
targetId
name
```


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
(:SourceDefinition)-[:HAS_FIELD]->(:Column:Field)
(:TargetDefinition)-[:HAS_FIELD]->(:Column:Field)
(:Table)-[:HAS_DEFINITION]->(:SourceDefinition)
(:Table)-[:HAS_DEFINITION]->(:TargetDefinition)
(:Table)-[:BELONGS_TO]->(:Database)
(:Database)-[:HAS_TABLE]->(:Table)
(:File)-[:HAS_DEFINITION]->(:SourceDefinition)
(:File)-[:HAS_DEFINITION]->(:TargetDefinition)
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

CREATE CONSTRAINT table_id IF NOT EXISTS
FOR (p:Table)
REQUIRE p.tableId IS UNIQUE;

CREATE CONSTRAINT file_id IF NOT EXISTS
FOR (p:File)
REQUIRE p.fileId IS UNIQUE;

CREATE CONSTRAINT database_id IF NOT EXISTS
FOR (p:Database)
REQUIRE p.databaseId IS UNIQUE;

CREATE CONSTRAINT target_id IF NOT EXISTS
FOR (p:TargetDefinition)
REQUIRE p.targetId IS UNIQUE;

CREATE CONSTRAINT source_id IF NOT EXISTS
FOR (p:SourceDefinition)
REQUIRE p.sourceId IS UNIQUE;

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
targetId = repositoryName + "." + folderName + "." + targetName
sourceId = repositoryName + "." + folderName + "." + sourceName
fileId = repositoryName + "." + folderName + "." + fileName
tableId = databaseType + "." + databaseName + "." + tableName
databaseId = databaseType + "." + databaseName
```

## Core Graph Paths

### Source Table Structure Path

```text
(:Database)-[:HAS_TABLE]->(:Table)-[:HAS_DEFINITION]->(:SourceDefinition)-[:HAS_FIELD]->(:Column:Field)
```

### Target Table Structure Path

```text
(:Database)-[:HAS_TABLE]->(:Table)-[:HAS_DEFINITION]->(:TargetDefinition)-[:HAS_FIELD]->(:Column:Field)
```

### Target File Structure Path

```text
(:File)-[:HAS_DEFINITION]->(:TargetDefinition)-[:HAS_FIELD]->(:Column:Field)
```

### Source File Structure Path

```text
(:File)-[:HAS_DEFINITION]->(:SourceDefinition)-[:HAS_FIELD]->(:Column:Field)
```

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
