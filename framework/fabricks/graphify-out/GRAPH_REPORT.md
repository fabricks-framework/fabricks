# Graph Report - fabricks  (2026-07-03)

## Corpus Check
- 179 files · ~41,615 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 259 nodes · 416 edges · 12 communities (9 shown, 3 thin omitted)
- Extraction: 99% EXTRACTED · 1% INFERRED · 0% AMBIGUOUS · INFERRED: 3 edges (avg confidence: 0.7)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `37271190`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- [[_COMMUNITY_Bronze|Bronze]]
- [[_COMMUNITY_Gold|Gold]]
- [[_COMMUNITY_Silver|Silver]]
- [[_COMMUNITY_Table|Table]]
- [[_COMMUNITY_DagProcessor|DagProcessor]]
- [[_COMMUNITY_.get_property|.get_property]]
- [[_COMMUNITY_DataFrame|DataFrame]]
- [[_COMMUNITY_.overwrite_schema|.overwrite_schema]]
- [[_COMMUNITY_ProcessorMixin|ProcessorMixin]]
- [[_COMMUNITY_FileSharePath|FileSharePath]]
- [[_COMMUNITY_.from_step_topic_item|.from_step_topic_item]]

## God Nodes (most connected - your core abstractions)
1. `Table` - 68 edges
2. `Bronze` - 44 edges
3. `Gold` - 37 edges
4. `Silver` - 30 edges
5. `DagProcessor` - 16 edges
6. `ProcessorMixin` - 7 edges
7. `run()` - 5 edges
8. `Direct access to typed bronze job options.` - 1 edges
9. `Direct access to typed bronze step conf.` - 1 edges
10. `Direct access to typed bronze step options.` - 1 edges

## Surprising Connections (you probably didn't know these)
- `Silver` --uses--> `Bronze`  [INFERRED]
  core/jobs/silver.py → core/jobs/bronze.py
- `run()` --references--> `Bronze`  [EXTRACTED]
  core/dags/run.py → core/jobs/bronze.py
- `run()` --references--> `Gold`  [EXTRACTED]
  core/dags/run.py → core/jobs/gold.py
- `run()` --references--> `Silver`  [EXTRACTED]
  core/dags/run.py → core/jobs/silver.py

## Import Cycles
- None detected.

## Communities (12 total, 3 thin omitted)

### Community 0 - "Bronze"
Cohesion: 0.05
Nodes (13): Bronze, CdcContext, DataFrame, FileSharePath, JobDependency, Row, Parses the data based on the specified mode and returns a DataFrame.          Ar, Direct access to typed bronze job options. (+5 more)

### Community 1 - "Gold"
Cohesion: 0.06
Nodes (15): BaseJob, Gold, CdcContext, DataFrame, JobDependency, Row, Direct access to typed gold job options., Direct access to typed gold step conf. (+7 more)

### Community 2 - "Silver"
Cohesion: 0.08
Nodes (11): CdcContext, DataFrame, JobDependency, Row, Direct access to typed silver job options., Direct access to typed silver step conf., Direct access to typed silver step options., Silver (+3 more)

### Community 3 - "Table"
Cohesion: 0.09
Nodes (3): DbObject, DeltaTable, Table

### Community 4 - "DagProcessor"
Cohesion: 0.19
Nodes (6): Any, AzureQueue, AzureTable, BaseDags, DagProcessor, run()

### Community 5 - ".get_property"
Cohesion: 0.11
Nodes (8): Mutable, must query fresh., Mutable, must query fresh., Get a table property value from the cache. Returns None if the property is not s, Immutable, safe to cache., Immutable, safe to cache., Immutable, safe to cache., Mutable, must query fresh., Mutable, must query fresh.

### Community 6 - "DataFrame"
Cohesion: 0.14
Nodes (3): ForeignKey, DataFrame, PrimaryKey

### Community 8 - "ProcessorMixin"
Cohesion: 0.27
Nodes (4): ProcessorMixin, DataFrame, Run the processor.          Args:             retry (bool, optional): Whether to, JobProtocol

## Knowledge Gaps
- **3 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Bronze` connect `Bronze` to `Gold`, `Silver`, `DagProcessor`?**
  _High betweenness centrality (0.213) - this node is a cross-community bridge._
- **Why does `Gold` connect `Gold` to `DagProcessor`?**
  _High betweenness centrality (0.198) - this node is a cross-community bridge._
- **Why does `run()` connect `DagProcessor` to `Bronze`, `Gold`, `Silver`?**
  _High betweenness centrality (0.159) - this node is a cross-community bridge._
- **What connects `Direct access to typed bronze job options.`, `Direct access to typed bronze step conf.`, `Direct access to typed bronze step options.` to the rest of the system?**
  _20 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Bronze` be split into smaller, more focused modules?**
  _Cohesion score 0.05387205387205387 - nodes in this community are weakly interconnected._
- **Should `Gold` be split into smaller, more focused modules?**
  _Cohesion score 0.06448979591836734 - nodes in this community are weakly interconnected._
- **Should `Silver` be split into smaller, more focused modules?**
  _Cohesion score 0.08108108108108109 - nodes in this community are weakly interconnected._