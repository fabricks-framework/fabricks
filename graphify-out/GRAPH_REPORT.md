# Graph Report - framework/fabricks  (2026-07-03)

## Corpus Check
- Corpus is ~41,550 words - fits in a single context window. You may not need a graph.

## Summary
- 1418 nodes · 2449 edges · 139 communities (119 shown, 20 thin omitted)
- Extraction: 82% EXTRACTED · 18% INFERRED · 0% AMBIGUOUS · INFERRED: 448 edges (avg confidence: 0.64)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_DAG Processing & Scheduling|DAG Processing & Scheduling]]
- [[_COMMUNITY_Gold Layer & SCD0 CDC|Gold Layer & SCD0 CDC]]
- [[_COMMUNITY_Data Quality Checks|Data Quality Checks]]
- [[_COMMUNITY_CDC Implementations (NoCDCSCD1SCD2)|CDC Implementations (NoCDC/SCD1/SCD2)]]
- [[_COMMUNITY_DAG Base & Logging|DAG Base & Logging]]
- [[_COMMUNITY_Data Reading & Streaming|Data Reading & Streaming]]
- [[_COMMUNITY_Job Protocol|Job Protocol]]
- [[_COMMUNITY_Job Configurator|Job Configurator]]
- [[_COMMUNITY_Bronze Layer Ingestion|Bronze Layer Ingestion]]
- [[_COMMUNITY_Masks & Deploy|Masks & Deploy]]
- [[_COMMUNITY_Extenders & Parsers|Extenders & Parsers]]
- [[_COMMUNITY_Table Generator Mixin|Table Generator Mixin]]
- [[_COMMUNITY_Delta Table Operations|Delta Table Operations]]
- [[_COMMUNITY_Step Config & Invoker|Step Config & Invoker]]
- [[_COMMUNITY_Runtime Config Models|Runtime Config Models]]
- [[_COMMUNITY_CDC SQL Templates|CDC SQL Templates]]
- [[_COMMUNITY_Config Settings Models|Config Settings Models]]
- [[_COMMUNITY_Metastore Database|Metastore Database]]
- [[_COMMUNITY_Runtime Deployment & Views|Runtime Deployment & Views]]
- [[_COMMUNITY_Legacy Step Mixins|Legacy Step Mixins]]
- [[_COMMUNITY_Job Retrieval|Job Retrieval]]
- [[_COMMUNITY_Job Dependencies|Job Dependencies]]
- [[_COMMUNITY_Community 22|Community 22]]
- [[_COMMUNITY_Community 23|Community 23]]
- [[_COMMUNITY_Community 24|Community 24]]
- [[_COMMUNITY_Community 25|Community 25]]
- [[_COMMUNITY_Community 26|Community 26]]
- [[_COMMUNITY_Community 27|Community 27]]
- [[_COMMUNITY_Community 28|Community 28]]
- [[_COMMUNITY_Community 29|Community 29]]
- [[_COMMUNITY_Community 30|Community 30]]
- [[_COMMUNITY_Community 31|Community 31]]
- [[_COMMUNITY_Community 32|Community 32]]
- [[_COMMUNITY_Community 33|Community 33]]
- [[_COMMUNITY_Community 34|Community 34]]
- [[_COMMUNITY_Community 35|Community 35]]
- [[_COMMUNITY_Community 36|Community 36]]
- [[_COMMUNITY_Community 37|Community 37]]
- [[_COMMUNITY_Community 38|Community 38]]
- [[_COMMUNITY_Community 39|Community 39]]
- [[_COMMUNITY_Community 40|Community 40]]
- [[_COMMUNITY_Community 41|Community 41]]
- [[_COMMUNITY_Community 42|Community 42]]
- [[_COMMUNITY_Community 43|Community 43]]
- [[_COMMUNITY_Community 44|Community 44]]
- [[_COMMUNITY_Community 45|Community 45]]
- [[_COMMUNITY_Community 46|Community 46]]
- [[_COMMUNITY_Community 47|Community 47]]
- [[_COMMUNITY_Community 48|Community 48]]
- [[_COMMUNITY_Community 49|Community 49]]
- [[_COMMUNITY_Community 50|Community 50]]
- [[_COMMUNITY_Community 51|Community 51]]
- [[_COMMUNITY_Community 52|Community 52]]
- [[_COMMUNITY_Community 53|Community 53]]
- [[_COMMUNITY_Community 54|Community 54]]
- [[_COMMUNITY_Community 55|Community 55]]
- [[_COMMUNITY_Community 56|Community 56]]
- [[_COMMUNITY_Community 57|Community 57]]
- [[_COMMUNITY_Community 58|Community 58]]
- [[_COMMUNITY_Community 59|Community 59]]
- [[_COMMUNITY_Community 60|Community 60]]
- [[_COMMUNITY_Community 61|Community 61]]
- [[_COMMUNITY_Community 62|Community 62]]
- [[_COMMUNITY_Community 63|Community 63]]
- [[_COMMUNITY_Community 64|Community 64]]
- [[_COMMUNITY_Community 65|Community 65]]
- [[_COMMUNITY_Community 66|Community 66]]
- [[_COMMUNITY_Community 68|Community 68]]
- [[_COMMUNITY_Community 70|Community 70]]
- [[_COMMUNITY_Community 71|Community 71]]
- [[_COMMUNITY_Community 72|Community 72]]
- [[_COMMUNITY_Community 73|Community 73]]
- [[_COMMUNITY_Community 126|Community 126]]
- [[_COMMUNITY_Community 127|Community 127]]
- [[_COMMUNITY_Community 128|Community 128]]
- [[_COMMUNITY_Community 134|Community 134]]

## God Nodes (most connected - your core abstractions)
1. `Table` - 93 edges
2. `CdcContext` - 58 edges
3. `JobProtocol` - 52 edges
4. `Bronze` - 49 edges
5. `Gold` - 43 edges
6. `ExtenderOptions` - 37 edges
7. `SparkOptions` - 36 edges
8. `FileSharePath` - 36 edges
9. `Silver` - 35 edges
10. `BaseStep` - 31 edges

## Surprising Connections (you probably didn't know these)
- `BaseCDC` --uses--> `Database`  [INFERRED]
  cdc/base.py → metastore/database.py
- `BaseCDC` --uses--> `Table`  [INFERRED]
  cdc/base.py → metastore/table.py
- `CDCAbstract` --uses--> `Table`  [INFERRED]
  cdc/cdc_abc.py → metastore/table.py
- `CDCAbstract` --uses--> `CdcContext`  [INFERRED]
  cdc/cdc_abc.py → models/cdc.py
- `CdcProtocol` --uses--> `Database`  [INFERRED]
  cdc/mixins/_protocol.py → metastore/database.py

## Import Cycles
- 1-file cycle: `utils/sqlglot.py -> utils/sqlglot.py`

## Hyperedges (group relationships)
- **Query rendering flow (base to final CTE chain)** — framework_fabricks_cdc_templates_readme_query_sql_jinja, framework_fabricks_cdc_templates_readme_base_cte, framework_fabricks_cdc_templates_readme_slice_cte, framework_fabricks_cdc_templates_readme_rectify_cte, framework_fabricks_cdc_templates_readme_queries_final [EXTRACTED 0.90]
- **Three CDC strategies and their query/merge templates** — framework_fabricks_cdc_templates_readme_nocdc, framework_fabricks_cdc_templates_readme_scd1, framework_fabricks_cdc_templates_readme_scd2, framework_fabricks_cdc_templates_readme_merge_sql_jinja, framework_fabricks_cdc_templates_readme_query_sql_jinja [EXTRACTED 0.85]

## Communities (139 total, 20 thin omitted)

### Community 0 - "DAG Processing & Scheduling"
Cohesion: 0.06
Nodes (17): DagProcessor, Any, _get_access_key_from_os(), _get_access_key_from_secret_scope(), get_connection_info(), get_table(), TableClient, TableServiceClient (+9 more)

### Community 1 - "Gold Layer & SCD0 CDC"
Cohesion: 0.06
Nodes (22): SparkSession, SCD0, Gold, DataFrame, Row, StepGoldConf, StepGoldOptions, Direct access to typed gold job options. (+14 more)

### Community 2 - "Data Quality Checks"
Cohesion: 0.07
Nodes (28): CheckerMixin, CheckException, CheckWarning, CustomException, PostRunCheckException, PostRunCheckWarning, PostRunInvokeException, PreRunCheckException (+20 more)

### Community 3 - "CDC Implementations (NoCDC/SCD1/SCD2)"
Cohesion: 0.08
Nodes (18): NoCDC, SparkSession, SparkSession, SCD1, SparkSession, SCD2, BaseStep, _create_db_object() (+10 more)

### Community 4 - "DAG Base & Logging"
Cohesion: 0.05
Nodes (19): BaseDags, DataFrame, DagGenerator, DataFrame, DagTerminator, generate(), process(), DataFrame (+11 more)

### Community 5 - "Data Reading & Streaming"
Cohesion: 0.06
Nodes (30): AllowedIOModes, _ensure_spark(), DataFrame, SparkSession, StructType, Drive `func` exactly once over `df` through a trigger-once writeStream.      Use, read(), read_batch() (+22 more)

### Community 6 - "Job Protocol"
Cohesion: 0.05
Nodes (7): JobProtocol, AllowedModes, DataFrame, StepBronzeConf, StepGoldConf, StepSilverConf, TOptions

### Community 7 - "Job Configurator"
Cohesion: 0.06
Nodes (12): AllowedChangeDataCaptures, ConfiguratorMixin, AllowedModes, StepBronzeConf, StepGoldConf, StepSilverConf, Direct access to typed runtime conf., Direct access to typed runtime options from context configuration. (+4 more)

### Community 8 - "Bronze Layer Ingestion"
Cohesion: 0.09
Nodes (3): Bronze, DataFrame, Parses the data based on the specified mode and returns a DataFrame.          Ar

### Community 9 - "Masks & Deploy"
Cohesion: 0.08
Nodes (18): get_masks(), is_registered(), SparkSession, register_all_masks(), register_mask(), get_step(), Deploy, Modes (+10 more)

### Community 10 - "Extenders & Parsers"
Cohesion: 0.09
Nodes (21): extender(), get_extender(), ExtenderMixin, DataFrame, BaseParser, DataFrame, SparkSession, Retrieves and processes data from the specified data path using the provided sch (+13 more)

### Community 11 - "Table Generator Mixin"
Cohesion: 0.09
Nodes (11): GeneratorMixin, DataFrame, Truncates the job by removing all data associated with it.          This method, Drops the current job and its dependencies.          This method drops the curre, Get a table option value with fallback priority: job options → step options → de, Creates a table or view based on the specified mode.          If `persist` is Tr, Register the job.          If `persist` is True, the job's table is registered., Creates or replaces a view.          This method is responsible for creating or (+3 more)

### Community 13 - "Step Config & Invoker"
Cohesion: 0.14
Nodes (28): get_step_conf(), InvokerOptions, Common types and type aliases used across all models., Grouped invoker operations for pre/run/post execution., Configuration for runtime updaters., Options for registering tables., RegisterOptions, UpdaterOptions (+20 more)

### Community 14 - "Runtime Config Models"
Cohesion: 0.18
Nodes (30): Database, ExtenderOptions, Spark SQL and configuration options., Configuration for runtime extenders., Database configuration., SparkOptions, MaskOptions, Runtime configuration models. (+22 more)

### Community 15 - "CDC SQL Templates"
Cohesion: 0.08
Nodes (31): ctes/base.sql.jinja, CDC Templates, Change Data Capture (CDC), ctes/current.sql.jinja, ctes/deduplicate_hash.sql.jinja, ctes/deduplicate_key.sql.jinja, Deduplication (key and hash), filter.sql.jinja (+23 more)

### Community 16 - "Config Settings Models"
Cohesion: 0.08
Nodes (19): BaseSettings, FieldInfo, ConfigOptions, PydanticBaseSettingsSource, Configuration models., Set default notebooks path if not provided., Get all paths resolved as Path objects.          Args:             runtime: The, Get all paths resolved as Path objects. (+11 more)

### Community 17 - "Metastore Database"
Cohesion: 0.09
Nodes (6): Database, DataFrame, SparkSession, DbObject, Column, SparkSession

### Community 18 - "Runtime Deployment & Views"
Cohesion: 0.16
Nodes (22): deploy_runtime(), Deploy runtime to the fabricks.runtime view., deploy_variables(), Deploy variables to the fabricks.variables view., create_or_replace_dbojects_view(), create_or_replace_dependencies_circular_view(), create_or_replace_dependencies_flat_view(), create_or_replace_dependencies_unpivot_view() (+14 more)

### Community 19 - "Legacy Step Mixins"
Cohesion: 0.08
Nodes (5): LegacyStepMixin, Backward-compat aliases for _internal methods removed in refactor.     Mix in be, DataFrame, Modes, StepProtocol

### Community 20 - "Job Retrieval"
Cohesion: 0.13
Nodes (23): GenericOptions, _get_job(), get_jobs(), get_jobs_internal(), get_jobs_internal_df(), get_jobs_sorted(), JobConfGeneric, DataFrame (+15 more)

### Community 21 - "Job Dependencies"
Cohesion: 0.11
Nodes (15): AllowedOrigins, Row, get_job_conf(), get_job_conf_internal(), Row, JobConf, JobDependency, Job dependency tracking models. (+7 more)

### Community 22 - "Community 22"
Cohesion: 0.09
Nodes (8): Row, StepSilverConf, StepSilverOptions, Direct access to typed silver job options., Direct access to typed silver step conf., Direct access to typed silver step options., Silver, JobSilverOptions

### Community 23 - "Community 23"
Cohesion: 0.10
Nodes (17): BaseModel, Direct access to typed step-level table options from context configuration., DatabasePathOptions, Path configuration for databases., ForeignKey, ForeignKeyOptions, PrimaryKey, PrimaryKeyOptions (+9 more)

### Community 24 - "Community 24"
Cohesion: 0.12
Nodes (15): get_schedule(), get_schedules(), get_schedules_df(), DataFrame, Any, Core YAML reading utilities with automatic variable substitution from context., Read YAML files with automatic variable substitution from runtime context., read_yaml() (+7 more)

### Community 25 - "Community 25"
Cohesion: 0.15
Nodes (6): AllowedTemplates, CdcProtocol, AllowedSources, Any, DataFrame, Protocol

### Community 26 - "Community 26"
Cohesion: 0.11
Nodes (10): BasePath, Walk the path and return all files., Return the JSON representation of the path., Create a path from a URI with optional regex substitution., Get the string representation of the path., Get the file name from the path., Read and return SQL content from a .sql file., Check if the path points to a SQL file. (+2 more)

### Community 27 - "Community 27"
Cohesion: 0.15
Nodes (3): ConfiguratorMixin, AllowedSources, DataFrame

### Community 28 - "Community 28"
Cohesion: 0.17
Nodes (11): UDFMixin, get_extension(), get_udfs(), is_registered(), SparkSession, Register all user-defined functions (UDFs)., Register a user-defined function (UDF)., register_all_udfs() (+3 more)

### Community 29 - "Community 29"
Cohesion: 0.17
Nodes (17): BaseInvokerOptions, Options for invoking notebooks during pre/post run operations., BronzeOptions, GoldOptions, Step configuration models., Grouped invoker operations for pre/run/post execution., Optional timeout overrides for individual steps., Path configuration for steps. (+9 more)

### Community 30 - "Community 30"
Cohesion: 0.21
Nodes (14): add_catalog_to_spark(), add_credentials_to_spark(), add_spark_options_to_spark(), build_spark_session(), init_spark_session(), SparkSession, SparkSession, RemoteDbUtils (+6 more)

### Community 31 - "Community 31"
Cohesion: 0.17
Nodes (7): DataFrame, DataFrameLike, create_or_replace_global_temp_view(), Any, DataFrame, View, SparkSessionLike

### Community 32 - "Community 32"
Cohesion: 0.30
Nodes (5): GeneratorMixin, AllowedSources, Any, DataFrame, CdcContext

### Community 33 - "Community 33"
Cohesion: 0.14
Nodes (11): Any, Perform variable substitution during parsing.          Loads variables from path, find_upward(), Find a file by searching upward through the directory hierarchy.      Args:, Path, PathlibPath, Path utilities without Spark dependencies., Legacy Path class with assume_git flag for backward compatibility. (+3 more)

### Community 34 - "Community 34"
Cohesion: 0.17
Nodes (9): Runs a notebook located at the given path.      Args:         path (GitPath): Th, run_notebook(), GitPath, PathlibPath, Check if the path exists in the local/git file system., Get the notebook path for Databricks workspace., Recursively yield all file paths in the git/local file system., Resolve a path as a GitPath with optional variable substitution and base path jo (+1 more)

### Community 35 - "Community 35"
Cohesion: 0.14
Nodes (4): JobConfig, StepBronzeConf, StepGoldConf, StepSilverConf

### Community 36 - "Community 36"
Cohesion: 0.18
Nodes (3): JobABC, DataFrame, TOptions

### Community 37 - "Community 37"
Cohesion: 0.22
Nodes (4): CDCAbstract, AllowedSources, Any, DataFrame

### Community 38 - "Community 38"
Cohesion: 0.31
Nodes (5): _previous_cte(), ProcessorMixin, AllowedSources, DataFrame, QueryContext

### Community 39 - "Community 39"
Cohesion: 0.26
Nodes (4): AddedColumn, ChangedColumn, DroppedColumn, SchemaDiff

### Community 40 - "Community 40"
Cohesion: 0.20
Nodes (7): BaseCDC, SparkSession, ConfiguratorMixin, BaseJob, Row, GeneratorMixin, ProcessorMixin

### Community 42 - "Community 42"
Cohesion: 0.24
Nodes (6): run(), Row, get_job(), get_job_internal(), Row, Retrieve a job based on the provided parameters.      Args:         step (Option

### Community 43 - "Community 43"
Cohesion: 0.25
Nodes (10): _as_variables(), load_variables(), Any, Utility functions for runtime configuration parsing and transformation., Extract variables dictionary from various data structures., Resolve all runtime paths to Path objects.      Note: Variable substitution has, Resolve the path to a variables file from config data., Load variables from external file or inline dict.      Priority order (first non (+2 more)

### Community 44 - "Community 44"
Cohesion: 0.25
Nodes (10): perform_variable_substitution(), Perform variable substitution on runtime config data.      Args:         data: R, build_variable_lookup(), _build_variable_lookup_cached(), Any, Shared variable substitution utilities., Build a lookup dictionary for variable substitution (cached internal implementat, Build a lookup dictionary for variable substitution. (+2 more)

### Community 45 - "Community 45"
Cohesion: 0.20
Nodes (4): Mutable, must query fresh., Mutable, must query fresh., Mutable, must query fresh., Mutable, must query fresh.

### Community 46 - "Community 46"
Cohesion: 0.25
Nodes (4): Get a table property value from the cache. Returns None if the property is not s, Immutable, safe to cache., Immutable, safe to cache., Immutable, safe to cache.

### Community 47 - "Community 47"
Cohesion: 0.54
Nodes (7): BronzeJobWrapper, get_job_schema(), GoldJobWrapper, JobWrapper, print_job_schema(), Wrapper for JobConf to generate array schema., SilverJobWrapper

### Community 48 - "Community 48"
Cohesion: 0.43
Nodes (3): MergerMixin, AllowedSources, DataFrame

### Community 49 - "Community 49"
Cohesion: 0.29
Nodes (7): Bronze register mode (default batch), Lazy-import isolation of streaming, legacy.streaming, streaming parsers, read.py, table.py, write.py

### Community 51 - "Community 51"
Cohesion: 0.29
Nodes (4): Self, Join this path with other path segments., Append a string to the path., Get the parent directory of the path.

### Community 54 - "Community 54"
Cohesion: 0.27
Nodes (4): StepBronzeOptions, Direct access to typed bronze job options., Direct access to typed bronze step options., JobBronzeOptions

### Community 55 - "Community 55"
Cohesion: 0.33
Nodes (3): PathlibPath, Recursively yield all file paths under the given path., Get the pathlib representation of the path.

### Community 56 - "Community 56"
Cohesion: 0.40
Nodes (5): Any, Cache YAML file reads with LRU eviction. Max 128 unique file paths cached., Read YAML files from a path with optional variable substitution.      Args:, read_yaml(), _read_yaml_cached()

### Community 59 - "Community 59"
Cohesion: 0.50
Nodes (3): StepBronzeConf, StepGoldConf, StepSilverConf

### Community 60 - "Community 60"
Cohesion: 0.50
Nodes (3): StepBronzeOptions, StepGoldOptions, StepSilverOptions

### Community 61 - "Community 61"
Cohesion: 0.83
Nodes (3): create_or_replace_view(), create_or_replace_view_internal(), create_or_replace_views()

### Community 62 - "Community 62"
Cohesion: 0.67
Nodes (3): get_tables(), get_views(), DataFrame

## Knowledge Gaps
- **20 isolated node(s):** `fabricks-metastore`, `formatter`, `colors`, `Generator`, `CDC Templates` (+15 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **20 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Table` connect `Delta Table Operations` to `CDC Implementations (NoCDC/SCD1/SCD2)`, `DAG Base & Logging`, `Data Reading & Streaming`, `Job Protocol`, `Job Configurator`, `Masks & Deploy`, `Metastore Database`, `Community 25`, `Community 27`, `Community 32`, `Community 37`, `Community 38`, `Community 39`, `Community 40`, `Community 41`, `Community 45`, `Community 46`, `Community 50`, `Community 53`, `Community 58`, `Community 70`?**
  _High betweenness centrality (0.267) - this node is a cross-community bridge._
- **Why does `JobProtocol` connect `Job Protocol` to `Community 32`, `Data Quality Checks`, `Community 35`, `Job Configurator`, `Community 40`, `Extenders & Parsers`, `Table Generator Mixin`, `Delta Table Operations`, `Community 23`, `Community 57`, `Community 28`, `Community 25`?**
  _High betweenness centrality (0.178) - this node is a cross-community bridge._
- **Why does `CdcContext` connect `Community 32` to `Gold Layer & SCD0 CDC`, `CDC Implementations (NoCDC/SCD1/SCD2)`, `Community 36`, `Community 37`, `Community 38`, `Job Protocol`, `Bronze Layer Ingestion`, `Masks & Deploy`, `Table Generator Mixin`, `Community 48`, `Community 53`, `Community 22`, `Community 23`, `Community 25`, `Community 31`?**
  _High betweenness centrality (0.174) - this node is a cross-community bridge._
- **Are the 19 inferred relationships involving `Table` (e.g. with `BaseCDC` and `.__init__()`) actually correct?**
  _`Table` has 19 INFERRED edges - model-reasoned connections that need verification._
- **Are the 21 inferred relationships involving `CdcContext` (e.g. with `CDCAbstract` and `GeneratorMixin`) actually correct?**
  _`CdcContext` has 21 INFERRED edges - model-reasoned connections that need verification._
- **Are the 3 inferred relationships involving `JobProtocol` (e.g. with `JobConfig` and `Table`) actually correct?**
  _`JobProtocol` has 3 INFERRED edges - model-reasoned connections that need verification._
- **Are the 4 inferred relationships involving `Bronze` (e.g. with `NoCDC` and `CdcContext`) actually correct?**
  _`Bronze` has 4 INFERRED edges - model-reasoned connections that need verification._