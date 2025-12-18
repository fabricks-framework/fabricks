# CDC Templates Documentation

This directory contains Jinja2 SQL templates used by the Fabricks CDC (Change Data Capture) system to generate queries for handling data changes across different CDC strategies (NoCDC, SCD1, SCD2).

## Directory Structure

```
templates/
├── ctes/           # Common Table Expression templates
├── filters/        # Filter logic for slicing and updating data
├── macros/         # Reusable Jinja2 macros
├── merges/         # MERGE statement templates for each CDC type
├── queries/        # Query assembly templates for each CDC type
├── filter.sql.jinja    # Main filter orchestration
├── merge.sql.jinja     # Main merge orchestration
└── query.sql.jinja     # Main query orchestration
```

## Main Templates

### query.sql.jinja
Main template that orchestrates the complete query generation process. It assembles various CTEs and query components based on the CDC type and configuration.

**Included Components:**
- Context information (via `queries/context.sql.jinja`)
- Base CTE (via `ctes/base.sql.jinja`)
- Optional slice CTE (via `ctes/slice.sql.jinja`)
- Optional deduplication CTEs (key and hash)
- Optional current state CTE (for update mode)
- Optional rectification CTE
- CDC-specific query logic (NoCDC, SCD1, or SCD2)
- Final output CTE

**Parameters:**
- `slice`: Type of slice filter ("update" or "latest")
- `deduplicate_key`: Enable key-based deduplication
- `deduplicate_hash`: Enable hash-based deduplication
- `mode`: Operation mode ("update" or "complete")
- `has_rows`: Whether target table has existing rows
- `rectify`: Enable data rectification
- `cdc`: CDC type ("nocdc", "scd1", or "scd2")

### merge.sql.jinja
Main template for generating MERGE statements to apply changes to target tables.

**Included Components:**
- `merges/scd1.sql.jinja` - For SCD Type 1 merges
- `merges/scd2.sql.jinja` - For SCD Type 2 merges
- `merges/nocdc.sql.jinja` - For tables without CDC

**Parameters:**
- `cdc`: CDC type determining which merge template to use

### filter.sql.jinja
Main template for generating filter queries to determine which data slices to process.

**Included Components:**
- Base CTE (via `ctes/base.sql.jinja`)
- Update filter (via `filters/update.sql.jinja`)
- Latest filter (via `filters/latest.sql.jinja`)
- Final aggregation (via `filters/final.sql.jinja`)

**Parameters:**
- `slice`: Type of slice ("update" or "latest")

## CTEs (Common Table Expressions)

### ctes/base.sql.jinja
Creates the base CTE that reads from the source and prepares the data with necessary transformations.

**Features:**
- Handles multiple source formats (query, table, global_temp_view, dataframe)
- Applies column casting
- Adds calculated columns
- Adds system columns (__timestamp, __operation, __last_updated, __source, __hash, __key, __metadata)
- Supports column overwriting
- Applies WHERE filter

**Parameters:**
- `format`: Source format type
- `src`: Source reference
- `cast`: Dictionary of columns to cast with target types
- `overwrite`: List of columns to overwrite
- `add_calculated_columns`: List of calculated column expressions
- `add_timestamp`, `add_operation`, `add_last_updated`, `add_source`, `add_hash`, `add_key`, `add_metadata`: Flags to add system columns
- `hashes`: List of columns to include in hash calculation
- `keys`: List of columns to include in key calculation
- `filter_where`: WHERE clause filter

### ctes/slice.sql.jinja
Filters data based on timestamp and source slices.

**Parameters:**
- `parent_slice`: Parent CTE name to slice from
- `slices`: Slice condition expression
- `has_source`: Whether source tracking is enabled

### ctes/deduplicate_key.sql.jinja
Removes duplicate records based on key and timestamp, keeping the most relevant record based on priority.

**Features:**
- Prioritizes delete operations over upserts
- Supports custom ordering for tie-breaking
- Advanced mode adds explicit row numbering; simple mode uses QUALIFY

**Parameters:**
- `parent_deduplicate_key`: Parent CTE to deduplicate
- `has_source`: Whether source tracking is enabled
- `advanced_deduplication`: Use advanced deduplication logic
- `has_order_by`: Whether custom ordering is specified
- `order_duplicate_by`: List of columns for ordering duplicates

### ctes/deduplicate_hash.sql.jinja
Removes consecutive duplicate records based on hash value changes.

**Features:**
- Detects when hash or operation changes from previous record
- Preserves only records where values differ from previous
- Advanced mode uses explicit LAG; simple mode uses QUALIFY

**Parameters:**
- `parent_deduplicate_hash`: Parent CTE to deduplicate
- `has_source`: Whether source tracking is enabled
- `advanced_deduplication`: Use advanced deduplication logic

### ctes/current.sql.jinja
Retrieves the current state from the target table for update operations.

**Features:**
- Handles different timestamp columns per CDC type (SCD2 uses __valid_from)
- Refreshes __timestamp, __last_updated, __hash, __key as needed
- Filters by __is_current flag for SCD1/SCD2
- Applies source and update filters

**Parameters:**
- `intermediates`: List of intermediate columns to select
- `tgt`: Target table name
- `cdc`: CDC type
- `add_timestamp`, `add_last_updated`, `add_hash`, `add_key`: Flags for column refresh
- `has_no_data`: Whether treating as delete operation
- `soft_delete`: Whether soft delete is enabled
- `sources`: Source filter condition
- `update_where`: Additional WHERE clause

### ctes/rectify.sql.jinja
Corrects historical data inconsistencies, particularly handling deleted records that reappear in subsequent reloads.

**Features:**
- Detects records deleted before reloads but present in later reloads
- Generates synthetic delete operations to maintain consistency
- Handles cross-reload data validation
- Filters out redundant current operations in update mode

**Logic Flow:**
1. Combines base records with current state (update mode)
2. Identifies next operation for each record
3. Tracks reload timestamps
4. Determines if records are deleted before next reload or missing in next reload
5. Generates appropriate delete operations

**Parameters:**
- `mode`: Operation mode ("update" or "complete")
- `parent_rectify`: Parent CTE to rectify
- `intermediates`: List of intermediate columns
- `has_rows`: Whether target has existing rows
- `has_source`: Whether source tracking is enabled

## Filters

### filters/update.sql.jinja
Generates filter conditions to select only new or updated records since the last load.

**Features:**
- Determines maximum timestamp from target table
- Generates slice conditions for records newer than max timestamp
- Handles different timestamp columns per CDC type
- Supports multi-source filtering

**Parameters:**
- `parent_slice`: Parent CTE name
- `tgt`: Target table name
- `cdc`: CDC type
- `has_source`: Whether source tracking is enabled

### filters/latest.sql.jinja
Generates filter conditions to select only the most recent timestamp per source.

**Features:**
- Finds maximum timestamp per source
- Creates slice conditions for latest data only

**Parameters:**
- `parent_slice`: Parent CTE name
- `has_source`: Whether source tracking is enabled

### filters/final.sql.jinja
Aggregates slice and source filter conditions using OR logic.

**Parameters:**
- `has_source`: Whether source tracking is enabled

## Macros

### macros/hash.sql.jinja
Defines macros for generating hash and key values.

**Macros:**
- `add_hash(fields)`: Creates MD5 hash from specified fields, treating __operation specially (deletes get different hash)
- `add_key(fields)`: Creates MD5 hash for key columns

**Features:**
- Uses array concatenation with '*' delimiter and '-1' null replacement
- Casts all fields to string
- Special handling for __operation field in hashes

### macros/backtick.sql.jinja
Simple macro to wrap field names in backticks for proper SQL escaping.

## Merge Templates

### merges/nocdc.sql.jinja
Generates MERGE statement for tables without CDC tracking.

**Features:**
- Matches on key columns
- Supports upsert and delete operations
- No historical tracking

**Parameters:**
- `format`: Source format ("dataframe" or "view")
- `tgt`: Target table name
- `src`: Source reference
- `has_key`: Whether to use __key column for matching
- `keys`: List of key columns for matching
- `has_source`: Whether source tracking is enabled
- `update_where`: Additional WHERE clause
- `columns`: List of all columns to merge

### merges/scd1.sql.jinja
Generates MERGE statement for SCD Type 1 (overwrite) tracking.

**Features:**
- Updates records in place
- Maintains __is_current and __is_deleted flags
- Supports soft delete option
- Updates metadata timestamps

**Parameters:**
- `format`: Source format
- `tgt`, `src`: Target and source references
- `has_key`, `keys`, `has_source`: Matching configuration
- `fields`: Data fields to update
- `has_timestamp`, `has_last_updated`, `has_metadata`, `has_hash`, `has_rescued_data`: System column flags
- `soft_delete`: Enable soft delete instead of physical delete
- `columns`: All columns for insert

### merges/scd2.sql.jinja
Generates MERGE statement for SCD Type 2 (versioned history) tracking.

**Features:**
- Closes current records by setting __valid_to
- Inserts new versions with __valid_from
- Maintains __is_current and __is_deleted flags
- Updates records matched with current flag only

**Operations:**
- `update`: Close current version and insert new version
- `delete`: Close current version and mark as deleted
- `insert`: Insert new version

**Parameters:**
- `format`: Source format
- `tgt`, `src`: Target and source references
- `has_key`, `keys`, `has_source`: Matching configuration
- `soft_delete`: Enable soft delete marking
- `has_metadata`, `has_last_updated`: System column flags
- `columns`: All columns for insert

## Query Templates

### queries/context.sql.jinja
Generates a SQL comment block documenting the query configuration and parameters.

**Sections:**
- ⚙️ BASE: CDC type and mode
- 🎯 SOURCE & TARGET: Format and references
- 📊 CTE's: Which CTEs are included
- 🔪 FILTERING: Filter conditions
- 🗑️ DELETES: Delete handling options
- ✅ DATA VALIDATION: Data state flags
- 🏷️ HAS FIELDS: Which system fields are present
- ➕ ADD COLUMNS: Which columns to add
- 🔄 EXTRA COLUMN OPERATIONS: Column transformations
- 👨‍👩‍👧 PARENTS: Parent CTE references
- 📦 LAYOUT: Column lists

### queries/final.sql.jinja
Final SELECT that outputs the result, excluding specified columns.

**Parameters:**
- `all_except`: List of columns to exclude from output

### queries/scd1.sql.jinja
Implements SCD Type 1 logic that maintains only current state.

**Features:**
- Takes latest record per key
- Marks deleted records
- Handles first delete when no upserts exist (update mode)
- Filters out fake updates (records matching current hash)
- Generates merge conditions for upsert/delete operations

**Parameters:**
- `parent_cdc`: Parent CTE name
- `mode`: "complete" or "update"
- `has_source`, `has_rows`: Configuration flags
- `soft_delete`: Enable soft delete
- `rectify`: Whether rectification was applied
- `outputs`: Output columns

### queries/scd2.sql.jinja
Implements SCD Type 2 logic that maintains full version history.

**Features:**
- Creates __valid_from and __valid_to temporal columns
- Assigns validity periods based on next timestamp
- Marks current records (__is_current)
- Identifies deleted records
- Generates merge conditions (insert/update/delete)
- Filters out fake updates
- Optional __valid_from correction to use 1900-01-01 for earliest records

**Parameters:**
- `parent_cdc`: Parent CTE name
- `mode`: "complete" or "update"
- `has_source`, `has_rows`: Configuration flags
- `correct_valid_from`: Correct earliest valid_from date
- `rectify`: Whether rectification was applied
- `outputs`: Output columns

### queries/nocdc/complete.sql.jinja
Generates complete load query for NoCDC mode.

**Features:**
- Selects all output columns
- Filters out 'current' operations if filter enabled

**Parameters:**
- `parent_cdc`: Parent CTE name
- `filter`: Enable operation filtering
- `outputs`: Output columns

### queries/nocdc/update.sql.jinja
Generates incremental update query for NoCDC mode.

**Features:**
- Identifies records to upsert (not matching current hash)
- Optional delete missing records
- Filters out 'current' operations if filter enabled

**Parameters:**
- `parent_cdc`: Parent CTE name
- `has_rows`: Whether target has existing rows
- `delete_missing`: Enable delete for missing records
- `has_source`: Whether source tracking is enabled
- `filter`: Enable operation filtering
- `outputs`: Output columns

## Usage Example

The templates are typically invoked through the CDC classes (NoCDC, SCD1, SCD2) which populate the template variables and render the appropriate templates based on the operation:

```python
from fabricks.cdc import SCD2

cdc = SCD2(
    src="source_table",
    tgt="target_table",
    keys=["id"],
    mode="update"
)

# Generates query using query.sql.jinja and dependencies
query = cdc.render_query()

# Generates merge using merge.sql.jinja and dependencies
merge = cdc.render_merge()
```

## Template Parameters Reference

### Common Parameters

- `cdc`: CDC type ("nocdc", "scd1", "scd2")
- `mode`: Operation mode ("complete", "update")
- `format`: Source format ("query", "table", "global_temp_view", "dataframe")
- `src`: Source table/query reference
- `tgt`: Target table name
- `keys`: List of key columns for matching records
- `hashes`: List of columns to include in hash calculation

### System Column Flags

- `has_timestamp`, `add_timestamp`: Timestamp tracking
- `has_last_updated`, `add_last_updated`: Last updated timestamp
- `has_operation`, `add_operation`: Operation type (upsert/delete/reload)
- `has_source`, `add_source`: Source system tracking
- `has_hash`, `add_hash`: Row hash for change detection
- `has_key`, `add_key`: Composite key hash
- `has_metadata`, `add_metadata`: Metadata struct (inserted/updated times)
- `has_identity`: Identity column present
- `has_rescued_data`: Rescued data column present

### Processing Options

- `deduplicate_key`: Enable key-based deduplication
- `deduplicate_hash`: Enable hash-based deduplication
- `advanced_deduplication`: Use explicit window functions
- `rectify`: Enable data rectification
- `soft_delete`: Enable soft delete (mark as deleted vs physical delete)
- `delete_missing`: Delete records not in source
- `slice`: Slice type ("update", "latest")
- `filter`: Enable operation filtering

### Data State Flags

- `has_rows`: Target table has existing rows
- `has_no_data`: Treating as empty/delete operation
- `has_order_by`: Custom ordering specified

### Column Lists

- `columns`: All columns in target table
- `inputs`: Input columns from source
- `intermediates`: Intermediate processing columns
- `outputs`: Final output columns
- `fields`: Data fields (non-system columns)
- `order_duplicate_by`: Columns for ordering duplicates
- `all_except`: Columns to exclude from output
- `all_overwrite`: Columns to overwrite
- `overwrite`: Columns to overwrite (subset)
- `cast`: Dictionary of column type casts
- `add_calculated_columns`: Calculated column expressions

### Filter Conditions

- `filter_where`: WHERE clause for base data
- `update_where`: WHERE clause for update operations
- `slices`: Slice condition expression
- `sources`: Source filter condition

### Parent CTE References

- `parent_slice`: Parent CTE for slicing
- `parent_rectify`: Parent CTE for rectification
- `parent_deduplicate_key`: Parent CTE for key deduplication
- `parent_deduplicate_hash`: Parent CTE for hash deduplication
- `parent_cdc`: Parent CTE for CDC logic
- `parent_final`: Parent CTE for final output

### SCD2-Specific

- `correct_valid_from`: Correct earliest __valid_from to 1900-01-01

## Template Rendering Flow

### Query Rendering (query.sql.jinja)

1. **Context** - Document configuration in SQL comment
2. **Base CTE** - Load and transform source data
3. **Slice CTE** (optional) - Filter to specific time slices
4. **Deduplicate Key CTE** (optional) - Remove key duplicates
5. **Current CTE** (optional, update mode) - Load existing target state
6. **Rectify CTE** (optional) - Fix historical inconsistencies
7. **Deduplicate Hash CTE** (optional) - Remove hash duplicates
8. **CDC Logic CTE** - Apply NoCDC/SCD1/SCD2 logic
9. **Final CTE** - Select output columns

### Merge Rendering (merge.sql.jinja)

1. Select appropriate merge template based on CDC type
2. Generate MERGE statement with ON clause
3. Define WHEN MATCHED and WHEN NOT MATCHED clauses
4. Specify UPDATE, DELETE, and INSERT operations

### Filter Rendering (filter.sql.jinja)

1. **Base CTE** - Load source metadata
2. **Update/Latest Filter CTE** - Determine slice conditions
3. **Final CTE** - Aggregate filter expressions
