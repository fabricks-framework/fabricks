class TBLPROPERTIES:
    DEFAULT: dict[str, str] = {
        "delta.enableTypeWidening": "true",
        "delta.enableDeletionVectors": "true",
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
        "delta.feature.timestampNtz": "supported",
        "fabricks.last_version": "0",
    }
    POWERBI: dict[str, str] = {
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
        "fabricks.last_version": "0",
    }


class SPARK:
    SQL: dict[str, str] = {
        "spark.databricks.delta.schema.autoMerge.enabled": "true",
        "spark.databricks.delta.resolveMergeUpdateStructsByName.enabled": "true",
    }


class TBLOPTIONS:
    POWERBI: bool = False
    LIQUID_CLUSTERING: bool = False
    PARTITIONING: bool = False
    IDENTITY: bool = False
    RETENTION_DAYS: int = 7


class DEFAULT:
    TIMEOUT: int = 300
    LOGLEVEL: str = "INFO"

    TBLPROPERTIES: TBLPROPERTIES = TBLPROPERTIES  # type: ignore
    TBLOPTIONS: TBLOPTIONS = TBLOPTIONS  # type: ignore
    SPARK: SPARK = SPARK  # type: ignore
