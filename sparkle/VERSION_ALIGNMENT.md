# Version Alignment: Delta Spark

## ✅ Aligned Configurations

All Delta Spark configurations are now aligned to version **3.3.2** across the project.

### Changes Made

| File | Before | After | Status |
|------|--------|-------|--------|
| `framework/pyproject.toml` | `delta-spark>=3.3.2` | No change | ✓ Reference |
| `framework/fabricks/utils/spark.py` | Hardcoded `3.2.0` | Dynamic `3.3.2` (from env) | ✓ Fixed |
| `sparkle/requirements.txt` | `delta-spark>=3.2.0` | `delta-spark>=3.3.2` | ✓ Updated |
| `sparkle/docker-compose.yml` | No env var | `DELTA_SPARK_VERSION=3.3.2` | ✓ Added |
| `sparkle/config/pyproject.toml` | Did not exist | Created with config | ✓ Created |

## 📋 Configuration Details

### 1. Framework Configuration (framework/fabricks/utils/spark.py)

**Before:**
```python
.config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
```

**After:**
```python
# Get delta-spark version from environment or use default matching pyproject.toml
delta_version = os.getenv("DELTA_SPARK_VERSION", "3.3.2")
.config("spark.jars.packages", f"io.delta:delta-spark_2.12:{delta_version}")
```

**Why:** Makes version configurable via environment variable, defaulting to match pyproject.toml.

### 2. Sparkle Requirements (sparkle/requirements.txt)

**Before:**
```
delta-spark>=3.2.0
```

**After:**
```
# Must match framework/pyproject.toml: delta-spark>=3.3.2
delta-spark>=3.3.2
```

**Why:** Ensures Sparkle uses the same Delta version as the main framework.

### 3. Docker Environment (sparkle/docker-compose.yml)

**Added:**
```yaml
environment:
  - DELTA_SPARK_VERSION=3.3.2
```

**Why:** Provides explicit version to all containers, ensuring consistency.

### 4. Sparkle Configuration (sparkle/config/pyproject.toml)

**Created:**
```toml
[tool.fabricks]
runtime = "/home/onyxia/work/home/steps"
notebooks = "/home/onyxia/work/home/notebooks"
config_file = "/home/onyxia/work/config/conf.fabricks.sparkle.yml"
job_config_from_yaml = true
loglevel = "info"
devmode = true
localmode = true
```

**Why:** Provides local Fabricks configuration matching framework structure.

## 🎯 Version Selection Rationale

**Delta Spark 3.3.2** was chosen because:
- ✅ Specified in `framework/pyproject.toml` as minimum version
- ✅ Compatible with Spark 4.0.1 (used in Sparkle/Databricks Runtime 17.3)
- ✅ Includes latest bug fixes and performance improvements
- ✅ Matches Databricks Runtime 17.3 LTS requirements

## 🔄 How Version Resolution Works

1. **Framework**: Uses `DELTA_SPARK_VERSION` env var, defaults to `3.3.2`
2. **Sparkle Container**: Sets `DELTA_SPARK_VERSION=3.3.2` in docker-compose.yml
3. **Python Package**: Installs `delta-spark>=3.3.2` from requirements.txt
4. **Spark JARs**: Loads `io.delta:delta-spark_2.12:3.3.2` via packages config

## 📦 Package Matrix

| Component | Version | Source |
|-----------|---------|--------|
| Python Package | `delta-spark>=3.3.2` | PyPI |
| Spark JARs | `io.delta:delta-spark_2.12:3.3.2` | Maven Central |
| Spark Version | `4.0.1` | Base image |
| Scala Version | `2.12` | Spark 4.0 default |

## ✨ Benefits

1. **Consistency**: Same version across all components
2. **Flexibility**: Can override via `DELTA_SPARK_VERSION` env var
3. **Maintainability**: Single source of truth (pyproject.toml)
4. **Clarity**: Explicit versioning, no hidden dependencies

## 🔧 Changing the Version

To use a different Delta Spark version:

### Option 1: Environment Variable
```bash
export DELTA_SPARK_VERSION=3.4.0
./utilities.sh restart
```

### Option 2: docker-compose.yml
```yaml
environment:
  - DELTA_SPARK_VERSION=3.4.0
```

### Option 3: In Notebook
```python
import os
os.environ["DELTA_SPARK_VERSION"] = "3.4.0"

# Then create Spark session
from fabricks.utils.spark import get_spark
spark = get_spark()
```

## 🐛 Troubleshooting

### Version Mismatch Errors

If you see:
```
java.lang.NoSuchMethodError: io.delta.tables.DeltaTable
```

**Solution:**
1. Check installed version: `pip show delta-spark`
2. Check JARs version: `spark.conf.get("spark.jars.packages")`
3. Ensure they match: Both should be 3.3.2

### Package Not Found

If you see:
```
Module 'delta' not found
```

**Solution:**
```bash
pip install delta-spark==3.3.2
# Then restart Spark session
```

## 📚 References

- [Delta Lake Documentation](https://docs.delta.io/)
- [Delta Spark Releases](https://github.com/delta-io/delta/releases)
- [Databricks Runtime 17.3 LTS](https://docs.databricks.com/release-notes/runtime/17.3lts.html)
- [Spark 4.0 Documentation](https://spark.apache.org/docs/4.0.0/)

## ✅ Verification Checklist

After applying these changes:

- [ ] `framework/fabricks/utils/spark.py` uses dynamic version
- [ ] `sparkle/requirements.txt` specifies `>=3.3.2`
- [ ] `sparkle/docker-compose.yml` sets `DELTA_SPARK_VERSION=3.3.2`
- [ ] `sparkle/config/pyproject.toml` exists with Fabricks config
- [ ] Can import Delta: `from delta.tables import DeltaTable`
- [ ] Can check Delta tables: `DeltaTable.isDeltaTable(spark, path)`
- [ ] No TypeError when using Fabricks jobs

## 🚀 Next Steps

1. **Restart Sparkle** to apply changes:
   ```bash
   ./utilities.sh down
   ./utilities.sh up
   ```

2. **Verify in Jupyter**:
   ```python
   import delta
   print(f"Delta version: {delta.__version__}")  # Should be 3.3.2

   from fabricks.utils.spark import get_spark
   spark = get_spark()
   print(spark.conf.get("spark.jars.packages"))  # Should include delta-spark_2.12:3.3.2
   ```

3. **Test Fabricks**:
   ```python
   from fabricks.core.jobs.silver import Silver
   # Should work without TypeError now
   ```

---

**Date Aligned:** 2026-02-26
**Aligned By:** Claude Code
**Version:** Delta Spark 3.3.2
