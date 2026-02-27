# Fix: Py4JError - io.delta.tables.DeltaTable does not exist in the JVM

## 🐛 The Error

```
Py4JError: io.delta.tables.DeltaTable does not exist in the JVM
```

## 🔍 Root Cause

**Spark 4.0.1 uses Scala 2.13**, but we were loading Delta Lake JARs compiled for Scala 2.12!

```python
# WRONG - Scala 2.12 doesn't work with Spark 4.0
.config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.2")

# CORRECT - Scala 2.13 matches Spark 4.0
.config("spark.jars.packages", "io.delta:delta-spark_2.13:3.3.2")
```

## ✅ The Fix

### What Changed

1. **framework/fabricks/utils/spark.py**: Now uses Scala 2.13 for Spark 4.0+
2. **sparkle/docker-compose.yml**: Added `SCALA_VERSION=2.13` environment variable

### Spark Version → Scala Version Mapping

| Spark Version | Scala Version | Delta Package |
|---------------|---------------|---------------|
| 3.0 - 3.5     | 2.12          | `delta-spark_2.12` |
| 4.0+          | 2.13          | `delta-spark_2.13` |

## 🚀 Quick Fix (In Running Container)

If you have an active Spark session, restart it with the correct Scala version:

```python
# Stop existing session
try:
    spark.stop()
except:
    pass

# Create new session with Scala 2.13
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Fabricks") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:3.3.2") \
    .config("spark.sql.warehouse.dir", "/home/onyxia/work/data/warehouse") \
    .getOrCreate()

# Verify Delta is loaded
from delta.tables import DeltaTable
print("✓ Delta Lake loaded successfully")
```

## 🏗️ Permanent Fix

### Option 1: Use Fabricks Utils (Recommended)

The framework now auto-detects the correct Scala version:

```python
import os
os.environ["FABRICKS_LOCALMODE"] = "true"
os.environ["SCALA_VERSION"] = "2.13"

from fabricks.utils.spark import get_spark
spark = get_spark()
```

### Option 2: Restart Sparkle

The docker-compose.yml now sets the correct environment variable:

```bash
cd sparkle
./utilities.sh down
./utilities.sh up
```

## 🔍 Verifying the Fix

```python
# Check Spark version
print(f"Spark version: {spark.version}")  # Should be 4.0.1

# Check loaded packages
packages = spark.conf.get("spark.jars.packages")
print(f"Packages: {packages}")
# Should contain: delta-spark_2.13:3.3.2

# Test Delta Lake
from delta.tables import DeltaTable
import shutil

test_path = "/tmp/delta_test"
shutil.rmtree(test_path, ignore_errors=True)

spark.range(10).write.format("delta").save(test_path)
is_delta = DeltaTable.isDeltaTable(spark, test_path)
print(f"✓ Delta working: {is_delta}")

shutil.rmtree(test_path, ignore_errors=True)
```

## 📋 Environment Variables

| Variable | Value | Purpose |
|----------|-------|---------|
| `FABRICKS_LOCALMODE` | `true` | Enable local mode |
| `DELTA_SPARK_VERSION` | `3.3.2` | Delta Lake version |
| `SCALA_VERSION` | `2.13` | Scala version for Spark 4.0+ |

## 🐛 Why This Happens

When you specify a JAR dependency with `spark.jars.packages`:

1. Spark downloads the JAR from Maven Central
2. The JAR must match Spark's Scala version
3. If versions don't match, the classes aren't available in the JVM
4. Python tries to access Java classes via Py4J → Error!

## 📦 Maven Coordinates Explained

```
io.delta:delta-spark_2.13:3.3.2
│      │  │              │     └─ Version
│      │  │              └─────── Scala version (MUST match Spark!)
│      │  └────────────────────── Artifact ID
│      └───────────────────────── Group ID
└──────────────────────────────── Organization
```

## 🔧 Troubleshooting

### Error: "Could not find artifact"

If you see:
```
Could not find artifact io.delta:delta-spark_2.13:jar:3.3.2
```

**Solution**: Check Delta Lake version compatibility:
- Delta 3.3.2 requires Spark 4.0+
- For Spark 3.x, use Delta 2.x with Scala 2.12

### Error: "ClassNotFoundException"

If Delta imports work but operations fail:
```
java.lang.ClassNotFoundException: io.delta.sql.DeltaSparkSessionExtension
```

**Solution**: Ensure extensions are configured:
```python
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
```

### Error: Still getting Py4JError

**Solution**: Clear cached JARs and restart:
```bash
# In container
rm -rf ~/.ivy2/cache/io.delta/
rm -rf ~/.ivy2/jars/

# Restart Spark session
python -c "from pyspark.sql import SparkSession; SparkSession.getActiveSession().stop()"
```

Then create a fresh session.

## 📚 References

- [Spark 4.0 Release Notes](https://spark.apache.org/releases/spark-release-4-0-0.html)
- [Delta Lake Quick Start](https://docs.delta.io/latest/quick-start.html)
- [Maven: Delta Spark Packages](https://mvnrepository.com/artifact/io.delta/delta-spark)

## ✅ Version Matrix

| Component | Version | Notes |
|-----------|---------|-------|
| Spark | 4.0.1 | From base image |
| Scala | 2.13 | Spark 4.0 default |
| Delta Spark (Python) | 3.3.2 | From pip |
| Delta Spark (JARs) | 2.13:3.3.2 | From Maven |

## 🎯 Summary

- **Problem**: Wrong Scala version (2.12) for Spark 4.0
- **Solution**: Use Scala 2.13 for Delta Lake JARs
- **Fix**: Updated spark.py and docker-compose.yml
- **Result**: Delta Lake classes now load correctly in JVM

---

**Date Fixed:** 2026-02-26
**Issue:** Scala version mismatch
**Resolution:** Use delta-spark_2.13 for Spark 4.0+
