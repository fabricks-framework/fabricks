PARENT=$(dirname "$0")
GRANDPARENT=$(cd "$(dirname "$0")"; cd ../..; pwd)

echo $GRANDPARENT

sudo echo FABRICKS_RUNTIME=$PARENT/runtime >> /etc/environment
sudo echo FABRICKS_NOTEBOOKS=$GRANDPARENT/fabricks/api/notebooks >> /etc/environment
# FABRICKS_CONFIG is deliberately NOT set here -- it comes from the cluster's
# spark_env_vars so databricks.yml's `fabricks_conf` variable is the single
# place that picks conf.fabricks.yml (hive) vs conf.uc.fabricks.yml (UC).

sudo echo FABRICKS_IS_JOB_CONFIG_FROM_YAML=TRUE >> /etc/environment

/databricks/python/bin/pip install pytest pytest-order pyyaml sqlglot jinja2 polars azure-data-tables azure-storage-queue azure-identity python-dotenv tqdm
