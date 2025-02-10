PARENT=$(dirname "$0")
GRANDPARENT=$(cd "$(dirname "$0")"; cd ../..; pwd)

echo $GRANDPARENT

sudo echo FABRICKS_RUNTIME=$PARENT/runtime >> /etc/environment
sudo echo FABRICKS_NOTEBOOKS=$GRANDPARENT/fabricks/api/notebooks >> /etc/environment

sudo echo FABRICKS_IS_TEST=TRUE >> /etc/environment
sudo echo FABRICKS_IS_LIVE=TRUE >> /etc/environment

/databricks/python/bin/pip install pytest pytest-order pyyaml sqlglot jinja2 polars azure-data-tables azure-storage-queue azure-identity python-dotenv
