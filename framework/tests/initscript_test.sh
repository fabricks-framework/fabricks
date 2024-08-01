DIRNAME=$(dirname "$0")
FRAMEWORK_DIR=$(pwd "$DIRNAME/../")
sudo echo PYTHONPATH=$PYTHONPATH:$FRAMEWORK_DIR >> /databricks/driver/conf/.bashrc
sudo echo FABRICKS_RUNTIME=$DIRNAME/runtime >> /etc/environment
sudo echo FABRICKS_NOTEBOOKS=$FRAMEWORK_DIR/fabricks/api/notebooks >> /etc/environment
sudo echo FABRICKS_IS_DEBUG=TRUE >> /etc/environment
sudo echo FABRICKS_IS_TEST=TRUE >> /etc/environment
sudo echo FABRICKS_IS_LIVE=TRUE >> /etc/environment
sudo echo FABRICKS_VERSION=dev >> /etc/environment

/databricks/python/bin/pip install pytest pytest-order pyyaml  sqlglot jinja2 polars  azure-data-tables  azure-storage-queue azure-identity python-dotenv
