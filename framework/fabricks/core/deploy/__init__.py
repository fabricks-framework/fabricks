from fabricks.core.deploy.tables import deploy_tables
from fabricks.core.deploy.views import deploy_views


class deploy:
    @staticmethod
    def tables(drop: bool = False):
        deploy_tables(drop=drop)

    @staticmethod
    def views():
        deploy_views()
