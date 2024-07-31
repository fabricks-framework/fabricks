from databricks.sdk.runtime import dbutils

from fabricks.utils.secret import AccessKey, ApplicationRegistration, Secret


def create_container(storage_account: str, container: str, secret: Secret):
    from azure.core.exceptions import ResourceExistsError
    from azure.identity import ClientSecretCredential
    from azure.storage.blob import BlobServiceClient

    assert isinstance(secret, ApplicationRegistration)

    cred = ClientSecretCredential(
        tenant_id=secret.directory_id,
        client_id=secret.application_id,
        client_secret=secret.secret,
    )

    try:
        blob_service_client = BlobServiceClient(f"https://{storage_account}.blob.core.windows.net", credential=cred)
        blob_service_client.create_container(container)

    except ResourceExistsError:
        pass


def mount_container(storage_account: str, container: str, secret: Secret):
    try:
        dbutils.fs.unmount(f"/mnt/{container}")  # type: ignore

    except Exception:
        pass

    if isinstance(secret, AccessKey):
        dbutils.fs.mount(  # type: ignore
            source=f"wasbs://{container}@{storage_account}.blob.core.windows.net",
            mount_point=f"/mnt/{container}",
            extra_configs={
                f"fs.azure.account.key.{storage_account}.blob.core.windows.net": f"{secret.key}",
            },
        )

    elif isinstance(secret, ApplicationRegistration):
        dbutils.fs.mount(  # type: ignore
            source=f"abfss://{container}@{storage_account}.blob.core.windows.net/",
            mount_point=f"/mnt/{container}",
            extra_configs={
                "fs.azure.account.auth.type": "OAuth",
                "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredentialsTokenProvider",
                "fs.azure.account.oauth2.client.id": secret.application_id,
                "fs.azure.account.oauth2.client.secret": secret.secret,
                "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{secret.directory_id}/oauth2/token",
            },
        )

    else:
        raise ValueError("secret is not valid")
