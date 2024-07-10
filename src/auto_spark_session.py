import findspark
findspark.init()  # reportedly finds the correct spark version

import os
import json
import pyspark
import determined as det

from typing import Optional, Any, Dict, List, Tuple, Union

from determined.common import api
from determined.experimental import client

from pyspark import SparkConf, _NoValue
from pyspark._globals import _NoValueType
from pyspark.sql import SparkSession
from pyspark.sql.conf import RuntimeConfig
from py4j.java_gateway import JavaObject

# NOTE URL: https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage
# NOTE: A service principal is created in the MS Entra area.
# NOTE: Then the service principal is assigned in IAM section of a storage account with a role attached.
# NOTE: Service Principals can give access to a path inside a container.
# NOTE: There can be multiple client secret keys for a service account.
# NOTE: service principal requires "Storage Blob Data Contributor" role on storage account.
# NOTE: application_id is found here: MS Entra admin center > Applications > App Registrations > Overview: Application (client) ID
# NOTE: directory_id is found here: MS Entra admin center > Applications > App Registrations > Overview: Directory (tenent) ID



def get_current_workspace() -> str:
    D = client.Determined()
    session = D._session

    def get_workspace_name_by_id(wid: int) -> str:
        resp = api.bindings.get_GetWorkspace(session, id=wid)
        return resp.workspace.name

    task_type = os.environ.get("DET_TASK_TYPE")
    if not task_type:
        raise RuntimeError("Could not determine the task type.")

    if task_type.lower() in [t.name for t in api.NTSC_Kind]:
        task_id = os.environ.get("DET_TASK_ID")
        task_kind = getattr(api.NTSC_Kind, task_type.lower())
        deets = api.get_ntsc_details(session, task_kind, task_id)
        return get_workspace_name_by_id(deets.workspaceId)
    else:
        exp_id = os.environ["DET_EXPERIMENT_ID"]
        resp = api.bindings.get_GetExperiment(session, experimentId=int(exp_id))
        return resp.experiment.workspaceName


class WrapRuntimeConfig(RuntimeConfig):
    """RuntimeConfig interface wrapper

    When a user makes a request to the RuntimeConfig for a key which is likely to contain
    a secret, we mask the value returned. All other requests are passed unaltered.
    """
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._mask_keys = ["secret", "password", "passwd"]
        self._mask = "*******"

    def get(self, key: str, defaultValue: Union[Optional[str], _NoValueType] = _NoValue) -> Optional[str]:
        if any(m in key for m in self._mask_keys):
            return f"{self._mask}"
        return super().get(key, defaultValue)
    # XXX I noticed getAll() was available on newer versions



class ServicePrincipal:
    _basedir_path = "/azure/service_principals/"

    def __init__(
        self,
        name: str,
        client_secret_id: str,
        client_secret_value: str,
        application_id: str,
        directory_id: str,
        expires: str
    ) -> None:
        self.name = name
        self.client_secret_id = client_secret_id
        self.client_secret_value = client_secret_value
        self.application_id = application_id
        self.directory_id = directory_id
        self.expires = expires

    @classmethod
    def from_file(cls, key: str) -> "ServicePrincipal":
        file_path = f"{key}.service-principal.json"
        full_path = os.path.join(cls._basedir_path, file_path)

        try:
            with open(full_path, "r") as f:
                return cls(**json.load(f))
        except TypeError as e:
            # got an unexpected keyword argument ''
            raise TypeError(f"Encountered invalid security principal file format: {e}")
        except PermissionError as e:
            # Occurs when a user doesn't have access to the service principal file
            raise PermissionError(
                "You do not have permission to access this service principal. "
                "Please contact your administrator."
            )
        except FileNotFoundError as e:
            # Occurs when we cannot locate the service principal file
            raise RuntimeError(
                "Unable to locate service principal. Please contact your system administrator."
            )
        # XXX What else can happen here?
        return cls

    @classmethod
    def from_secret(cls, key: str) -> "ServicePrincipal":
        def parse_secret() -> Dict[str, str]:
            pass

        raise NotImplementedError("IMPL ME")
        return cls(**parse_secret())

    @classmethod
    def from_workspace(cls) -> "ServicePrincipal":
        service_principal_key = get_current_workspace()
        return cls.from_file(service_principal_key)



def get_spark_session(
    storage_account_name: str,
    conf: Optional[SparkConf] = None
) -> "SparkSession":
    # TODO: wrap all calls and report this must be run on a 
    # det.get_cluster_info() is None, error
    if det.get_cluster_info() is None:
        raise RuntimeError("get_spark_session() must be run on a Determined Cluster")

    service_principal = ServicePrincipal.from_workspace()

    if conf is None:
        conf = SparkConf()

    # When we create a SparkConf, that is the Python side representation of the config.
    # When we call SparkSession.builder() with the SparkConf, it creates the JVM and
    # all the relevant JavaObjects containing the Scala side representation of the config.
    # Attached to the SparkSession is a RuntimeConfig object which acts as the API for
    # for Python to get/set config options with the JavaObject during runtime. So, what we
    # do is to intercept any calls to this RuntimeConfig, mask any containing secrets, and
    # passing any other calls to the API without modification.

    # XXX Nolan, why is this version hard coded?
    conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.4.0")
    conf.set(
        f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
    conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    conf.set(
        f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net",
        service_principal.application_id
    )
    conf.set(
        f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net",
            service_principal.client_secret_value)
    conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net",
        f"https://login.microsoftonline.com/{service_principal.directory_id}/oauth2/token",
    )
    # Build our SparkSession
    spark_session = SparkSession.builder.config(conf=conf).getOrCreate()

    # Create a new RuntimeConfig backed by the stolen JavaObject Conf
    new_conf = WrapRuntimeConfig(spark_session.conf._jconf)

    # Replace reference in the SparkSession to our new RuntimeConfig
    spark_session._conf = new_conf

    return spark_session
