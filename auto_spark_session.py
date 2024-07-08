import findspark
findspark.init()  # reportedly finds the correct spark version

import os
import json
import pyspark

from typing import Optional, Any, List, Tuple, Union

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



# OK, how this works is this:
# When we create a SparkConf, that is the Python side representation of the config.
# When we call SparkSession.builder() with the SparkConf, it creates the JVM and
# all the relevant JavaObjects containing the Scala side representation of the config.
# Attached to the SparkSession is a RuntimeConfig object which acts as the API for
# for Python to get/set config options with the JavaObject during runtime. So, what we
# do is to intercept any calls to this RuntimeConfig, mask any containing secrets, and
# passing any other calls to the API without modification.

# XXX Wondering if we can do something neat like encrypt the file with a cert?

class WrapRuntimeConfig(RuntimeConfig):
    """RuntimeConfig interface wrapper

    When a user makes a request to the RuntimeConfig for a key which is likely to contain
    a secret, we mask the value returned. All other requests are passed unaltered.
    """
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._mask_keys = ["secret", "password", "passwd"]
        self._mask = "*******"

    # XXX How to handle version differences?
    def get(self, key: str, defaultValue: Union[Optional[str], _NoValueType] = _NoValue) -> Optional[str]:
        if any(m in key for m in self._mask_keys):
            return f"{self._mask}"
        return super().get(key, defaultValue)

    # XXX I noticed getAll() was available on newer versions



class ServicePrincipal:
    # XXX Is this the right place for this?
    _basedir_path = "/tmp/stuff/service_principals"
    # XXX check if we can do the following:
    # 1. Have a /path/to/some/HASH/sp_dir
    # 2. Change the ownership to root from /path/to and down to the leaves
    # 3. Then chmod 640 as well.
    # 4. Then see if we can read the absolute path, if we know it, but cannot actually
    #    browse to it in a directory.

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
    def from_files(cls, name: str) -> "ServicePrincipal":
        file_path = f"{name}.service-principal.json"
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
    def from_secret(cls, name: str) -> "ServicePrincipal":
        def parse_secret() -> Dict[str, str]:
            pass

        return cls(**parse_secret())
        # XXX IMPL me.
        # XXX XXX XXX Actually, we should just mount the secret in the same place this is looking for it?
        # Reportedly, we can create a secret with the same name but different values
        # depending on which K8s Namespace the Job is launched in. Therefore, we should
        # be able to 
        raise NotImplementedError("IMPL ME")
        return cls

    @classmethod
    def auto(cls, name: str) -> "ServicePrincipal":
        try:
            return cls.from_secret(name)
        except Exception as e:
            return cls.from_files(name)


class AutoSparkSession(SparkSession):
    def __init__(self, *args: Any, **kwargs: Any):
        pass
        #super().__init__(*args, **kwargs)

    # XXX Is this the correct use of encapsulation?

    @classmethod
    def from_storage_account(cls, storage_account_name: str) -> "AutoSparkSession":
        service_principal = cls.__get_service_principal()
        c = {
            "spark.jars.packages": "org.apache.hadoop:hadoop-azure:3.4.0",
            f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net": "OAuth",
            f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net":
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net":
                service_principal.application_id,
            f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net":
                service_principal.client_secret_value,
            f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net":
            f"https://login.microsoftonline.com/{service_principal.directory_id}/oauth2/token",
        }
        # Build our SparkSession
        spark_session = SparkSession.builder.config(map=c).getOrCreate()
        # Create a new RuntimeConfig backed by the stolen JavaObject Conf
        new_conf = WrapRuntimeConfig(spark_session.conf._jconf)
        # Replace reference in the SparkSession to our new RuntimeConfig
        spark_session._conf = new_conf
        return spark_session

    def __get_service_principal() -> ServicePrincipal:
        # XXX Remove this function, it shouldn't be callable even though it is hidden-ish
        # XXX So, we need a way to map the Workgroup to the ServicePrincipal name...
        # Or do we...!? Maybe we just key off of the WorkgroupName
        service_principal_name = "search-rec"
        # XXX TODO, get this from the environment somewhere
        return ServicePrincipal.auto(service_principal_name) 
