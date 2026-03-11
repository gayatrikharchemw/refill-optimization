from functools import cached_property
import logging
from pathlib import Path
from typing import Dict
import os


import yaml
from cmappclient import CMAPPClient
from mw_vault import Vault
from s3path import S3Path
from outreach_api_client import OutreachAPIClient
from niceclient import niceclient as nc

from cmappmongo.authentication import MongoDBConfig, get_mongo_client

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

class ConfigError(Exception): 
    pass

class HumanaRefillConfig:
    def __init__(self, config_file):
        self.config = self.__load_config(config_file)

    def __set_paths(self, config: Dict):
        is_s3 = config["fs"] == "s3"

        for item in config["paths"]:
            if is_s3:
                config["paths"][item] = S3Path(config["paths"][item])
            else:
                config["paths"][item] = Path(config["paths"][item])

    def __load_config(self, config_file: Path) -> Dict:
        with config_file.open("r") as fp:
            config = yaml.safe_load(fp)

        self.__set_paths(config)

        return config

    def __getitem__(self, key):
        return self.config[key]

    @cached_property
    def vault(self) -> Vault:
        return Vault()

    def get_cmapp_client(self) -> CMAPPClient:
        cmapp_config = self.config["cmapp"]

        cmapp_creds = self.vault.get_secret(cmapp_config["vault_path"])

        client = CMAPPClient(
            host=cmapp_config["host"],
            client=cmapp_config["client"],
            username=cmapp_creds["username"],
            password=cmapp_creds["password"],
            num_request_retries=15,
            backoff_factor=10,
            refresh=False,
        )

        return client

class Config:
    """
    Configuration class
    """

    def __init__(self, config_path: Path):
        self.config_path = config_path
        self.config = self.__load_config()

    def __load_config(self) -> Dict:
        """
        Load the configuration file
        """
        with open(self.config_path, "r", encoding="utf-8") as file:
            config = yaml.safe_load(file)
        return config

    def __getitem__(self, key):
        return self.config[key]

    def __contains__(self, key):
        return key in self.config

    @cached_property
    def vault(self) -> Vault:
        """
        Vault object
        """
        return Vault(auto_renew_token=True)

    def set_aws_creds(self) -> Dict:
        """
        Set AWS credentials in env variables
        """
        if "aws" in self.config:
            aws_config = self.config["aws"]
            aws_creds = self.vault.get_secret(aws_config["vault_path"])

            os.environ["AWS_ACCESS_KEY_ID"] = aws_creds["AWS_ACCESS_KEY_ID"]
            os.environ["AWS_SESSION_TOKEN"] = aws_creds["AWS_SESSION_TOKEN"]
            os.environ["AWS_SECRET_ACCESS_KEY"] = aws_creds["AWS_SECRET_ACCESS_KEY"]
        else:
            raise KeyError("AWS configuration not found in config file")

    def get_mongo_client(self, mongo_config: Dict | None = None, ttl: int = 1200, cache: bool = True):
        """
        Get a MongoDB client

        Args:
            ttl: Time to live for the credentials
            cache: Whether to cache the credentials
        """
        if not mongo_config:
            mongo_config = self.config["mongodb"]

        mongo_creds = self.vault.get_database_credentials(
            path=mongo_config["vault_path"], cache=cache
        )

        mongo_creds.lease.renew(increment=ttl)

        uri = mongo_config["uri"]

        mongo_login_config = MongoDBConfig(
            username=mongo_creds["username"],
            password=mongo_creds["password"],
            uri=uri,
            database=mongo_config["db"],
        )

        client = get_mongo_client(
            mongodb_config=mongo_login_config, num_retries=25, retry_delay_seconds=5
        )

        return client

    def get_cmapp_client(self, cmapp_config: Dict | None = None):
        """
        Get a CMAPP client
        """
        if not cmapp_config:
            cmapp_config = self.config["cmapp"]
        creds = Vault(auto_renew_token=True).get_secret(cmapp_config["vault_path"])
        return CMAPPClient(
            host=cmapp_config["host"],
            client=cmapp_config["client"],
            username=creds["username"],
            password=creds["password"],
            refresh=False,
        )

    def get_outreach_db_engine(self) -> Engine:
        """
        Returns the Outreach DB client from the config.
        """
        if "outreach_db" not in self.config:
            raise ConfigError("No section 'outreach_db' in config file")

        outreach_db_config = self.config["outreach_db"]

        missing_keys = []
        for key in ["vault_path", "host", "port", "db"]:
            if key not in outreach_db_config:
                missing_keys.append(key)

        if missing_keys:
            raise ConfigError(
                f"Missing keys in 'outreach_db' section of config file: {', '.join(missing_keys)}"
            )

        outreach_db_creds = self.vault.get_database_credentials(
            outreach_db_config["vault_path"]
        )

        missing_cred_keys = []
        for key in ["username", "password"]:
            if key not in outreach_db_creds:
                missing_cred_keys.append(key)

        if missing_cred_keys:
            raise ConfigError(
                f"Missing keys in Outreach DB credentials: {', '.join(missing_cred_keys)}"
            )

        outreach_db_engine = create_engine(
            (
                f"postgresql+psycopg://{outreach_db_creds['username']}:"
                f"{outreach_db_creds['password']}@"
                f"{outreach_db_config['host']}:"
                f"{outreach_db_config['port']}/"
                f"{outreach_db_config['db']}"
            ),
        )

        return outreach_db_engine
    

    def get_nice_client(
        self, host: str = "https://api-c38.nice-incontact.com/"
    ) -> nc.Client:
        """
        Returns the Nice client from the config.
        """
        if "nice" not in self.config:
            raise ConfigError("No section 'nice' in config file")

        nice_config = self.config["nice"]

        if "vault_path" not in nice_config:
            raise ConfigError("No 'vault_path' in 'nice' section of config file")

        nice_cxone_api_creds = self.vault.get_secret(nice_config["vault_path"])

        missing_keys = []
        for key in [
            "access_key_id",
            "access_secret_key",
            "client_id",
            "client_secret",
        ]:
            if key not in nice_cxone_api_creds:
                missing_keys.append(key)

        if missing_keys:
            raise ConfigError(
                f"Missing keys in Nice CXone API credentials: {', '.join(missing_keys)}"
            )

        nice_client = nc.Client(
            username=nice_cxone_api_creds["access_key_id"],
            password=nice_cxone_api_creds["access_secret_key"],
            client_id=nice_cxone_api_creds["client_id"],
            client_secret=nice_cxone_api_creds["client_secret"],
            host=host,
        )

        return nice_client

    def get_outreach_api_client(self) -> OutreachAPIClient:
        outreach_api_config = self.config["outreach"]["api"]

        vault_path = outreach_api_config["vault_path"]
        scope = outreach_api_config["scope"]
        host = outreach_api_config["host"]

        outreach_api_creds = self.vault.get_secret(vault_path)

        return OutreachAPIClient(
            username=outreach_api_creds["username"],
            password=outreach_api_creds["password"],
            scope=scope,
            host=host,
        )
