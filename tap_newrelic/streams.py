"""Stream class for tap-newrelic."""


import requests


from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable


from singer_sdk.streams import GraphQLStream



from singer_sdk.authenticators import (
    APIAuthenticatorBase,
    SimpleAuthenticator,
    OAuthAuthenticator,
    OAuthJWTAuthenticator
)

from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    DateTimeType,
    IntegerType,
    NumberType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")



class NewRelicStream(GraphQLStream):
    """NewRelic stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["api_url"]

    # Alternatively, use a static string for url_base:
    # url_base = "https://api.mysample.com"



    @property
    def authenticator(self) -> APIAuthenticatorBase:
        return SimpleAuthenticator(
            stream=self,
            auth_headers={
                "Private-Token": self.config.get("auth_token")
            }
        )

    # Alternatively, you can pass auth tokens directly within http_headers:
    # @property
    # def http_headers(self) -> dict:
    #     headers = {}
    #     if "user_agent" in self.config:
    #         headers["User-Agent"] = self.config.get("user_agent")
    #     headers["Private-Token"] = self.config.get("auth_token")
    #     return headers






# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.
class UsersStream(NewRelicStream):
    name = "users"
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = PropertiesList(
        Property("name", StringType),
        Property("id", StringType),
        Property("age", IntegerType),
        Property("email", StringType),
        Property(
            "address",
            ObjectType(
                Property("street", StringType),
                Property("city", StringType),
                Property("state", StringType),
                Property("zip", StringType),
            )
        ),
    ).to_dict()
    primary_keys = ["id"]
    replication_key = None
    graphql_query = """
        users {
            name
            id
            age
            email
            address {
                street
                city
                state
                zip
            }
        }
        """


class GroupsStream(NewRelicStream):
    name = "groups"
    schema = PropertiesList(
        Property("name", StringType),
        Property("id", StringType),
        Property("modified", DateTimeType),
    ).to_dict()
    primary_keys = ["id"]
    replication_key = "modified"
    graphql_query = """
        groups {
            name
            id
            modified
        }
        """



