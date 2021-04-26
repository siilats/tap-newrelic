"""Stream class for tap-newrelic."""
import inflection
import pendulum
import requests

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk.streams import GraphQLStream

from singer_sdk.authenticators import (
    APIAuthenticatorBase,
    SimpleAuthenticator,
)

from singer_sdk.typing import (
    BooleanType,
    DateTimeType,
    IntegerType,
    NumberType,
    PropertiesList,
    Property,
    StringType,
)

class NewRelicStream(GraphQLStream):
    """NewRelic stream class."""

    primary_keys = ["id"]
    replication_method = "INCREMENTAL"
    replication_key = "timestamp"
    is_timestamp_replication_key = True
    is_sorted = True

    query = """
        query ($accountId: Int!, $query: Nrql!) {
          actor {
            account(id: $accountId) {
              nrql(query: $query) {
                results
              }
            }
          }
        }
    """

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["api_url"]

    @property
    def authenticator(self) -> APIAuthenticatorBase:
        return SimpleAuthenticator(
            stream=self,
            auth_headers={
                "API-Key": self.config.get("api_key")
            }
        )

    def get_url_params(self, partition, next_page_token: Optional[DateTimeType] = None) -> dict:
        next_page_token = next_page_token or self.get_starting_timestamp(partition)
        return {
            "accountId": self.config.get("account_id"),
            "query": self.nqrl_query.format(next_page_token.strftime("%Y-%m-%d %H:%M:%S"))
        }

    def prepare_request_payload(
        self, partition: Optional[dict], next_page_token: Optional[DateTimeType] = None
    ) -> Optional[dict]:
        res = super().prepare_request_payload(partition, next_page_token)
        res["query"] = self.query # TODO: GraphQLStream wraps the query in `query { }` but we need args
        return res

    def get_next_page_token(self, response, previous_token):
        resp_json = response.json()
        if len(resp_json["data"]["actor"]["account"]["nrql"]["results"]) == 0:
            return None

        # Annoyingly, NRQL's `SINCE` is inclusive, so at the end we get the same token
        # twice
        if previous_token and self.latest_row["timestamp"] <= previous_token:
            return None

        return self.latest_row["timestamp"]

    def transform_row(self, row: dict) -> dict:
        row["timestamp"] = pendulum.from_timestamp(row["timestamp"] / 1000)
        return { inflection.underscore(k): v for k, v in row.items() }

    def parse_response(self, response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        resp_json = response.json()
        for row in resp_json["data"]["actor"]["account"]["nrql"]["results"]:
            self.latest_row = self.transform_row(row)
            yield self.latest_row

    def get_replication_key_signpost(
        self, partition: Optional[dict]
    ):
        # This represents the highest allowed replication_key. I'm sure
        # it's useful for partitioning or something. The SDK sets the
        # default to the time that the stream was started, but most streams
        # including this one continue to get records, so new records may be
        # added while the pipeline runs. Here we override it to explictly
        # disable the feature.
        return None


class SyntheticCheckStream(NewRelicStream):
    name = "synthetic_check"

    nqrl_query = "SELECT * FROM SyntheticCheck SINCE '{}' ORDER BY timestamp LIMIT MAX"

    schema = PropertiesList(
        Property("duration", NumberType),
        Property("entity_guid", StringType),
        Property("has_user_defined_headers", BooleanType),
        Property("id", StringType),
        Property("location", StringType),
        Property("location_label", StringType),
        Property("minion", StringType),
        Property("minion_container_system", StringType),
        Property("minion_container_system_version", StringType),
        Property("minion_deployment_mode", StringType),
        Property("minion_id", StringType),
        Property("monitor_extended_type", StringType), # TODO: enum
        Property("monitor_id", StringType),
        Property("monitor_name", StringType),
        Property("result", StringType), # TODO: enum
        Property("secure_credentials", StringType),
        Property("timestamp", DateTimeType), # TODO: milisecodns epoch
        Property("total_request_body_size", IntegerType),
        Property("total_request_header_size", IntegerType),
        Property("total_response_body_size", IntegerType),
        Property("total_response_header_size", IntegerType),
        Property("type", StringType), # TODO: enum
        Property("type_label", StringType),
    ).to_dict()
