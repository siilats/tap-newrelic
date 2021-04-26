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
    latest_timestamp = None

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
            "query": self.nqrl_query.format(
                next_page_token.strftime("%Y-%m-%d %H:%M:%S"),
                self.get_replication_key_signpost(partition=partition).strftime("%Y-%m-%d %H:%M:%S"),
            )
        }

    def prepare_request_payload(
        self, partition: Optional[dict], next_page_token: Optional[DateTimeType] = None
    ) -> Optional[dict]:
        res = super().prepare_request_payload(partition, next_page_token)
        res["query"] = self.query # TODO: GraphQLStream wraps the query in `query { }` but we need args
        nqrl = res["variables"]["query"]
        self.logger.debug(f"nqrl: {nqrl}")
        return res

    def get_next_page_token(self, response, previous_token):
        latest = pendulum.parse(self.latest_timestamp)
        if self.results_count == 0:
            return None
        if previous_token and latest == previous_token:
            return None

        return latest

    def transform_row(self, row: dict) -> dict:
        row["timestamp"] = str(pendulum.from_timestamp(row["timestamp"] / 1000))
        return { inflection.underscore(k): v for k, v in row.items() }

    def parse_response(self, response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        resp_json = response.json()
        try:
            results = resp_json["data"]["actor"]["account"]["nrql"]["results"]
            self.results_count = len(results)
            for row in results:
                latest_row = self.transform_row(row)
                if self.latest_timestamp and latest_row["timestamp"] < self.latest_timestamp:
                    # Because NRQL doesn't take timestamps down to miliseconds, sometimes you get
                    # duplicate rows from the same second which breaks GraphQLStream's detection
                    # of out-of-order rows. We can simply skip these rows because they've already
                    # been posted
                    continue
                self.latest_timestamp = latest_row["timestamp"]
                yield latest_row
        except Exception as err:
            self.logger.warn(f"Problem with response: {resp_json}")
            raise err

class SyntheticCheckStream(NewRelicStream):
    name = "synthetic_checks"

    nqrl_query = "SELECT * FROM SyntheticCheck SINCE '{}' UNTIL '{}' ORDER BY timestamp LIMIT MAX"

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
        Property("error", StringType),
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
