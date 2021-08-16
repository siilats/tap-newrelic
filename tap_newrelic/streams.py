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

def unix_timestamp_to_iso8601(timestamp):
    return str(pendulum.from_timestamp(timestamp / 1000))

class NewRelicStream(GraphQLStream):
    """NewRelic stream class."""

    primary_keys = []#["id"]
    replication_method = "INCREMENTAL"
    replication_key = "timestamp"
    is_timestamp_replication_key = True
    is_sorted = True
    latest_timestamp = None

    datetime_format = "%Y-%m-%d %H:%M:%S"
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
        nqrl = self.nqrl_query.format(
            next_page_token.strftime(self.datetime_format),
            self.get_replication_key_signpost(partition).strftime(self.datetime_format),
        )
        self.logger.debug(nqrl)
        return {
            "accountId": self.config.get("account_id"),
            "query": nqrl,
        }

    def get_next_page_token(self, response, previous_token):
        latest = pendulum.parse(self.latest_timestamp)
        if self.results_count == 0:
            return None
        if previous_token and latest == previous_token:
            return None

        return latest

    def parse_response(self, response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        resp_json = response.json()
        try:
            results = resp_json["data"]["actor"]["account"]["nrql"]["results"]
            self.results_count = len(results)
            for row in results:
                latest_row = self.transform(row)
                if self.latest_timestamp and latest_row["timestamp"] < self.latest_timestamp:
                    # Because NRQL doesn't take timestamps down to miliseconds, sometimes you get
                    # duplicate rows from the same second which breaks GraphQLStream's detection
                    # of out-of-order rows. We can simply skip these rows because they've already
                    # been posted
                    self.logger.info(f"skipping duplicate {latest_row['timestamp']}")
                    continue
                self.latest_timestamp = latest_row["timestamp"]
                yield latest_row
        except Exception as err:
            self.logger.warn(f"Problem with response: {resp_json}")
            raise err

    def transform(self, row: dict, partition: Optional[dict] = None) -> dict:
        row["timestamp"] = unix_timestamp_to_iso8601(row["timestamp"])
        return { inflection.underscore(k): v for k, v in row.items() }

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
        Property("timestamp", DateTimeType),
        Property("total_request_body_size", IntegerType),
        Property("total_request_header_size", IntegerType),
        Property("total_response_body_size", IntegerType),
        Property("total_response_header_size", IntegerType),
        Property("type", StringType), # TODO: enum
        Property("type_label", StringType),
    ).to_dict()

class MobileAppStream(NewRelicStream):
    name = "mobile_app"
    primary_keys = [ "timestamp", "event_id"]  # ["id"]
    schema = PropertiesList(
        Property("action", StringType),
        Property("action_type", StringType),
        Property("appointment_status", StringType),
        Property("app_mode", StringType),
        Property("app_build", StringType),
        Property("app_id", IntegerType),
        Property("app_name", StringType),
        Property("app_version", StringType),
        Property("app_version_id", IntegerType),
        Property("architecture", StringType),
        Property("ar_mode", StringType),
        Property("asn", StringType),
        Property("asn_owner", StringType),
        Property("brand", StringType),
        Property("camera_permission_state", StringType),
        Property("carrier", StringType),
        Property("category", StringType),
        Property("change_to_state", StringType),
        Property("changed_to_state", StringType),
        Property("compression_quality", StringType),
        Property("connectivity_status", StringType),
        Property("city", StringType),
        Property("coaching_completed", StringType),
        Property("coaching_step", StringType),
        Property("consultant_gid", StringType),
        Property("contained_smiley", StringType),
        Property("country_code", StringType),
        Property("customer_gid", StringType),
        Property("datetime", StringType),
        Property("device", StringType),
        Property("device_group", StringType),
        Property("device_manufacturer", StringType),
        Property("device_model", StringType),
        Property("device_type", StringType),
        Property("device_uuid", StringType),
        Property("duration", StringType),
        Property("enabled_accessibilities", StringType),
        Property("enabled_features", StringType),
        Property("entity_guid", StringType),
        Property("event_id", StringType),
        Property("exit_view", StringType),
        Property("hearing_aid_today_time_spent", StringType),
        Property("hearing_aid_1990_time_spent", StringType),
        Property("hearing_aid_2000_time_spent", StringType),
        Property("input_type", StringType),
        Property("install", StringType),
        Property("installation_id", StringType),
        Property("interaction", StringType),
        Property("kb_article_id", StringType),
        Property("kb_article_page_number", StringType),
        Property("kb_article_read_completely", StringType),
        Property("last_interaction", StringType),
        Property("length", StringType),
        Property("mem_usage_mb", NumberType),
        Property("mobile_os", StringType),
        Property("name", StringType),
        Property("new_relic_agent", StringType),
        Property("new_relic_version", StringType),
        Property("occurrence_timestamp", StringType),
        Property("opening_hours_available", StringType),
        Property("orientation", StringType),
        Property("os_build", StringType),
        Property("os_major_version", StringType),
        Property("os_name", StringType),
        Property("os_version", StringType),
        Property("page_id", StringType),
        Property("partner_gid", StringType),
        Property("phone_number", StringType),
        Property("platform", StringType),
        Property("platform_version", StringType),
        Property("product_id", StringType),
        Property("push_notification", StringType),
        Property("push_opened", StringType),
        Property("rating", StringType),
        Property("region_code", StringType),
        Property("registration_type", StringType),
        Property("remote_app_name", StringType),
        Property("reported_timestamp_ms", StringType),
        Property("request_code_result", StringType),
        Property("request_code_status", StringType),
        Property("run_time", StringType),
        Property("restart", StringType),
        Property("screen", StringType),
        Property("scrolling_time", StringType),
        Property("session_crashed", StringType),
        Property("session_duration", NumberType),
        Property("session_id", StringType),
        Property("session_move_count", StringType),
        Property("session_move_duration", StringType),
        Property("session_pinch_gesture_count", StringType),
        Property("session_twist_gesture_count", StringType),
        Property("session_y_rotation_duration", StringType),
        Property("size", StringType),
        Property("story_id", StringType),
        Property("time_since_load", NumberType),
        Property("timestamp", DateTimeType),
        Property("to_orientation", StringType),
        Property("tracking_id", StringType),
        Property("triggered_from", StringType),
        Property("triggered_from_item", StringType),
        Property("unread_messages", StringType),
        Property("upgrade_from", StringType),
        Property("url", StringType),
        Property("uuid", StringType),
        Property("video_url", StringType),
        Property("video_view_through_rate", StringType),
    ).to_dict()
    nqrl_query = "SELECT * FROM mobile_app SINCE '{}' UNTIL '{}' ORDER BY timestamp LIMIT MAX"

