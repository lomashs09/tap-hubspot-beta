"""REST client handling, including hubspotStream base class."""

from typing import Any, Dict, Optional

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_hubspot.client_base import hubspotStream


class hubspotV3SearchStream(hubspotStream):
    """hubspot stream class."""

    rest_method = "POST"

    records_jsonpath = "$.results[*]"
    next_page_token_jsonpath = "$.paging.next.after"

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        all_matches = extract_jsonpath(self.next_page_token_jsonpath, response.json())
        return next(iter(all_matches), None)

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request."""
        payload = {}
        payload["limit"] = 100
        if next_page_token:
            payload["after"] = next_page_token
        if self.replication_key:
            start_date = self.get_starting_timestamp(context)
            start_date_ts_ms = int(start_date.timestamp() * 1000)

            payload["filters"] = [
                {
                    "propertyName": self.replication_key_filter,
                    "operator": "GT",
                    "value": start_date_ts_ms,
                }
            ]
            payload["properties"] = self.selected_properties
            return payload

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        for name, value in row["properties"].items():
            row[name] = value
        del row["properties"]
        return row


class hubspotV3Stream(hubspotStream):
    """hubspot stream class."""

    records_jsonpath = "$.results[*]"
    next_page_token_jsonpath = "$.paging.next.after"

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        all_matches = extract_jsonpath(self.next_page_token_jsonpath, response.json())
        return next(iter(all_matches), None)

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["limit"] = 100
        if next_page_token:
            params["after"] = next_page_token
        return params

    def parse_response(self, response: requests.Response):
        """Parse the response and return an iterator of result rows."""
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        return row
