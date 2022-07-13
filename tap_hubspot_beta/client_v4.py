"""REST client handling, including hubspotStream base class."""

import logging
from typing import Any, Dict, Optional

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_hubspot_beta.client_base import hubspotStream


class hubspotV4Stream(hubspotStream):
    """hubspot stream class."""

    rest_method = "POST"
    records_jsonpath = "$.results[*]"

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request."""
        payload = {"inputs": [context]}
        return payload

    def parse_response(self, response: requests.Response):
        """Parse the response and return an iterator of result rows."""
        for row in extract_jsonpath(self.records_jsonpath, input=response.json()):
            output = {}
            output["from_id"] = row["from"]["id"]
            for to in row["to"]:
                output["to_id"] = str(to["toObjectId"])
                yield output
