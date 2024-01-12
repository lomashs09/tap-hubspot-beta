
import logging
from datetime import datetime
from typing import Any, Dict, Optional

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_hubspot_beta.client_base import hubspotStreamSchema


class hubspotV2Stream(hubspotStreamSchema):
    """hubspot stream class."""

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["limit"] = self.page_size
        params.update(self.additional_prarams)
        if self.properties_url:
            params["properties"] = self.selected_properties
        if next_page_token:
            params["offset"] = next_page_token["offset"]
        return params
    
    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        if self.properties_url:
            for name, value in row.get("properties").items():
                row[name] = value.get("value")
            row["id"] = str(row["companyId"])
            del row["properties"]
        for field in self.datetime_fields:
            if row.get(field) is not None:
                if row.get(field) in [0, ""]:
                    row[field] = None
                else:
                    dt_field = datetime.fromtimestamp(int(row[field]) / 1000)
                    row[field] = dt_field.isoformat()
        return row
