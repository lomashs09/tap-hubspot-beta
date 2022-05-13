"""REST client handling, including hubspotStream base class."""

from backports.cached_property import cached_property

import requests
import logging
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream

from tap_hubspot_beta.auth import OAuth2Authenticator

logging.getLogger("backoff").setLevel(logging.CRITICAL)


class hubspotStream(RESTStream):
    """hubspot stream class."""

    url_base = "https://api.hubapi.com/"
    base_properties = []
    additional_prarams = {}
    properties_url = None

    @property
    def authenticator(self) -> OAuth2Authenticator:
        """Return a new authenticator object."""
        return OAuth2Authenticator(
            self, self._tap.config_file, "https://api.hubapi.com/oauth/v1/token"
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["Content-Type"] = "application/json"
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    @cached_property
    def datetime_fields(self):
        datetime_fields = []
        for key, value in self.schema["properties"].items():
            if value.get("format") == "date-time":
                datetime_fields.append(key)
        return datetime_fields

    @cached_property
    def selected_properties(self):
        selected_properties = []
        for key, value in self.metadata.items():
            if isinstance(key, tuple) and len(key) == 2 and value.selected:
                selected_properties.append(key[-1])
        return selected_properties

    @staticmethod
    def extract_type(field):
        field_type = field.get("type")
        if field_type in ["string", "enumeration", "phone_number", "date"]:
            return th.StringType
        if field_type == "number":
            return th.StringType
        if field_type == "datetime":
            return th.DateTimeType
        if field_type == "bool":
            return th.BooleanType
        else:
            return None

    @cached_property
    def schema(self):
        properties = self.base_properties
        headers = self.http_headers
        headers.update(self.authenticator.auth_headers or {})
        response = requests.get(self.url_base + self.properties_url, headers=headers)

        fields = response.json()
        for field in fields:
            if not field.get("deleted"):
                property = th.Property(field.get("name"), self.extract_type(field))
                properties.append(property)

        return th.PropertiesList(*properties).to_dict()
