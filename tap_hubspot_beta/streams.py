"""Stream type classes for tap-hubspot."""
from singer_sdk import typing as th
from typing import Any, Dict, Optional, List, Iterable

from tap_hubspot_beta.client_v1 import hubspotV1Stream
from tap_hubspot_beta.client_v3 import hubspotV3SearchStream, hubspotV3Stream
from tap_hubspot_beta.client_base import hubspotStream
import requests
import json
from backports.cached_property import cached_property


class ContactsV3Stream(hubspotV3SearchStream):
    """Contacts Stream"""

    name = "contacts_v3"
    path = "crm/v3/objects/contacts/search"
    primary_keys = ["id", "updatedAt"]
    replication_key = "updatedAt"
    replication_key_filter = "lastmodifieddate"

    properties_url = "properties/v1/contacts/properties"

    base_properties = [
        th.Property("id", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("archived", th.BooleanType),
    ]


class CompaniesStream(hubspotV3SearchStream):
    """Companies Stream"""

    name = "companies"
    path = "crm/v3/objects/companies/search"
    primary_keys = ["id", "updatedAt"]
    replication_key = "updatedAt"
    replication_key_filter = "hs_lastmodifieddate"

    properties_url = "properties/v1/companies/properties"

    base_properties = [
        th.Property("id", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("archived", th.BooleanType),
    ]


class DealsStream(hubspotV3SearchStream):
    """Deals Stream"""

    name = "deals"
    path = "crm/v3/objects/deals/search"
    primary_keys = ["id", "updatedAt"]
    replication_key = "updatedAt"
    replication_key_filter = "hs_lastmodifieddate"
    properties_url = "properties/v1/deals/properties"

    base_properties = [
        th.Property("id", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("archived", th.BooleanType),
    ]


class ContactsStream(hubspotV1Stream):
    """Contacts Stream"""

    name = "contacts"
    path = "contacts/v1/lists/all/contacts/all"
    records_jsonpath = "$.contacts[*]"
    primary_keys = ["vid"]
    replication_key = None
    additional_prarams = dict(showListMemberships=True)
    properties_url = "properties/v1/contacts/properties"

    base_properties = [
        th.Property("vid", th.IntegerType),
        th.Property("addedAt", th.DateTimeType),
        th.Property("portal-id", th.IntegerType),
        th.Property("list-memberships", th.CustomType({"type": ["array", "string"]})),
    ]


class OwnersStream(hubspotV3Stream):
    """Owners Stream"""

    name = "owners"
    path = "crm/v3/owners/"
    primary_keys = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("email", th.StringType),
        th.Property("firstName", th.StringType),
        th.Property("lastName", th.StringType),
        th.Property("teams", th.CustomType({"type": ["array", "string"]})),
        th.Property("archived", th.BooleanType),
        th.Property("userId", th.IntegerType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
    ).to_dict()


class ListsStream(hubspotV1Stream):
    """Lists Stream"""

    name = "lists"
    path = "contacts/v1/lists"
    records_jsonpath = "$.lists[*]"
    primary_keys = ["listId", "updatedAt"]
    replication_key = "updatedAt"
    page_size = 250

    schema = th.PropertiesList(
        th.Property("listId", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("authorId", th.IntegerType),
        th.Property("portalId", th.IntegerType),
        th.Property("internalListId", th.IntegerType),
        th.Property("dynamic", th.BooleanType),
        th.Property("listType", th.StringType),
        th.Property("metaData", th.CustomType({"type": ["object", "string"]})),
        th.Property("filters", th.CustomType({"type": ["array", "string"]})),
        th.Property("teamIds", th.CustomType({"type": ["array", "string"]})),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("deleteable", th.BooleanType),
        th.Property("archived", th.BooleanType),
    ).to_dict()


class ContactListsStream(hubspotStream):
    """Lists Stream"""

    name = "contact_list"
    parent_stream_type = None
    records_jsonpath = "$.lists[*]"
    primary_keys = ["id", "name"]
    replication_key = None
    path = "/contacts/v1/lists"

    @cached_property
    def schema(self) -> dict:
        """Dynamically detect the json schema for the stream.
        This is evaluated prior to any records being retrieved.
        """
        
        # Init request session
        self._requests_session = requests.Session()
        # Get the data from Iterable
        records = self.request_records(dict())

        properties = []
        property_names = set()
        name = "id"
        property_names.add(name)
        properties.append(th.Property(name, th.StringType))
        name = "name"
        property_names.add(name)
        properties.append(th.Property(name, th.StringType))
        # Loop through all records – some objects have different keys
        for record in records:
            # Add the new property to our list
            name = f"{record['name']} - {record['listId']}"
            property_names.add(name)
            properties.append(th.Property(name, th.StringType))

        # Return the list as a JSON Schema dictionary object
        property_list = th.PropertiesList(*properties).to_dict()

        return property_list

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        selected_properties = self.selected_properties
        ignore = ["id", "name"]
        for property in selected_properties:
            if property not in ignore:
                list_id = property.split("-", 1)
                list_id = list_id[-1]
                yield {"id": list_id.strip(), "name": property}

    @cached_property
    def selected_properties(self):
        selected_properties = []
        for key, value in self.metadata.items():
            if isinstance(key, tuple) and len(key) == 2 and value.selected:
                field_name = key[-1]
                selected_properties.append(field_name)
        return selected_properties

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "list_id": record["id"],
        }


class ContactListData(hubspotStream):
    """Lists Stream"""

    name = "contact_list_data"
    records_jsonpath = "$.contacts[*]"
    parent_stream_type = ContactListsStream
    primary_keys = ["vid"]
    replication_key = None
    page_size = 100
    path = "/contacts/v1/lists/{list_id}/contacts/all"

    schema = th.PropertiesList(
        th.Property("vid", th.IntegerType),
        th.Property("listId", th.IntegerType),
        th.Property("addedAt", th.IntegerType),
        th.Property("authorId", th.IntegerType),
        th.Property("canonical-vid", th.IntegerType),
        th.Property("merged-vids", th.CustomType({"type": ["array", "string"]})),
        th.Property("portal-id", th.IntegerType),
        th.Property("is-contact", th.BooleanType),
        th.Property("properties", th.CustomType({"type": ["object", "string"]})),
        th.Property("form-submissions", th.CustomType({"type": ["array", "string"]})),
        th.Property("identity-profiles", th.CustomType({"type": ["array", "string"]})),
        th.Property("merge-audits", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row["listId"] = int(context.get("list_id"))
        return row
