"""Stream type classes for tap-hubspot."""
import json
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import requests
from backports.cached_property import cached_property
from singer_sdk import typing as th

from tap_hubspot_beta.client_base import hubspotStream
from tap_hubspot_beta.client_v1 import hubspotV1Stream
from tap_hubspot_beta.client_v3 import hubspotV3SearchStream, hubspotV3Stream
from tap_hubspot_beta.client_v4 import hubspotV4Stream


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

    def get_child_context(self, record: dict, context) -> dict:
        return {"id": record["id"]}


class AssociationDealsStream(hubspotV4Stream):
    """Association Base Stream"""
    primary_keys = ["from_id", "to_id"]
    parent_stream_type = DealsStream

    schema = th.PropertiesList(
        th.Property("from_id", th.StringType),
        th.Property("to_id", th.StringType)
    ).to_dict()


class AssociationDealsCompaniesStream(AssociationDealsStream):
    """Association Deals -> Companies Stream"""
    name = "associations_deals_companies"
    path = "crm/v4/associations/deals/companies/batch/read"


class AssociationDealsContactsStream(AssociationDealsStream):
    """Association Deals -> Contacts Stream"""
    name = "associations_deals_contacts"
    path = "crm/v4/associations/deals/contacts/batch/read"


class AssociationDealsLineItemsStream(AssociationDealsStream):
    """Association Deals -> LineItems Stream"""
    name = "associations_deals_line_items"
    path = "crm/v4/associations/deals/line_items/batch/read"


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

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "contact_id": record["vid"],
        }


class ContactEventsStream(hubspotV3Stream):
    """ContactEvents Stream"""
    name = "contact_events"
    path = "events/v3/events/?objectType=contact&objectId={contact_id}"

    records_jsonpath = "$.results[*]"
    parent_stream_type = ContactsStream
    primary_keys = ["id"]
    replication_key = "occurredAt"

    schema = th.PropertiesList(
        th.Property("objectType", th.StringType),
        th.Property("objectId", th.StringType),
        th.Property("eventType", th.StringType),
        th.Property("occurredAt", th.DateTimeType),
        th.Property("id", th.StringType),
        th.Property("contact_id", th.IntegerType),
        th.Property("properties", th.CustomType({"type": ["object", "string"]})),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        row = super().post_process(row, context)
        row["contact_id"] = context.get("contact_id")
        return row


class EmailEventsStream(hubspotV1Stream):
    """EmailEvents Stream"""
    name = "email_events"
    path = "email/public/v1/events"
    records_jsonpath = "$.events[*]"
    primary_keys = ["listId", "created"]
    replication_key = "created"
    page_size = 250

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("emailCampaignId", th.IntegerType),
        th.Property("hmid", th.StringType),
        th.Property("recipient", th.StringType),
        th.Property("type", th.StringType),
        th.Property("userAgent", th.StringType),
        th.Property("location", th.CustomType({"type": ["object", "string"]})),
        th.Property("browser", th.CustomType({"type": ["object", "string"]})),
        th.Property("portalId", th.IntegerType),
        th.Property("created", th.DateTimeType),
        th.Property("appName", th.StringType),
        th.Property("deviceType", th.StringType),
        th.Property("duration", th.IntegerType),
        th.Property("sentBy", th.CustomType({"type": ["object", "string"]})),
        th.Property("smtpId", th.StringType),
        th.Property("filteredEvent", th.BooleanType),
        th.Property("appId", th.IntegerType),
        th.Property("response", th.StringType),
        th.Property("attempt", th.IntegerType),
        th.Property("subject", th.StringType),
        th.Property("cc", th.CustomType({"type": ["array", "string"]})),
        th.Property("bcc", th.CustomType({"type": ["array", "string"]})),
        th.Property("replyTo", th.CustomType({"type": ["array", "string"]})),
        th.Property("from", th.StringType),
        th.Property("sourceId", th.StringType),
        th.Property("subscriptions", th.CustomType({"type": ["array", "string"]})),
        th.Property("portalSubscriptionStatus", th.StringType),
        th.Property("source", th.StringType),
    ).to_dict()


class FormsStream(hubspotV3Stream):
    """Forms Stream"""
    name = "forms"
    path = "marketing/v3/forms/"
    primary_keys = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("fieldGroups", th.CustomType({"type": ["array", "string"]})),
        th.Property("configuration", th.CustomType({"type": "object"})),
        th.Property("displayOptions", th.CustomType({"type": "object"})),
        th.Property("legalConsentOptions", th.CustomType({"type": "object"})),
        th.Property("formType", th.StringType),
        th.Property("archived", th.BooleanType),
        th.Property("userId", th.IntegerType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "form_id": record["id"],
        }


class FormSubmissionsStream(hubspotV1Stream):
    """FormSubmissions Stream"""

    name = "form_submissions"
    records_jsonpath = "$.results[*]"
    parent_stream_type = FormsStream
    # NOTE: There is no primary_key for this stream
    replication_key = "submittedAt"
    path = "/form-integrations/v1/submissions/forms/{form_id}"

    schema = th.PropertiesList(
        th.Property("form_id", th.StringType),
        th.Property("values", th.CustomType({"type": ["array", "string"]})),
        th.Property("submittedAt", th.DateTimeType),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        row = super().post_process(row, context)
        row["form_id"] = context.get("form_id")
        return row


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
        # Get the data from Hubspot
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
                list_id = property.split("-")
                list_id = list_id[-1]
                yield {"id": list_id.strip(), "name": property}

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "list_id": record["id"],
        }


class ContactListData(hubspotV1Stream):
    """Lists Stream"""

    name = "contact_list_data"
    records_jsonpath = "$.contacts[*]"
    parent_stream_type = ContactListsStream
    primary_keys = ["vid", "listId"]
    replication_key = None
    path = "/contacts/v1/lists/{list_id}/contacts/all"
    properties_url = "properties/v1/contacts/properties"

    base_properties = [
        th.Property("vid", th.IntegerType),
        th.Property("addedAt", th.DateTimeType),
        th.Property("portal-id", th.IntegerType),
        th.Property("listId", th.IntegerType),
    ]

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        if self.properties_url:
            for name, value in row["properties"].items():
                row[name] = value.get("value")
            del row["properties"]
        for field in self.datetime_fields:
            if row.get(field):
                dt_field = datetime.fromtimestamp(int(row[field]) / 1000)
                row[field] = dt_field.isoformat()
        row["listId"] = int(context.get("list_id"))
        return row


class AccountStream(hubspotV1Stream):
    """Account Stream"""
    name = "account"
    path = "integrations/v1/me"
    records_jsonpath = "$"
    primary_keys = ["portalId"]

    schema = th.PropertiesList(
        th.Property("portalId", th.IntegerType),
        th.Property("timeZone", th.StringType),
        th.Property("accountType", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("utcOffset", th.StringType),
        th.Property("utcOffsetMilliseconds", th.IntegerType),
    ).to_dict()


class ProductsStream(hubspotV3SearchStream):
    """Products Stream"""
    name = "products"
    path = "crm/v3/objects/products/search"
    primary_keys = ["id", "updatedAt"]
    replication_key = "updatedAt"
    replication_key_filter = "hs_lastmodifieddate"
    page_size = 10
    properties_url = "properties/v2/products/properties"

    base_properties = [
        th.Property("id", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("archived", th.BooleanType),
        th.Property("archivedAt", th.DateTimeType),
    ]


class LineItemsStream(hubspotV3SearchStream):
    """Products Stream"""
    name = "lineitems"
    path = "crm/v3/objects/line_items/search"
    primary_keys = ["id", "updatedAt"]
    replication_key = "updatedAt"
    replication_key_filter = "hs_lastmodifieddate"
    page_size = 10
    properties_url = "properties/v2/line_items/properties"

    base_properties = [
        th.Property("id", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("archived", th.BooleanType),
        th.Property("archivedAt", th.DateTimeType),
    ]
