"""Stream type classes for tap-hubspot."""
from singer_sdk import typing as th

from tap_hubspot_beta.client_v1 import hubspotV1Stream
from tap_hubspot_beta.client_v3 import hubspotV3SearchStream, hubspotV3Stream


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
