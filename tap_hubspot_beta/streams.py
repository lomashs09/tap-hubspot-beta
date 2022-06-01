"""Stream type classes for tap-hubspot."""
from singer_sdk import typing as th

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
