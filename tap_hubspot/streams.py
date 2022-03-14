"""Stream type classes for tap-hubspot."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_hubspot.client import hubspotStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class CompaniesPropertiesStream(hubspotStream):
    """Companies properties Stream"""
    name = "companies_properties"
    path = "companies/v2/properties"
    primary_keys = ["name"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("label", th.StringType),
        th.Property("description", th.StringType),
        th.Property("groupName", th.StringType),
        th.Property("type", th.StringType),
        th.Property("fieldType", th.StringType),
        th.Property("fieldLevelPermission", th.StringType),
        th.Property("hidden", th.BooleanType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("deleted", th.StringType),
        th.Property("referencedObjectType", th.StringType),
        th.Property("displayOrder", th.NumberType),
        th.Property("externalOptionsReferenceType", th.StringType),
        th.Property("searchableInGlobalSearch", th.BooleanType),
        th.Property("hasUniqueValue", th.BooleanType),
        th.Property("createdUserId", th.StringType),
        th.Property("optionSortStrategy", th.StringType),
        th.Property("numberDisplayHint", th.StringType),
        th.Property("readOnlyDefinition", th.BooleanType),
        th.Property("hubspotDefined", th.BooleanType),
        th.Property("isCustomizedDefault", th.BooleanType),
        th.Property("textDisplayHint", th.StringType),
        th.Property("formField", th.BooleanType),
        th.Property("readOnlyValue", th.BooleanType),
        th.Property("mutableDefinitionNotDeletable", th.BooleanType),
        th.Property("favorited", th.BooleanType),
        th.Property("favoritedOrder", th.NumberType),
        th.Property("calculated", th.BooleanType),
        th.Property("externalOptions", th.BooleanType),
        th.Property("displayMode", th.StringType),
        th.Property("showCurrencySymbol", th.BooleanType),
        th.Property("optionsAreMutable", th.BooleanType),
        th.Property("searchTextAnalysisMode", th.StringType),
        th.Property("currencyPropertyName", th.StringType),
        th.Property("updatedUserId", th.StringType)
    ).to_dict()




