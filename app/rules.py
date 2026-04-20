"""Static rules for discovery and normalization in Subphase 2."""

from __future__ import annotations


SUPPORTED_FILE_TYPES: dict[str, str] = {
    ".csv": "csv",
    ".xlsx": "xlsx",
}

CANONICAL_COLUMNS: list[str] = [
    "id",
    "email",
    "domain",
    "fname",
    "lname",
    "state",
    "address",
    "county",
    "city",
    "zip",
    "website",
    "ip",
]

COLUMN_ALIASES: dict[str, str] = {
    "e_mail": "email",
    "email_address": "email",
    "emailaddress": "email",
    "mail": "email",
    "domain_name": "domain",
    "domainname": "domain",
    "first_name": "fname",
    "firstname": "fname",
    "last_name": "lname",
    "lastname": "lname",
    "postal_code": "zip",
    "postalcode": "zip",
    "zip_code": "zip",
    "zipcode": "zip",
}

MINIMUM_REQUIRED_COLUMNS: list[str] = ["email"]

TECHNICAL_METADATA_COLUMNS: list[str] = [
    "source_file",
    "source_row_number",
    "source_file_type",
    "chunk_index",
]
