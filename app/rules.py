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
    # --- email ---
    "e_mail": "email",
    "email_address": "email",
    "emailaddress": "email",
    "mail": "email",
    # Spanish
    "correo": "email",
    "correo_electronico": "email",  # accent-stripped form of "correo electrónico"
    "correo_electronico_": "email",

    # --- domain ---
    "domain_name": "domain",
    "domainname": "domain",

    # --- first name ---
    "first_name": "fname",
    "firstname": "fname",
    "given_name": "fname",
    # Spanish
    "nombre": "fname",
    "nombres": "fname",
    "primer_nombre": "fname",

    # --- last name ---
    "last_name": "lname",
    "lastname": "lname",
    "surname": "lname",
    "family_name": "lname",
    # Spanish
    "apellido": "lname",
    "apellidos": "lname",
    "primer_apellido": "lname",

    # --- phone (passthrough column, not in CANONICAL_COLUMNS) ---
    "phone_number": "phone",
    "phonenumber": "phone",
    "mobile": "phone",
    "cell": "phone",
    # Spanish
    "telefono": "phone",
    "tel": "phone",
    "celular": "phone",
    "movil": "phone",

    # --- company (passthrough) ---
    "organization": "company",
    "org": "company",
    # Spanish
    "empresa": "company",
    "compania": "company",  # accent-stripped form of "compañía"/"compañia"
    "razon_social": "company",

    # --- city / state (passthrough; "state" is already canonical) ---
    "ciudad": "city",
    "estado": "state",
    "provincia": "state",
    "region": "state",

    # --- postal code ---
    "postal_code": "zip",
    "postalcode": "zip",
    "zip_code": "zip",
    "zipcode": "zip",
    "codigo_postal": "zip",
    "cp": "zip",
}

MINIMUM_REQUIRED_COLUMNS: list[str] = ["email"]

TECHNICAL_METADATA_COLUMNS: list[str] = [
    "source_file",
    "source_row_number",
    "source_file_type",
    "chunk_index",
]
