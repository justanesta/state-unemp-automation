"""Static reference data for all 50 U.S. states.

Populate fips_code, census_region, and census_division with your authoritative
source values before running the pipeline. The values below are pre-filled from
U.S. Census Bureau definitions — verify against your source if required.
"""

from collections import Counter

# ---------------------------------------------------------------------------
# State reference data
# ---------------------------------------------------------------------------
# Fields:
#   name            – canonical full name (title case)
#   usps_code       – 2-letter USPS postal code
#   fips_code       – 2-digit zero-padded FIPS state code
#   census_region   – Census Bureau region name
#   census_division – Census Bureau division name
#
# Region / Division mapping (U.S. Census Bureau):
#   Northeast   → New England, Middle Atlantic
#   Midwest     → East North Central, West North Central
#   South       → South Atlantic, East South Central, West South Central
#   West        → Mountain, Pacific
# ---------------------------------------------------------------------------

STATES: list[dict] = [
    {"name": "Alabama",        "usps_code": "AL", "fips_code": "01", "census_region": "South",     "census_division": "East South Central"},
    {"name": "Alaska",         "usps_code": "AK", "fips_code": "02", "census_region": "West",      "census_division": "Pacific"},
    {"name": "Arizona",        "usps_code": "AZ", "fips_code": "04", "census_region": "West",      "census_division": "Mountain"},
    {"name": "Arkansas",       "usps_code": "AR", "fips_code": "05", "census_region": "South",     "census_division": "West South Central"},
    {"name": "California",     "usps_code": "CA", "fips_code": "06", "census_region": "West",      "census_division": "Pacific"},
    {"name": "Colorado",       "usps_code": "CO", "fips_code": "08", "census_region": "West",      "census_division": "Mountain"},
    {"name": "Connecticut",    "usps_code": "CT", "fips_code": "09", "census_region": "Northeast", "census_division": "New England"},
    {"name": "Delaware",       "usps_code": "DE", "fips_code": "10", "census_region": "South",     "census_division": "South Atlantic"},
    {"name": "Florida",        "usps_code": "FL", "fips_code": "12", "census_region": "South",     "census_division": "South Atlantic"},
    {"name": "Georgia",        "usps_code": "GA", "fips_code": "13", "census_region": "South",     "census_division": "South Atlantic"},
    {"name": "Hawaii",         "usps_code": "HI", "fips_code": "15", "census_region": "West",      "census_division": "Pacific"},
    {"name": "Idaho",          "usps_code": "ID", "fips_code": "16", "census_region": "West",      "census_division": "Mountain"},
    {"name": "Illinois",       "usps_code": "IL", "fips_code": "17", "census_region": "Midwest",   "census_division": "East North Central"},
    {"name": "Indiana",        "usps_code": "IN", "fips_code": "18", "census_region": "Midwest",   "census_division": "East North Central"},
    {"name": "Iowa",           "usps_code": "IA", "fips_code": "19", "census_region": "Midwest",   "census_division": "West North Central"},
    {"name": "Kansas",         "usps_code": "KS", "fips_code": "20", "census_region": "Midwest",   "census_division": "West North Central"},
    {"name": "Kentucky",       "usps_code": "KY", "fips_code": "21", "census_region": "South",     "census_division": "East South Central"},
    {"name": "Louisiana",      "usps_code": "LA", "fips_code": "22", "census_region": "South",     "census_division": "West South Central"},
    {"name": "Maine",          "usps_code": "ME", "fips_code": "23", "census_region": "Northeast", "census_division": "New England"},
    {"name": "Maryland",       "usps_code": "MD", "fips_code": "24", "census_region": "South",     "census_division": "South Atlantic"},
    {"name": "Massachusetts",  "usps_code": "MA", "fips_code": "25", "census_region": "Northeast", "census_division": "New England"},
    {"name": "Michigan",       "usps_code": "MI", "fips_code": "26", "census_region": "Midwest",   "census_division": "East North Central"},
    {"name": "Minnesota",      "usps_code": "MN", "fips_code": "27", "census_region": "Midwest",   "census_division": "West North Central"},
    {"name": "Mississippi",    "usps_code": "MS", "fips_code": "28", "census_region": "South",     "census_division": "East South Central"},
    {"name": "Missouri",       "usps_code": "MO", "fips_code": "29", "census_region": "Midwest",   "census_division": "West North Central"},
    {"name": "Montana",        "usps_code": "MT", "fips_code": "30", "census_region": "West",      "census_division": "Mountain"},
    {"name": "Nebraska",       "usps_code": "NE", "fips_code": "31", "census_region": "Midwest",   "census_division": "West North Central"},
    {"name": "Nevada",         "usps_code": "NV", "fips_code": "32", "census_region": "West",      "census_division": "Mountain"},
    {"name": "New Hampshire",  "usps_code": "NH", "fips_code": "33", "census_region": "Northeast", "census_division": "New England"},
    {"name": "New Jersey",     "usps_code": "NJ", "fips_code": "34", "census_region": "Northeast", "census_division": "Middle Atlantic"},
    {"name": "New Mexico",     "usps_code": "NM", "fips_code": "35", "census_region": "West",      "census_division": "Mountain"},
    {"name": "New York",       "usps_code": "NY", "fips_code": "36", "census_region": "Northeast", "census_division": "Middle Atlantic"},
    {"name": "North Carolina", "usps_code": "NC", "fips_code": "37", "census_region": "South",     "census_division": "South Atlantic"},
    {"name": "North Dakota",   "usps_code": "ND", "fips_code": "38", "census_region": "Midwest",   "census_division": "West North Central"},
    {"name": "Ohio",           "usps_code": "OH", "fips_code": "39", "census_region": "Midwest",   "census_division": "East North Central"},
    {"name": "Oklahoma",       "usps_code": "OK", "fips_code": "40", "census_region": "South",     "census_division": "West South Central"},
    {"name": "Oregon",         "usps_code": "OR", "fips_code": "41", "census_region": "West",      "census_division": "Pacific"},
    {"name": "Pennsylvania",   "usps_code": "PA", "fips_code": "42", "census_region": "Northeast", "census_division": "Middle Atlantic"},
    {"name": "Rhode Island",   "usps_code": "RI", "fips_code": "44", "census_region": "Northeast", "census_division": "New England"},
    {"name": "South Carolina", "usps_code": "SC", "fips_code": "45", "census_region": "South",     "census_division": "South Atlantic"},
    {"name": "South Dakota",   "usps_code": "SD", "fips_code": "46", "census_region": "Midwest",   "census_division": "West North Central"},
    {"name": "Tennessee",      "usps_code": "TN", "fips_code": "47", "census_region": "South",     "census_division": "East South Central"},
    {"name": "Texas",          "usps_code": "TX", "fips_code": "48", "census_region": "South",     "census_division": "West South Central"},
    {"name": "Utah",           "usps_code": "UT", "fips_code": "49", "census_region": "West",      "census_division": "Mountain"},
    {"name": "Vermont",        "usps_code": "VT", "fips_code": "50", "census_region": "Northeast", "census_division": "New England"},
    {"name": "Virginia",       "usps_code": "VA", "fips_code": "51", "census_region": "South",     "census_division": "South Atlantic"},
    {"name": "Washington",     "usps_code": "WA", "fips_code": "53", "census_region": "West",      "census_division": "Pacific"},
    {"name": "West Virginia",  "usps_code": "WV", "fips_code": "54", "census_region": "South",     "census_division": "South Atlantic"},
    {"name": "Wisconsin",      "usps_code": "WI", "fips_code": "55", "census_region": "Midwest",   "census_division": "East North Central"},
    {"name": "Wyoming",        "usps_code": "WY", "fips_code": "56", "census_region": "West",      "census_division": "Mountain"},
]

# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------

_BY_CODE: dict[str, dict] = {s["usps_code"].upper(): s for s in STATES}
_BY_NAME: dict[str, dict] = {s["name"].lower(): s for s in STATES}


def get_state_by_code(code: str) -> dict | None:
    """Look up a state by 2-letter USPS code (case-insensitive)."""
    return _BY_CODE.get(code.upper())


def get_state_by_name(name: str) -> dict | None:
    """Look up a state by canonical name (case-insensitive, exact match)."""
    return _BY_NAME.get(name.lower())


# ---------------------------------------------------------------------------
# Derived constants — computed once at import time
# ---------------------------------------------------------------------------

REGION_STATE_COUNTS: dict[str, int] = dict(Counter(s["census_region"] for s in STATES))
DIVISION_STATE_COUNTS: dict[str, int] = dict(Counter(s["census_division"] for s in STATES))
