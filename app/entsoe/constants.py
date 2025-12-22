"""
ENTSO-E EIC Codes for Czech Republic and neighboring bidding zones.

These constants are used for cross-border physical flow queries (A11 document type)
and unified generation queries (A75 document type).
"""

# Czech Republic bidding zone
CZ_BZN = "10YCZ-CEPS-----N"

# Neighboring bidding zones
DE_TENNET = "10YDE-EON------1"  # Germany TenneT
AT_BZN = "10YAT-APG------L"     # Austria
PL_BZN = "10YPL-AREA-----S"     # Poland
SK_BZN = "10YSK-SEPS-----K"     # Slovakia

# Mapping for iteration in runner (key = column suffix, value = EIC code)
CZ_NEIGHBORS = {
    "de": DE_TENNET,
    "at": AT_BZN,
    "pl": PL_BZN,
    "sk": SK_BZN,
}

# Area IDs for partitioned tables (must match entsoe_areas table)
# These IDs are stable and used for partition routing
AREA_IDS = {
    CZ_BZN: 1,       # Czech Republic
    DE_TENNET: 2,    # Germany (TenneT)
    AT_BZN: 3,       # Austria
    PL_BZN: 4,       # Poland
    SK_BZN: 5,       # Slovakia
}

# Reverse mapping: area_id -> EIC code
AREA_CODES = {v: k for k, v in AREA_IDS.items()}

# Active areas for unified generation fetching
# All areas with is_active=TRUE in entsoe_areas table
ACTIVE_GENERATION_AREAS = [
    (1, CZ_BZN, "CZ"),
    (2, DE_TENNET, "DE"),
    (3, AT_BZN, "AT"),
    (4, PL_BZN, "PL"),
    (5, SK_BZN, "SK"),
]
