"""
ENTSO-E EIC Codes for Czech Republic and neighboring bidding zones.

These constants are used for cross-border physical flow queries (A11 document type)
and unified generation queries (A75 document type).
"""

# Czech Republic bidding zone
CZ_BZN = "10YCZ-CEPS-----N"

# German TSO control areas (4 TSOs cover all of Germany)
DE_TENNET = "10YDE-EON------1"      # Germany TenneT (north/east)
DE_50HERTZ = "10YDE-VE-------2"     # Germany 50Hertz (east)
DE_AMPRION = "10YDE-RWENET---I"     # Germany Amprion (west)
DE_TRANSNETBW = "10YDE-ENBW-----N"  # Germany TransnetBW (southwest)

# Other neighboring bidding zones
AT_BZN = "10YAT-APG------L"     # Austria
PL_BZN = "10YPL-AREA-----S"     # Poland
SK_BZN = "10YSK-SEPS-----K"     # Slovakia

# Mapping for iteration in runner (key = column suffix, value = EIC code)
# Note: For cross-border flows, we use TenneT as representative for DE
CZ_NEIGHBORS = {
    "de": DE_TENNET,
    "at": AT_BZN,
    "pl": PL_BZN,
    "sk": SK_BZN,
}

# Area IDs for partitioned tables (must match entsoe_areas table)
# These IDs are stable and used for partition routing
AREA_IDS = {
    CZ_BZN: 1,           # Czech Republic
    DE_TENNET: 2,        # Germany (TenneT)
    AT_BZN: 3,           # Austria
    PL_BZN: 4,           # Poland
    SK_BZN: 5,           # Slovakia
    DE_50HERTZ: 6,       # Germany (50Hertz)
    DE_AMPRION: 7,       # Germany (Amprion)
    DE_TRANSNETBW: 8,    # Germany (TransnetBW)
}

# Reverse mapping: area_id -> EIC code
AREA_CODES = {v: k for k, v in AREA_IDS.items()}

# Active areas for unified generation fetching
# All areas with is_active=TRUE in entsoe_areas table
ACTIVE_GENERATION_AREAS = [
    (1, CZ_BZN, "CZ"),
    (2, DE_TENNET, "DE-TenneT"),
    (3, AT_BZN, "AT"),
    (4, PL_BZN, "PL"),
    (5, SK_BZN, "SK"),
    (6, DE_50HERTZ, "DE-50Hertz"),
    (7, DE_AMPRION, "DE-Amprion"),
    (8, DE_TRANSNETBW, "DE-TransnetBW"),
]

# German TSO areas for aggregation queries
DE_TSO_AREAS = [
    (2, DE_TENNET, "DE-TenneT"),
    (6, DE_50HERTZ, "DE-50Hertz"),
    (7, DE_AMPRION, "DE-Amprion"),
    (8, DE_TRANSNETBW, "DE-TransnetBW"),
]
