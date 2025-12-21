"""
ENTSO-E EIC Codes for Czech Republic and neighboring bidding zones.

These constants are used for cross-border physical flow queries (A11 document type).
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
