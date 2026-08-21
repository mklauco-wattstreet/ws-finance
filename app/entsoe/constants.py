"""
ENTSO-E EIC Codes for Czech Republic and neighboring bidding zones.

These constants are used for cross-border physical flow queries (A11 document type)
and unified generation queries (A75 document type).
"""

from datetime import date

# Czech Republic bidding zone
CZ_BZN = "10YCZ-CEPS-----N"

# German TSO control areas (4 TSOs cover all of Germany)
DE_TENNET = "10YDE-EON------1"      # Germany TenneT (north/east)
DE_50HERTZ = "10YDE-VE-------2"     # Germany 50Hertz (east)
DE_AMPRION = "10YDE-RWENET---I"     # Germany Amprion (west)
DE_TRANSNETBW = "10YDE-ENBW-----N"  # Germany TransnetBW (southwest)

# DE-LU unified bidding zone (for day-ahead prices, distinct from TSO control areas)
DE_LU_BZN = "10Y1001A1001A82H"

# Other neighboring bidding zones
AT_BZN = "10YAT-APG------L"     # Austria
PL_BZN = "10YPL-AREA-----S"     # Poland
SK_BZN = "10YSK-SEPS-----K"     # Slovakia
HU_BZN = "10YHU-MAVIR----U"     # Hungary

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
    HU_BZN: 9,           # Hungary
    DE_LU_BZN: 10,       # Germany-Luxembourg (bidding zone)
}

# Reverse mapping: area_id -> EIC code
AREA_CODES = {v: k for k, v in AREA_IDS.items()}

# Active areas for unified generation fetching
# All areas with is_active=TRUE in entsoe_areas table
# Tuple format: (area_id, eic_code, display_label, country_code)
# country_code is used for partition routing (e.g., 'DE' for all German TSOs)
ACTIVE_GENERATION_AREAS = [
    (1, CZ_BZN, "CZ", "CZ"),
    (2, DE_TENNET, "DE-TenneT", "DE"),
    (3, AT_BZN, "AT", "AT"),
    (4, PL_BZN, "PL", "PL"),
    (5, SK_BZN, "SK", "SK"),
    (6, DE_50HERTZ, "DE-50Hertz", "DE"),
    (7, DE_AMPRION, "DE-Amprion", "DE"),
    (8, DE_TRANSNETBW, "DE-TransnetBW", "DE"),
]

# German TSO areas for aggregation queries
DE_TSO_AREAS = [
    (2, DE_TENNET, "DE-TenneT"),
    (6, DE_50HERTZ, "DE-50Hertz"),
    (7, DE_AMPRION, "DE-Amprion"),
    (8, DE_TRANSNETBW, "DE-TransnetBW"),
]

# Forecast process type labels (for logging)
FORECAST_PROCESS_TYPES = {"A01": "Day-Ahead", "A18": "Current", "A40": "Intraday"}

# Active areas for current (A18) generation forecast
# CZ and SK do not publish A18 forecasts via ENTSO-E
ACTIVE_CURRENT_FORECAST_AREAS = [
    (2, DE_TENNET, "DE-TenneT", "DE"),
    (3, AT_BZN, "AT", "AT"),
    (4, PL_BZN, "PL", "PL"),
    (6, DE_50HERTZ, "DE-50Hertz", "DE"),
    (7, DE_AMPRION, "DE-Amprion", "DE"),
    (8, DE_TRANSNETBW, "DE-TransnetBW", "DE"),
]

# Active areas for day-ahead prices fetching
# Tuple format: (area_id, eic_code, display_label, country_code)
# Day-ahead prices are per bidding zone (not per TSO like generation)
# Note: CZ is absent by design - Czech day-ahead prices come straight from OTE
# into ote_prices_day_ahead, not via ENTSO-E, so the "CZ first" ordering rule
# that applies to the other ACTIVE_* lists has nothing to order here.
ACTIVE_DAY_AHEAD_AREAS = [
    (9, HU_BZN, "HU", "HU"),
    (10, DE_LU_BZN, "DE-LU", "DE"),
    (3, AT_BZN, "AT", "AT"),
    (5, SK_BZN, "SK", "SK"),
    (4, PL_BZN, "PL", "PL"),
]

# Active areas for imbalance prices fetching
# Tuple format: (area_id, eic_code, display_label, country_code)
# Note: Imbalance prices are fetched per control area
# Currency: CZ uses CZK, HU uses EUR
ACTIVE_IMBALANCE_AREAS = [
    (1, CZ_BZN, "CZ", "CZ"),
    (2, DE_TENNET, "DE", "DE"),
    (3, AT_BZN, "AT", "AT"),
    (4, PL_BZN, "PL", "PL"),
    (5, SK_BZN, "SK", "SK"),
    (9, HU_BZN, "HU", "HU"),
]

# Outages (A77 production unit unavailability). Fetched per bidding zone.
# CZ only to start; outage data relevant from 2026-03-01 (see OUTAGE_BACKFILL_FLOOR).
ACTIVE_OUTAGE_AREAS = [
    (1, CZ_BZN, "CZ", "CZ"),
]

# Procured Balancing Capacity [GL EB 12.3.F] (documentType A15). CZ only.
# Fetched per Area_Domain with type_MarketAgreement.Type=A01 (daily product).
# CZ publishes aFRR (A51) and mFRR (A47); FCR/RR are out of scope.
ACTIVE_PROCURED_CAPACITY_AREAS = [
    (1, CZ_BZN, "CZ", "CZ"),
]

# processType -> internal reserve_type label. One API request per entry per day.
PROCURED_CAPACITY_PROCESS_TYPES = {
    "A51": "afrr",  # Automatic Frequency Restoration Reserve
    "A47": "mfrr",  # Manual Frequency Restoration Reserve
}

# Procured capacity data is published from 2026-03-01; do not backfill earlier.
PROCURED_CAPACITY_BACKFILL_FLOOR = date(2026, 3, 1)

# Intraday Offered Transfer Capacity [12.1.A/B] (documentType A31) border
# directions from CZ. IMPORTANT: this uses the DE-LU BIDDING ZONE
# (10Y1001A1001A82H), NOT the DE_TENNET control area used by CZ_NEIGHBORS /
# the A11 cross-border flow runner — capacity is a bidding-zone-level product.
CZ_CAPACITY_BORDERS = {
    "de": DE_LU_BZN,
    "at": AT_BZN,
    "pl": PL_BZN,
    "sk": SK_BZN,
}

# Intraday capacity products -> (auction.Type, classificationSequence position).
# idct: continuous intraday capacity, auction.Type=A08, no classification
#   sequence; documents carry update_DateAndOrTime.dateTime (rolling revisions
#   during the delivery day).
# ida1/ida2/ida3: implicit intraday auction results, auction.Type=A01 with
#   classificationSequence_AttributeInstanceComponent.Position=1|2|3; these
#   documents do NOT carry update_DateAndOrTime (no revisions).
#   ida3 covers only the second half of the delivery day (Period timeInterval
#   starts 10:00Z = 12:00 Prague, 48 positions) — that is normal, not a gap.
INTRADAY_CAPACITY_PRODUCTS = {
    "idct": ("A08", None),
    "ida1": ("A01", 1),
    "ida2": ("A01", 2),
    "ida3": ("A01", 3),
}

# Active areas for intraday offered transfer capacity fetching. CZ only.
ACTIVE_INTRADAY_CAPACITY_AREAS = [
    (1, CZ_BZN, "CZ", "CZ"),
]

# Congestion income [12.1.E] (documentType A25, businessType B10). Queried
# per bidding zone with in_Domain = out_Domain (CZ is in the Core
# flow-based region, so per-border queries return "no matching data").
# CZ only.
ACTIVE_CONGESTION_INCOME_AREAS = [
    (1, CZ_BZN, "CZ", "CZ"),
]
