"""
CEPS data type definitions and constants.

Based on CEPS.md data identifications.
"""

# Base URL for all CEPS data pages
CEPS_BASE_URL = "https://www.ceps.cz/cs/data"

# CEPS data types with their configurations
# Format: (tag, display_name, default_time_unit_minutes, downloadable)
CEPS_DATA_TYPES = [
    ("AktualniSystemovaOdchylkaCR", "Aktuální systémová odchylka ČR", 1, True),
    ("Load", "Load", 1, True),
    ("PowerBalance", "Power Balance", 1, True),
    ("CrossborderPowerFlows", "Crossborder Power Flows", 1, True),
    ("EmergencyExchange", "Emergency Exchange", 1, True),
    ("GenerationPlan", "Generation Plan", 1, True),
    ("Generation", "Generation", 1, True),
    ("GenerationRES", "Generation RES", 1, True),
    ("OdhadovanaCenaOdchylky", "Odhadovaná cena odchylky", 1, True),
    ("NepredvidatelneOdmitnuteNabidky", "Nepředvídatelné odmítnuté nabídky", 1, True),
    ("AktivaceSVRvCR", "Aktivace SVR v ČR", 1, True),
    ("Frekvence", "Frekvence", 1, True),
    ("MaximalniCenySVRnaDT", "Maximální ceny SVR na DT", 1, True),
    ("AktualniCenaRE", "Aktuální cena RE", 1, True),
    ("Emise", "Emise", 1, True),
]

# Map tag to display name for verification
TAG_TO_DISPLAY = {
    tag: display_name for tag, display_name, _, _ in CEPS_DATA_TYPES
}

# Downloadable tags
DOWNLOADABLE_TAGS = [
    tag for tag, _, _, downloadable in CEPS_DATA_TYPES if downloadable
]

# Czech month names mapping for date selection
CZECH_MONTHS = {
    1: "leden", 2: "únor", 3: "březen", 4: "duben",
    5: "květen", 6: "červen", 7: "červenec", 8: "srpen",
    9: "září", 10: "říjen", 11: "listopad", 12: "prosinec"
}
