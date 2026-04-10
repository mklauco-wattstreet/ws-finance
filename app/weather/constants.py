"""Open-Meteo API constants for Czech weather data."""

# Central Czechia centroid
LATITUDE = 49.80
LONGITUDE = 15.47

# API endpoints
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
PREVIOUS_RUNS_URL = "https://previous-runs-api.open-meteo.com/v1/forecast"

# Weather variables requested from all endpoints
WEATHER_VARIABLES = [
    "temperature_2m",
    "shortwave_radiation",
    "direct_radiation",
    "cloud_cover",
    "wind_speed_10m",
]

# Previous Runs API uses _previous_day1 suffix
WEATHER_VARIABLES_PREVIOUS_DAY1 = [
    f"{v}_previous_day1" for v in WEATHER_VARIABLES
]
