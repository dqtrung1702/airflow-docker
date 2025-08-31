import openmeteo_requests
import requests_cache
from retry_requests import retry
import pandas as pd
from Sale.config import CONFIG


class WeatherForecastProcessor:
    def __init__(self, pg_hook):
        self.pg_hook = pg_hook
        self.config = CONFIG["weather_forecast"]

    def get_weather_data(self):
        """Fetch weather data from Open-Meteo API and process it into a DataFrame."""
        # Setup the Open-Meteo API client with cache and retry on error
        openmeteo = self._setup_openmeteo_client()

        # API parameters
        params = self._get_api_params()

        # Fetch data from API
        response = openmeteo.weather_api("https://api.open-meteo.com/v1/forecast", params=params)[0]

        # Process hourly data
        hourly_data = self._process_hourly_data(response)

        # Convert to DataFrame and aggregate daily data
        hourly_dataframe = pd.DataFrame(data=hourly_data)
        daily_dataframe = hourly_dataframe.groupby("date").mean(numeric_only=True).reset_index()

        return daily_dataframe

    def _setup_openmeteo_client(self):
        """Setup Open-Meteo API client with caching and retry logic."""
        cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        return openmeteo_requests.Client(session=retry_session)

    def _get_api_params(self):
        """Return API parameters for the Open-Meteo request."""
        return {
            "latitude": 21.0245,
            "longitude": 105.8412,
            "hourly": [
                "temperature_2m", "rain", "weather_code", "cloud_cover",
                "wind_speed_10m", "soil_temperature_0cm"
            ],
            "timezone": "Asia/Bangkok",
            "past_days": 1
        }

    def _process_hourly_data(self, response):
        """Process hourly data from the API response."""
        hourly = response.Hourly()
        return {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            ).date,
            "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
            "rain": hourly.Variables(1).ValuesAsNumpy(),
            "weather_code": hourly.Variables(2).ValuesAsNumpy(),
            "cloud_cover": hourly.Variables(3).ValuesAsNumpy(),
            "wind_speed_10m": hourly.Variables(4).ValuesAsNumpy(),
            "soil_temperature_0cm": hourly.Variables(5).ValuesAsNumpy(),
        }

    def process_and_load(self):
        """Fetch weather data from API and load it into PostgreSQL."""
        # Fetch data from API
        daily_dataframe = self.get_weather_data()

        # Prepare data for insertion
        rows = [
            tuple(row)
            for row in daily_dataframe.itertuples(index=False)
        ]

        # Connect to PostgreSQL and execute queries
        conn = self.pg_hook.get_conn()
        cursor = conn.cursor()

        # Delete existing data
        unique_dates = tuple(daily_dataframe["date"].tolist())
        cursor.execute(self.config["delete_sql"], (unique_dates,))

        # Insert new data
        cursor.executemany(self.config["insert_sql"], rows)
        conn.commit()
        cursor.close()
        conn.close()
        