from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import requests
import pandas as pd

with DAG(
    dag_id="stock_market_etl",
    start_date=datetime(year=2024, month=1, day=1, hour=9, minute=0),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    }
) as dag:
    
    @task()

    def hit_polygon_api(**context):
        # Instantiate a list of tickers that will be pulled and looped over
        stock_ticket = "AMZN"

        # Set variables
        polygon_api_key = "<your-api-key>"
        ds = context.get("ds")

        # created the URL

        response = requests.get(f"<https://api.polygon.io/v1/open-close/{stock_ticket}/{ds}?adjusted=true&apiKey={polygon_api_key}>")

        # return the raw data

        return response.json()
    hit_polygon_api()

    @task

    def flatten_market_data(polygon_response, **context):

        # create a list of headers and a list to store the normalized data in

        columns = {
            "status": "closed",
            "from": context.get("ds"),
            "symbol": "AMZN",
            "open": None,
            "high": None,
            "low": None,
            "close": None,
            "volume": None
        }

        # create a list to append the data to
        
        flattened_record = []

        for header_name, default_value in columns.items():

            flattened_record.append(polygon_response.get(header_name, default_value))

        # convert to pandas DataFrame
        
        flattened_df = pd.DataFrame([flattened_record], columns=columns.keys())

        return flattened_df

# set dependencies

    raw_market_data = hit_polygon_api()
    i = flatten_market_data(raw_market_data)

    print(i)