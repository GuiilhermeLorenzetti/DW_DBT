"""
This script is an ETL (Extract, Transform, Load) pipeline that fetches daily
closing price data for specific commodities (crude oil, gold, and silver)
from Yahoo Finance, consolidates it into a single DataFrame, and then saves it
to a table in a PostgreSQL database.

The script is designed to be run periodically to keep the database table
updated with data from the last 5 days.
"""

# Library imports
import pandas as pd
import yfinance
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# --- CONFIGURATION ---

# List of tickers for the desired commodities on Yahoo Finance.
# CL=F -> Crude Oil
# GC=F -> Gold
# SI=F -> Silver
commodities = ['CL=F', 'GC=F', 'SI=F']

# Load environment variables from the .env file for security.
# The .env file should be in the same directory and contain the database credentials.
load_dotenv()

# Retrieve database credentials from the environment variables.
DB_HOST_PROD = os.getenv('DB_HOST_PROD')
DB_PORT_PROD = os.getenv('DB_PORT_PROD')
DB_NAME_PROD = os.getenv('DB_NAME_PROD')
DB_USER_PROD = os.getenv('DB_USER_PROD')
DB_PASS_PROD = os.getenv('DB_PASS_PROD')
DB_SCHEMA_PROD = os.getenv('DB_SCHEMA_PROD')

# Build the connection string for the PostgreSQL database.
database_url = f'postgresql://{DB_USER_PROD}:{DB_PASS_PROD}@{DB_HOST_PROD}:{DB_PORT_PROD}/{DB_NAME_PROD}'

# Create the database connection engine using SQLAlchemy.
engine = create_engine(database_url)

# --- FUNCTIONS ---

def fetch_commodity_data(symbol: str, period: str = '5y', interval: str = '1d') -> pd.DataFrame:
    """
    Fetches historical data for a single commodity from Yahoo Finance.

    Args:
        symbol (str): The asset's ticker (e.g., 'CL=F').
        period (str, optional): The time window for the data. Defaults to '5y' (5year).
        interval (str, optional): The interval between data points. Defaults to '1d' (1 day).

    Returns:
        pd.DataFrame: A DataFrame containing the historical closing prices ('Close')
                      and a column with the asset's symbol. Returns an empty
                      DataFrame if the symbol is not found or an error occurs.
    """
    try:
        ticker = yfinance.Ticker(symbol)
        # Download the history, selecting only the 'Close' price column.
        data = ticker.history(period=period, interval=interval)[['Close']]
        # Add a column to identify which commodity this data belongs to.
        data['symbol'] = symbol
        return data
    except Exception as e:
        print(f"Error fetching data for symbol {symbol}: {e}")
        return pd.DataFrame() # Return an empty DataFrame in case of an error

def concatenate_commodity_data(commodities: list) -> pd.DataFrame:
    """
    Orchestrates fetching data for a list of commodities and joins them into a single DataFrame.

    Args:
        commodities (list): A list of commodity tickers (e.g., ['CL=F', 'GC=F']).

    Returns:
        pd.DataFrame: A single DataFrame with the concatenated data for all
                      requested commodities.
    """
    all_data = pd.DataFrame()
    for symbol in commodities:
        print(f"Fetching data for {symbol}...")
        data = fetch_commodity_data(symbol)
        # Concatenate the current commodity's data with the main DataFrame.
        all_data = pd.concat([all_data, data])
    return all_data


def save_data_to_database(df: pd.DataFrame, table_name: str = 'commodities_data'):
    """
    Saves a DataFrame to a table in the PostgreSQL database.

    If the table already exists, it will be replaced.

    Args:
        df (pd.DataFrame): The DataFrame to be saved to the database.
        table_name (str, optional): The name of the table in the database.
                                     Defaults to 'commodities_data'.
    """
    if not df.empty:
        print(f"Saving data to table '{table_name}'...")
        try:
            # Use the pandas to_sql method to insert the data.
            # if_exists='replace': If the table exists, drop the old one and create a new one.
            # index=True: Save the DataFrame's index (which is the date) as a column.
            # index_label='date': Name the index column 'date'.
            df.to_sql(table_name, engine, if_exists='replace', index=True, index_label='date')
            print("Data saved successfully!")
        except Exception as e:
            print(f"Error saving data to the database: {e}")
    else:
        print("No data to save.")

# --- SCRIPT EXECUTION ---

if __name__ == "__main__":
    # 1. Extraction and Transformation: Fetch and concatenate the commodity data.
    get_data = concatenate_commodity_data(commodities)

    # 2. Load: Save the consolidated data to the database.
    save_data_to_database(get_data)