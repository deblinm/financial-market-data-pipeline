import psycopg2
from fastapi import FastAPI,HTTPException,status
import os
from dotenv import load_dotenv,find_dotenv
import requests
from datetime import datetime
from decimal import Decimal, InvalidOperation
from loguru import logger
from psycopg2 import connect
from starlette.status import HTTP_404_NOT_FOUND

load_dotenv(find_dotenv())
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
DATABASE_URL=os.getenv("DATABASE_URL")
app = FastAPI()

@app.get ("/get_stock_daily_data/{ticker}")
def return_stock_data(ticker:str):
    url_overview =f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={ALPHA_VANTAGE_API_KEY}"
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&apikey={ALPHA_VANTAGE_API_KEY}"

    # --- STEP 1: Fetch and extract exchange detail ---
    # Check if ticket exist and it is from which exchange
    try:
            ticker_detail = requests.get(url_overview,timeout=(3.0,5.0) )
            ticker_detail.raise_for_status()
            ticker_detail_data = ticker_detail.json()

            if not ticker_detail_data or "Exchange" not in ticker_detail_data:
                raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No exchange details found for ticker {ticker} on Alpha Vantage."
                )
            exchange = ticker_detail_data["Exchange"]
    except requests.exceptions.Timeout:
        raise HTTPException(status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                            detail="Alpha Vantage Overview request timed out.")
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY,
                            detail=f"Failed to communicate with Alpha Vantage: {str(e)}")

    # --- STEP 2: Database validation --- ticker+exchange should exist in stock_info table
    try :
        with psycopg2.connect(DATABASE_URL) as connection:
            with connection.cursor() as cursor:
                cursor.execute('SELECT STOCK_ID FROM "stock_data"."STOCK_INFO" WHERE TICKER=%s and EXCHANGE=%s',(ticker,exchange))
                extract_stock_id = cursor.fetchone()
                if extract_stock_id is None:
                    raise HTTPException(
                                status_code=HTTP_404_NOT_FOUND,
                                detail= f"No data found for combination of stock_id {ticker} and exchange {exchange}"
                            )
                else:
                    stock_id = extract_stock_id[0]
    except psycopg2.DatabaseError as e:
        print(f"Database error occurred: {e}")
        raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database connection failure."
        )


# --- STEP 3: Fetch Daily Time Series from alpha vantage ---
    try:
          response = requests.get(url,timeout=(3.0,5.0))
          response.raise_for_status()
          ticker_data = response.json()
    except requests.exceptions.Timeout:
          raise HTTPException(status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                              detail="Alpha Vantage Time Series request timed out.")
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Failed to fetch time series: {str(e)}")


# --- STEP 4: Parse API response values ---
    if "Error Message" in ticker_data:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=ticker_data["Error Message"]
                )
    elif  "Information" in ticker_data:
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=ticker_data["Information"]
                )
    elif "Time Series (Daily)" in ticker_data:
                final_ticker_data = []
                time_series_data = ticker_data["Time Series (Daily)"]

                try:
                    with psycopg2.connect(DATABASE_URL) as connection:
                        with connection.cursor() as cursor:
                            #looping through time series data to get the daily data
                            for date, daily_data in time_series_data.items():
                                transaction_date= datetime.strptime(date, "%Y-%m-%d").date()
                                try:
                                    opening_price = Decimal(daily_data["1. open"])
                                    closing_price = Decimal(daily_data["4. close"])
                                    days_high = Decimal(daily_data["2. high"])
                                    days_low = Decimal(daily_data["3. low"])
                                    volume = int(daily_data["5. volume"])

                                    dict_data = {
                                        "stock_id": stock_id,
                                        "date":transaction_date,
                                        "open":opening_price,
                                        "close":closing_price,
                                        "high":days_high,
                                        "low":days_low,
                                        "volume":volume
                                        }
                                    # STEP 5: Insert Records in Stock prices table ---

                                    cursor.execute('''INSERT INTO "stock_data"."STOCK_PRICES"
                                                                        (stock_id,trade_date,open_price,close_price,days_low,days_high,volume_traded)
                                                                        VALUES
                                                                        (%s,%s,%s,%s,%s,%s,%s)
                                                                        ON CONFLICT (stock_id,trade_date)
                                                                        DO UPDATE SET
                                                                        open_price = EXCLUDED.open_price,
                                                                        close_price = EXCLUDED.close_price,
                                                                        days_low = EXCLUDED.days_low,
                                                                        days_high = EXCLUDED.days_high,
                                                                        volume_traded = EXCLUDED.volume_traded,
                                                                        ingestion_dt_tm = NOW()''',
                                                       (stock_id, transaction_date, opening_price, closing_price,
                                                        days_low, days_high, volume))
                                    final_ticker_data.append(dict_data)
                                except KeyError as key_err:
                                    print(f"Error: Key {key_err} is missing from {transaction_date} data.")
                                    continue
                                except (InvalidOperation, ValueError, TypeError) as e:
                                    print(
                                        f'Error : Could not convert to Decimal/Integer for {transaction_date} records. Skipping this record and proceeding with next.')
                                    continue
                except   psycopg2.DatabaseError as e:
                                    print(f"Database error occurred: {e}")
                                    raise HTTPException(
                                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                    detail="Internal database connection failure."
                                     )


                return  {ticker:final_ticker_data}



