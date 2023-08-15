import pandas as pd
from pandas import DataFrame



def transforData(df: DataFrame):

    # Data Cleaning
    # removing rows with null value

    df.dropna(axis=0, how="any", inplace=True)

    # converting some column types from float to integer
    df.loc[:, ["VendorID", "passenger_count", "RatecodeID", "payment_type"]] = df.loc[
        :, ["VendorID", "passenger_count", "RatecodeID", "payment_type"]
    ].astype(int)

    # converting both tpep_pickup_datetime and tpep_dropoff_datetime columns to datetime type
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    # # Feature Engineering
    # # adding a new column called trip_duration that calculates how long each trip took in minutes
    df["trip_duration"] = round(
        (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds()
        / 60,
        2,
    )

    # # setting ride_id as the primary key for this table
    df["trip_id"] = df.index
    ## reindexing (reordering) the columns
    df = df.reindex(
        columns=[
            "trip_id",
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "trip_duration",
            "pickup_longitude",
            "pickup_latitude",
            "RatecodeID",
            "store_and_fwd_flag",
            "dropoff_longitude",
            "dropoff_latitude",
            "payment_type",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
        ]
    )

    # # Data Modeling
    # creating datetime_dim
    datetime_dim = df[["tpep_pickup_datetime", "tpep_dropoff_datetime"]].reset_index(
        drop=True
    )
    datetime_dim["tpep_pickup_datetime"] = df["tpep_pickup_datetime"]
    datetime_dim["datetime_id"] = datetime_dim.index
    datetime_dim["year"] = datetime_dim["tpep_pickup_datetime"].dt.year
    datetime_dim["pickup_month"] = datetime_dim["tpep_pickup_datetime"].dt.month
    datetime_dim["pickup_day"] = datetime_dim["tpep_pickup_datetime"].dt.day
    datetime_dim["pickup_hour"] = datetime_dim["tpep_pickup_datetime"].dt.hour

    datetime_dim["dropoff_month"] = df["tpep_dropoff_datetime"].dt.month
    datetime_dim["dropoff_day"] = df["tpep_dropoff_datetime"].dt.day
    datetime_dim["dropoff_hour"] = df["tpep_dropoff_datetime"].dt.hour

    datetime_dim["trip_id"] = df["trip_id"]

    datetime_dim = datetime_dim.reindex(
        columns=[
            "datetime_id",
            "trip_id",
            "tpep_pickup_datetime",
            "pickup_month",
            "pickup_day",
            "pickup_hour",
            "tpep_dropoff_datetime",
            "dropoff_month",
            "dropoff_day",
            "dropoff_hour",
        ]
    )

    # # creating trip_payment_dim table
    # creating payment_dim
    payment_type_name = {
        1: "Credit card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip",
    }

    payment_dim = df[["payment_type"]].reset_index(drop=True)
    payment_dim["payment_id"] = payment_dim.index
    payment_dim["payment_name"] = df["payment_type"].map(payment_type_name)
    payment_dim["trip_id"] = df["trip_id"]
    payment_dim = payment_dim.reindex(
        columns=["payment_id", "trip_id", "payment_type", "payment_name"]
    )

    # creating rate_code_dim
    rate_code_type = {
        1: "Standard rate",
        2: "JFK",
        3: "Newark",
        4: "Nassau or Westchester",
        5: "Negotiated fare",
        6: "Group ride",
    }

    rate_code_dim = pd.DataFrame()
    rate_code_dim.index = df.index
    rate_code_dim["ratecode_id"] = rate_code_dim.index
    rate_code_dim["RatecodeID"] = df["RatecodeID"]
    rate_code_dim["ratecode_name"] = rate_code_dim["RatecodeID"].map(rate_code_type)
    rate_code_dim["trip_id"] = df["trip_id"]
    rate_code_dim = rate_code_dim.reindex(
        columns=["ratecode_id", "trip_id", "RatecodeID", "ratecode_name"]
    )

    # creating geoInfo_dim
    geoInfo_dim = pd.DataFrame()
    geoInfo_dim.index = df.index
    geoInfo_dim["geoInfo_id"] = geoInfo_dim.index
    geoInfo_dim["pickup_longitude"] = df["pickup_longitude"]
    geoInfo_dim["pickup_latitude"] = df["pickup_latitude"]
    geoInfo_dim["dropoff_longitude"] = df["dropoff_longitude"]
    geoInfo_dim["dropoff_latitude"] = df["dropoff_latitude"]
    geoInfo_dim["trip_id"] = df["trip_id"]

    geoInfo_dim = geoInfo_dim.reindex(
        columns=[
            "geoInfo_id",
            "trip_id",
            "pickup_longitude",
            "pickup_latitude",
            "dropoff_longitude",
            "dropoff_latitude",
        ]
    )

    ## creating fact_table
    fact_table = (
        df.merge(datetime_dim, right_on="trip_id", left_on="trip_id")
        .merge(payment_dim, right_on="trip_id", left_on="trip_id")
        .merge(rate_code_dim, right_on="trip_id", left_on="trip_id")
        .merge(geoInfo_dim, right_on="trip_id", left_on="trip_id")[
            [
                "trip_id",
                "datetime_id",
                "geoInfo_id",
                "payment_id",
                "ratecode_id",
                "VendorID",
                "passenger_count",
                "trip_distance",
                "trip_duration",
                "fare_amount",
                "extra",
                "mta_tax",
                "tip_amount",
                "tolls_amount",
                "improvement_surcharge",
                "total_amount",
            ]
        ]
    )

    Data = {
        "datetime_dim": datetime_dim,
        "payment_dim": payment_dim,
        "rate_code_dim": rate_code_dim,
        "geoInfo_dim": geoInfo_dim,
        "fact_table": fact_table,
    }

    return Data
