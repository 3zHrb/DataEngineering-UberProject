import pandas as pd
from pandas import DataFrame

from pandas.io import gbq

import pyarrow as pa
import pyarrow.parquet as pq

import os

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = "/Users/abdulazizalharbi/Downloads/uber-data-388311-1874b8443b31.json"


project_id = "uber-data-388311"
database = "uberData"


def loadData(dictOfDF):

    for dfName, df in dictOfDF.items():
        # # if dfName != "rate_code_dim":
        # print(dfName)
        # print(df)
        print(f"loadding df: {dfName}")
        # print(df.dtypes)
        df.to_gbq(
            destination_table=f"{database}.{dfName}",
            project_id=project_id,
            if_exists="replace",
        )
