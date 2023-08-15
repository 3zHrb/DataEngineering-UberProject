import pandas as pd
import requests
import io



def extractData(FileUrl):
    response = requests.get(FileUrl)
    df = pd.read_csv(io.StringIO(response.text), sep=",")
    return df
