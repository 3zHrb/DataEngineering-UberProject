import Extract
import Transform
import Load
import psutil
import datetime


def pipeline(url):

    start = datetime.datetime.now()
    print("pipeline started ...")
    print(
        f"extractData ended, CPU : {psutil.cpu_percent()}, Memory: {psutil.virtual_memory().percent}"
    )
    extractedData = Extract.extractData(url)
    transformedDataDict = Transform.transforData(extractedData)
    Load.loadData(transformedDataDict)

    print(
        f"extractData ended, CPU : {psutil.cpu_percent()}, Memory: {psutil.virtual_memory().percent}"
    )
    end = datetime.datetime.now()
    print("pipeline ended ...")
    print(f"total time to execute {end - start}")


dataApiSource = "https://storage.googleapis.com/uber-data-storage-bucket/uberData2023-06-07%2019%3A48%3A49.092952.csv"
pipeline(dataApiSource)
