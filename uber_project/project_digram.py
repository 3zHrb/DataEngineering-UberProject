from diagrams import Diagram, Cluster
from diagrams.gcp.storage import Storage
from diagrams.gcp.compute import ComputeEngine
from diagrams.gcp.analytics import Bigquery
from diagrams.custom import Custom
from urllib.request import urlretrieve


lookerIcon = urlretrieve(
    "https://kondado.io/assets/images/visualization_lookerstudio.png",
    "Looker-Studio-Logo.png",
)

pandasIcon = urlretrieve(
    url="https://upload.wikimedia.org/wikipedia/commons/thumb/e/ed/Pandas_logo.svg/2560px-Pandas_logo.svg.png",
    filename="pandasIcon.png",
)

with Diagram("Uber Project Pipeline - Built By: Abdulaziz Alharbi", show="false"):

    with Cluster("ETL"):
        with Cluster("Extract"):
            gcpStorage = Storage("GCP Storage")
        with Cluster(label="Transform"):
            gcpCompute = Custom("Pandas", "pandasIcon.png")
        with Cluster("Load"):
            gcpBigQuery = Bigquery("GCP BigQuery")

        etl = gcpStorage >> gcpCompute >> gcpBigQuery

    with Cluster("Data Analysis"):
        lookerStudio = Custom("looker Studio", icon_path="Looker-Studio-Logo.png")

    etl >> lookerStudio
