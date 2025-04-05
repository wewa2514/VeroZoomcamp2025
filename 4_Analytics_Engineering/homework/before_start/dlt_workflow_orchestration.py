import argparse
import dlt
import pandas as pd
import requests
import gzip
import io
import logging
import os


# Setup logging
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "app.log")

logging.basicConfig(
    filename=LOG_FILE,
    filemode='a',
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class WorkflowOrchestrationDlt:
    
    def __init__(self, taxi_type: str, start_date: str, end_date: str):
        self.taxi_type = taxi_type
        self.start_date = start_date
        self.end_date = end_date
        self.base_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{self.taxi_type}/"

    def generate_file_names(self):
        """
        Generates a list of taxi file names between two dates.
        Returns:
            List[str]: List of file names
        """
        date_range = pd.date_range(start=self.start_date, end=self.end_date, freq='MS')
        return [f"{self.taxi_type}_tripdata_{dt.strftime('%Y-%m')}.csv.gz" for dt in date_range]
    
    def get_resource(self):
        @dlt.resource(name=f"{self.taxi_type}_tripdata", write_disposition="append")
        def extract_nyc_taxi_data():
            """Fetch and process NYC taxi trip data from GitHub."""
            file_names = self.generate_file_names()
            logger.info(f"Generated file names: {file_names}")
            
            for file_name in file_names:
                file_url = f"{self.base_url}{file_name}"
                logger.info(f"Downloading: {file_name}")
                logger.debug(f"File URL: {file_url}")
                
                response = requests.get(file_url, stream=True)
                if response.status_code != 200:
                    logger.warning(f"Failed to download {file_name}, Status Code: {response.status_code}")
                    continue
                else:
                    logger.info(f"Successfully downloaded {file_name}")

                try:
                    with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
                        df = pd.read_csv(f)
                    yield df.to_dict(orient="records")
                    logger.info(f"Yielded {len(df)} rows from {file_name}")

                except Exception as e:
                    logger.error(f"Error processing {file_name}: {e}")
                    continue

        return extract_nyc_taxi_data()


def main():
    parser = argparse.ArgumentParser(description="Run DLT pipeline for NYC Taxi data.")
    parser.add_argument("--taxi_type", type=str, required=True, help="Type of taxi (e.g. 'green', 'yellow', 'fhv')")
    parser.add_argument("--start_date", type=str, required=True, help="Start date in format YYYY-MM")
    parser.add_argument("--end_date", type=str, required=True, help="End date in format YYYY-MM")
    
    args = parser.parse_args()
    
    pipeline = dlt.pipeline(
        pipeline_name="nyc_taxi_pipeline",
        destination="bigquery",
        dataset_name="nyc_taxi_data"
    )
    
    logger.info("Starting DLT pipeline...")
    workflow = WorkflowOrchestrationDlt(args.taxi_type, args.start_date, args.end_date)
    pipeline.run(workflow.get_resource())
    logger.info("All files processed and uploaded successfully!")


if __name__ == "__main__":
    main()
