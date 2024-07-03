from database_utils import DatabaseConnector
import pandas as pd
import tabula
import yaml
import time
import requests
import boto3
from io import StringIO
from sqlalchemy import Engine
from typing import Dict, Any


class DataExtractor():

  """
  DataExtractor class used to manage extraction of data from AWS RDS databases, PDF, API and S3 buckets.

  Methods:
    read_rds_table(engine: Engine) -> pd.DataFrame:
      Reads a table from an AWS relational database and returns it as a pandas DataFrame.

      
    retrieve_pdf_data(pdf_path: str) - pd.DataFrame:
      Extracts data from a PDF file and returns it as a pandas DataFrame.

    
    list_number_of_stores(api_credentials: str) -> int:
      Retrieves the number of stores via an API call and returns it as an int.

    
    retrieve_stores_data(api_credentials: str) -> pd.DataFrame:
      Retrieves store data via an API call and returns it as a pandas DataFrame.


    extract_from_s3(s3_path: str) -> pd.DataFrame:
      Extracts data from an S3 bucket and returns it as a pandas DataFrame.
  """
  
  
  def read_rds_table(self, engine: Engine, table_name: str) -> pd.DataFrame:

    """
    Reads a table from an AWS RDS database and returns it as a dataframe.

    Args:
      engine (Engine): SQLAlchemy engine instance connected to the database.
      table_name (str): The name of the table to read from the database.

    Returns:
      pd.DataFrame: DataFrame containing the table data.
    """
    engine.connect()
    df = pd.read_sql_table(table_name, engine)
    return df
  

  def retrieve_pdf_data(self, pdf_path: str) -> pd.DataFrame:

    """
    Extracts data from a PDF file and returns it as a pandas DataFrame.

    Args:
      pdf_path (str): Filepath to the YAML file containing the link to the PDF.

    Returns:
      pd.DataFrame: DataFrame containing the PDF data.
    """

    with open (pdf_path, "r") as stream:
      config = yaml.safe_load(stream)
    link = config["LINK"]

    print("Beginning PDF read.\nOperation takes approx. 130s")
    start_time = time.time()
    dfs = tabula.read_pdf(link, pages="all", stream=True, multiple_tables=False)
    end_time = time.time()
    elapsed_time = end_time - start_time
    dataframe = dfs[0]
    print(f"Retrieved data in {elapsed_time:.2f} seconds")
    return dataframe
  

  def list_number_of_stores(self, api_creds: str) -> int:

    """
    Retrieves the number of stores via an API call.

    Args:
      api_creds (str): Filepath to the YAML file containing API credentials.

    Returns:
      int: Number of stores.
    """

    with open (api_creds, "r") as stream:
      creds = yaml.safe_load(stream)
    
    try:
      r = requests.get(creds["num_of_stores"], headers={"x-api-key": creds["x-api-key"]})
      r.raise_for_status()
    except requests.RequestException as e:
      print(f"Error: {e}")
      return

    number = r.json()["number_stores"]
    print(f"Data to be pulled from {number} stores")

    return number
  

  def retrieve_stores_data(self, api_creds: str) -> pd.DataFrame:
    """
    Retrieves stores data from an API and returns it as a DataFrame.

    Args:
      api_creds (str): Filepath to the YAML file containing the API credentials.
    
    Returns:
      pd.DataFrame: DataFrame containing the stores data.
    """

    with open (api_creds, "r") as stream:
      creds = yaml.safe_load(stream)
    print("Beginning API read\nOperation takes approx. 50 seconds.")
    store_path = creds["store_path"]

    total_stores = self.list_number_of_stores(api_creds)

    all_stores_data = []

    start_time = time.time()
    for store_number in range(total_stores):
      url = store_path.format(store_number)

      try:
        r = requests.get(url, headers={"x-api-key": creds["x-api-key"]})
        r.raise_for_status()
        store_data = r.json()
        all_stores_data.append(store_data)

      except requests.RequestException as e:
        print(f"Error: {e}")
        return
      
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Retrieved data in {execution_time:.2f} seconds")

    dataframe = pd.DataFrame.from_dict(all_stores_data)
    return dataframe
  

  def extract_from_s3(self, s3_path: str) -> pd.DataFrame:

    """
    Extracts data from an S3 bucket and returns it as a DataFrame.

    Args:
      s3_path (str): Filepath to the YAML file containing the S3 path information.

    Returns:
      pd.DataFrame: DataFrame containing the extracted data.
    """

    print("Attempting to extract data from s3 bucket")

    with open(s3_path, "r") as stream:
      content = yaml.safe_load(stream)
    
    if content["PATH"].startswith("https://"):
      path = content["PATH"]
      try:
        df = pd.read_json(path)
        print("Successfully extracted bucket data")
        return df
      except Exception as e:
        print(f"Couldn't extract dataframe from bucket:\n{e}")

    elif content["PATH"].startswith("s3://"):
      path = content["PATH"]
      split_path = path.split("/")
      bucket = split_path[-2]
      object_name = split_path[-1]

      s3 = boto3.client("s3")
      
      try:
        s3_object = s3.get_object(Bucket=bucket, Key=object_name)
        s3_data = s3_object["Body"].read().decode("utf-8")
        df = pd.read_csv(StringIO(s3_data))
        print("Successfully extracted bucket data")
        return df
      except Exception as e:
        print(f"Couldn't extract data from bucket:\n{type(e)}")
    
    else:
      raise ValueError("Invalid path")



if __name__ == "__main__":
  path = "./pdf_link.yaml"
  api_creds = "./api_creds.yaml"
  extractor = DataExtractor()
  # Uncomment lines below to test various methods:

  # extractor.retrieve_pdf_data(path)
  # extractor.retrieve_stores_data(api_creds)
  # extractor.extract_from_s3("./s3_path2.yaml")