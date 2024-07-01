from database_utils import DatabaseConnector
import pandas as pd
import tabula
import yaml
import time
import requests
import boto3
from io import StringIO


class DataExtractor():
  
  
  def read_rds_table(self, engine, table_name="legacy_users"):
    engine.connect()
    df = pd.read_sql_table(table_name, engine)
    return df
  

  def retrieve_pdf_data(self, pdf_path):

    with open (pdf_path, "r") as stream:
      config = yaml.safe_load(stream)
    link = config["LINK"]

    print("Beginning PDF read\nOperation takes approx. 130s")
    start_time = time.time()
    #returns a list of len 1, the 0th element is the dataframe containing the entire pdf data
    dfs = tabula.read_pdf(link, pages="all", stream=True, multiple_tables=False)
    end_time = time.time()
    elapsed_time = end_time - start_time
    # print(f"Execution time: {elapsed_time:.2f} seconds")
    # return the dataframe
    dataframe = dfs[0]
    print("Read complete - loading dataframe")
    
    return dataframe
  

  def list_number_of_stores(self, api_creds):

    with open (api_creds, "r") as stream:
      creds = yaml.safe_load(stream)
    
    try:
      r = requests.get(creds["num_of_stores"], headers={"x-api-key": creds["x-api-key"]})
      r.raise_for_status()
      print(f"status code: {r.status_code}")
    except requests.RequestException as e:
      print(f"Error: {e}")
      return

    number = r.json()["number_stores"]
    print(f"Data to be pulled from {number} stores")

    return number
  

  def retrieve_stores_data(self, api_creds):

    with open (api_creds, "r") as stream:
      creds = yaml.safe_load(stream)

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
  

  def extract_from_s3(self, s3_path):

    with open(s3_path, "r") as stream:
      s3_path = yaml.safe_load(stream)
    
    path = s3_path["PATH"]
    split_path = path.split("/")
    bucket = split_path[-2]
    object_name = split_path[-1]

    print(bucket)
    print(object_name)

    s3 = boto3.client("s3")
    print("Attempting to extract data from s3 bucket")
    try:
      s3_object = s3.get_object(Bucket=bucket, Key=object_name)
      s3_data = s3_object["Body"].read().decode("utf-8")
      df = pd.read_csv(StringIO(s3_data))
      print("Successfully extracted bucket data")
      return df
    except Exception as e:
      print(f"Couldn't extract data from bucket:\n{type(e)}")



if __name__ == "__main__":
  path = "./pdf_link.yaml"
  api_creds = "./api_creds.yaml"
  # connection = DatabaseConnector()
  extractor = DataExtractor()

  # table_list = connection.list_db_tables("./db_creds.yaml")


  # extractor.retrieve_pdf_data(path)

  # extractor.retrieve_stores_data(api_creds)
  extractor.extract_from_s3("./s3_path.yaml")