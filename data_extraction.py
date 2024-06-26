from database_utils import DatabaseConnector
import pandas as pd
import tabula

import time


class DataExtractor():
  
  
  def read_rds_table(self, engine, table_name="legacy_users"):
    engine.connect()
    df = pd.read_sql_table(table_name, engine)
    return df
  
  def retrieve_pdf_data(self, link_to_pdf):
    print("Beginning PDF read")
    start_time = time.time()
    #returns a list of len 1, the 0th element is the dataframe containing the entire pdf data
    dfs = tabula.read_pdf(link_to_pdf, pages="all", stream=True, multiple_tables=False)
    end_time = time.time()
    elapsed_time = end_time - start_time
    # print(f"Execution time: {elapsed_time:.2f} seconds")
    # return the dataframe
    dataframe = dfs[0]
    print("Read complete - loading dataframe")
    return dataframe
  


  

if __name__ == "__main__":
  # connection = DatabaseConnector()

  # table_list = connection.list_db_tables("./db_creds.yaml")

  link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

  extractor = DataExtractor()
  extractor.retrieve_pdf_data(link)

