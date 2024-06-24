from database_utils import DatabaseConnector
import pandas as pd


class DataExtractor():
  
  
  def read_rds_table(self, engine, table_name="legacy_users"):
    engine.connect()
    df = pd.read_sql_table(table_name, engine)
    return df
  

if __name__ == "__main__":
  connection = DatabaseConnector()
  # engine = connection.init_db_engine("./db_creds.yaml")
  table_list = connection.list_db_tables("./db_creds.yaml")

  # extractor = DataExtractor()

  # for table in table_list:
  #   extractor.read_rds_table(engine, table)