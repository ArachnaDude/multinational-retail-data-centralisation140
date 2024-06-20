from database_utils import DatabaseConnector
import pandas as pd


class DataExtractor():
  
  
  def read_rds_table(self, engine, table_name="legacy_users"):
    engine.connect()
    df = pd.read_sql_table(table_name, engine)
    # print(type(df))
    return df
  

if __name__ == "__main__":
  connection = DatabaseConnector()
  engine = connection.init_db_engine()
  table_list = connection.list_db_tables()

  extractor = DataExtractor()

  for table in table_list:
    extractor.read_rds_table(engine, table)