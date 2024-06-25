import yaml
from sqlalchemy import create_engine, MetaData
import pandas as pd

class DatabaseConnector():

  def read_db_creds(self, credentials):
    with open(credentials, "r") as stream:
      data = yaml.safe_load(stream)
    return data
  
  def init_db_engine(self, credentials):
    creds = self.read_db_creds(credentials)

    # DATABASE_TYPE = "postgresql"
    # DBAPI = "psycopg2"
    HOST = creds["HOST"]
    PASSWORD = creds["PASSWORD"]
    USER = creds["USER"]
    DATABASE = creds["DATABASE"]
    PORT = creds["PORT"]

    engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
    return engine
  
  def list_db_tables(self, credentials):
    engine = self.init_db_engine(credentials)
    engine.execution_options(isolation_level="AUTOCOMMIT").connect()
    metadata = MetaData()
    metadata.reflect(engine)
    print("Tables in this database:")
    
    for table in metadata.tables.keys():
      print(table)
    return list(metadata.tables.keys())
  
  def upload_to_db(self, dataframe, table_name, local_db_credentials):
    engine = self.init_db_engine(local_db_credentials)
    dataframe.to_sql(table_name, engine, if_exists="replace", index=False)

    

if __name__ == "__main__":
  cred_path = "./db_creds.yaml"
  connection = DatabaseConnector()
  connection.list_db_tables(cred_path)