import yaml
from sqlalchemy import create_engine, MetaData, Engine
import pandas as pd

from typing import Dict, Union, List

class DatabaseConnector():

  """
  DatabaseConnector class used to govern connections to remote and local databases.

  Methods:
    read_db_creds(database_credentials):
      Reads and returns database credentials from a YAML file.

    
    init_db_engine(database_credentials):
      Initialises a SQLAlchemy engine using provided credentials.


    list_db_tables(database_credentials):
      Verifies a connection to a database by returning all tables contained.

      
    upload_to_db(dataframe, table_name, database_credentials):
      Uploads a pandas dataframe to a database table.
  """


  def read_db_creds(self, credentials: str) -> Dict[str, Union[str, int]]:
    
    """
    Reads the database credentials from a user-provided YAML file.

    Args:
      credentials (str): Filepath to the YAML file containing credentials.

    Returns:
      Dict[str, Union[str, int]]: Python dict containing database credentials. 
    """

    with open(credentials, "r") as stream:
      data = yaml.safe_load(stream)
    return data
  
  def init_db_engine(self, credentials:str) -> Engine:

    """
    Initialises a SQLAlchemy engine using supplied credentials.

    Args:
      credentials (str): Filepath to the YAML file containing credentials.

    Returns:
      Engine: SQLAlchemy engine instance connected to the database.
    """
    creds = self.read_db_creds(credentials)

    HOST = creds["HOST"]
    PASSWORD = creds["PASSWORD"]
    USER = creds["USER"]
    DATABASE = creds["DATABASE"]
    PORT = creds["PORT"]

    engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
    return engine
  
  def list_db_tables(self, credentials: str) -> List[str]:

    """
    Verifies connection to database and lists all tables contained.

    Args:
      credentials (str): Filepath to the YAML file containing credentials.

    Returns:
      List[str]: List of table names in the database.
    """
    engine = self.init_db_engine(credentials)
    engine.execution_options(isolation_level="AUTOCOMMIT").connect()
    metadata = MetaData()
    metadata.reflect(engine)
    print("Tables in this database:")
    
    for table in metadata.tables.keys():
      print(table)
    return list(metadata.tables.keys())
  
  def upload_to_db(self, dataframe: pd.DataFrame, table_name: str, credentials: str) -> None:

    """
    Uploads a pandas DataFrame to a local database table.

    Args:
      dataframe (pd.DataFrame): The DataFrame to be uploaded.
      table_name (str): The name of the table to upload data as.
      credentials (str): Filepath to the YAML file containing local database credentials.

    Returns:
      None
    """

    engine = self.init_db_engine(credentials)
    dataframe.to_sql(table_name, engine, if_exists="replace", index=False)

    

if __name__ == "__main__":
  cred_path = "./db_creds.yaml"
  connection = DatabaseConnector()
  connection.list_db_tables(cred_path)