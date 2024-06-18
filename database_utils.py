import yaml
from sqlalchemy import create_engine

class DatabaseConnector():

  def read_db_creds(self):
    with open("./db_creds.yaml", "r") as stream:
      data = yaml.safe_load(stream)
      print(data)
      return data
  
  def init_db_engine(self, cred_dict):
    cred_dict = self.read_db_creds()
    engine = create_engine(f"postgresql+psycopg2://{cred_dict['RDS_USER']}:{cred_dict['RDS_PASSWORD']}@{cred_dict['RDS_HOST']}:{cred_dict['RDS_PORT']}/{cred_dict['RDS_DATABASE']}")
    return engine
  
  def list_db_tables(self, engine):
    engine = self.init_db_engine()
    

connection = DatabaseConnector()

connection.read_db_creds()