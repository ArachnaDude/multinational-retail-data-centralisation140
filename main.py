from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

if __name__ == "__main__":
  connection = DatabaseConnector()
  engine = connection.init_db_engine()
  table_list = connection.list_db_tables()
  for table in table_list:
    print(table)

  extractor = DataExtractor()
  