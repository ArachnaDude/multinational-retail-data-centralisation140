from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


def process_users(remote_creds, local_creds):
  connection = DatabaseConnector()
  remote_engine = connection.init_db_engine(remote_creds)
  
  extractor = DataExtractor()
  legacy_users_df = extractor.read_rds_table(remote_engine, "legacy_users")
  
  cleaner = DataCleaning()
  cleaned_df = cleaner.clean_user_data(legacy_users_df)
  
  connection.upload_to_db(cleaned_df, "dim_users", local_creds)




if __name__ == "__main__":

  local_creds = "./local_creds.yaml"
  remote_creds = "./db_creds.yaml"

  process_users(remote_creds, local_creds)



  # extractor = DataExtractor()
  # legacy_users_df = extractor.read_rds_table(engine, "legacy_users")
  # legacy_store_df = extractor.read_rds_table(engine, "legacy_store_details")
  # orders_df = extractor.read_rds_table(engine, "orders_table")
  # card_details_df = extractor.read_rds_table(engine, "dim_card_details")


