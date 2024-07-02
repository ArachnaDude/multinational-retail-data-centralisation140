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


def process_dim_card_details(pdf_path, local_creds):
  connection = DatabaseConnector()

  extractor = DataExtractor()
  dim_card_details_df = extractor.retrieve_pdf_data(pdf_path)
  
  cleaner = DataCleaning()
  cleaned_df = cleaner.clean_card_data(dim_card_details_df)

  connection.upload_to_db(cleaned_df, "dim_card_details", local_creds)


def process_store_data(api_creds, local_creds):
  connection = DatabaseConnector()

  extractor = DataExtractor()
  store_details_df = extractor.retrieve_stores_data(api_creds)

  cleaner = DataCleaning()
  cleaned_df = cleaner.clean_store_data(store_details_df)

  connection.upload_to_db(cleaned_df, "dim_store_details", local_creds)


def process_products_data(s3_path, local_creds):
  connection = DatabaseConnector()

  extractor = DataExtractor()
  product_details_df = extractor.extract_from_s3(s3_path)
  
  cleaner = DataCleaning()
  cleaned_df = cleaner.clean_products_data(product_details_df)

  connection.upload_to_db(cleaned_df, "dim_products", local_creds)


def process_orders_table(remote_creds, local_creds):
  connection = DatabaseConnector()
  remote_engine = connection.init_db_engine(remote_creds)

  extractor = DataExtractor()
  orders_df = extractor.read_rds_table(remote_engine, "orders_table")

  cleaner = DataCleaning()
  cleaned_df = cleaner.clean_orders_table(orders_df)

  connection.upload_to_db(cleaned_df, "orders_table", local_creds)


def process_date_times(s3_path, local_creds):
  connection = DatabaseConnector()

  extractor = DataExtractor()
  date_times_df = extractor.extract_from_s3(s3_path)

  cleaner = DataCleaning()
  cleaned_df = cleaner.clean_date_times_data(date_times_df)

  connection.upload_to_db(cleaned_df, "dim_date_times", local_creds)



if __name__ == "__main__":

  local_creds = "./local_creds.yaml"
  remote_creds = "./db_creds.yaml"
  process_users(remote_creds, local_creds)


  path_to_pdf = "./pdf_link.yaml"
  process_dim_card_details(path_to_pdf, local_creds)

  api_creds = "./api_creds.yaml"
  process_store_data(api_creds, local_creds)

  s3_path = "./s3_path.yaml"
  process_products_data(s3_path, local_creds)

  process_orders_table(remote_creds, local_creds)

  path_2 = "./s3_path2.yaml"
  process_date_times(path_2, local_creds)


  # extractor = DataExtractor()
  # legacy_users_df = extractor.read_rds_table(engine, "legacy_users")
  # legacy_store_df = extractor.read_rds_table(engine, "legacy_store_details")
  # orders_df = extractor.read_rds_table(engine, "orders_table")
  # card_details_df = extractor.read_rds_table(engine, "dim_card_details")


