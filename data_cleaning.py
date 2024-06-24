from data_extraction import DataExtractor
from database_utils import DatabaseConnector

import pandas as pd
from datetime import datetime

connection = DatabaseConnector()
engine = connection.init_db_engine()

extractor = DataExtractor()
legacy_users_df = extractor.read_rds_table(engine, "legacy_users")
legacy_store_df = extractor.read_rds_table(engine, "legacy_store_details")
orders_df = extractor.read_rds_table(engine, "orders_table")
card_details_df = extractor.read_rds_table(engine, "dim_card_details")


class DataCleaning():
  
  def clean_user_data(self, pandas_dataframe):
    
    pass