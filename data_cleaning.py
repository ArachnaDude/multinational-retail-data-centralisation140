import pandas as pd
from datetime import datetime


class DataCleaning():
  
  def clean_user_data(self, pandas_dataframe):
    
    df = pandas_dataframe

    def handle_index(dataframe):
      sorted_df = dataframe.sort_values(by="index")
      sorted_df.reset_index(drop=True, inplace=True)
      sorted_df.drop(columns="index", inplace=True)
      return sorted_df
    
    df = handle_index(df)

    def drop_null_and_junk_entries(dataframe):
      nulls_and_junk = dataframe.loc[(dataframe["user_uuid"] == "NULL") | (dataframe["first_name"].str.contains(r"\d"))]
      filtered_df = df.drop(nulls_and_junk.index)
      filtered_df.reset_index(drop=True, inplace=True)
      return filtered_df

    df = drop_null_and_junk_entries(df)

    def process_dates(date_str):
      formats = ["%Y %B %d", "%Y/%m/%d", "%B %Y %d", "%Y-%m-%d"]
      for format in formats:
        try:
          return datetime.strptime(date_str, format)
        except ValueError:
          continue
      return pd.NaT
      
    df["date_of_birth"] = df["date_of_birth"].apply(process_dates)
    df["join_date"] = df["join_date"].apply(process_dates)

    def handle_country_code(dataframe):
      dataframe.loc[dataframe["country_code"] == "GGB", "country_code"] = "GB"
      return dataframe

    df = handle_country_code(df)

    def handle_bad_emails(dataframe):
      dataframe["email_address"] = dataframe["email_address"].str.replace("@@", "@")
      return dataframe
    
    df = handle_bad_emails(df)

    return df
