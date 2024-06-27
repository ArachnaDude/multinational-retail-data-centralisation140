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

  def clean_card_data(self, pandas_dataframe):
    df = pandas_dataframe

    def drop_null_and_junk_values(dataframe):
      dataframe = dataframe.drop(dataframe.loc[dataframe.isnull().sum(axis=1) >= 3].index)
      dataframe = dataframe.drop(dataframe.loc[dataframe["card_number"].str.match(r"[a-zA-Z]")].index)
      dataframe = dataframe.reset_index(drop=True)
      return dataframe
    
    df = drop_null_and_junk_values(df)

    def handle_merged_columns(dataframe):
      merged_rows = dataframe[(dataframe["card_number"].str.contains(r"/")) & (dataframe["expiry_date"].isnull())].index
      df_to_fix = dataframe.loc[merged_rows]
      df_to_fix[["card_number", "expiry_date"]] = df_to_fix["card_number"].str.split(" ", expand=True)
      dataframe.update(df_to_fix)
      return dataframe
    
    df = handle_merged_columns(df)

    def process_dates(date_str):
      formats = ["%Y %B %d", "%Y/%m/%d", "%B %Y %d", "%Y-%m-%d"]
      for format in formats:
        try:
          return datetime.strptime(date_str, format)
        except ValueError:
          continue
      return pd.NaT

    df["date_payment_confirmed"] = df["date_payment_confirmed"].apply(process_dates)

    def handle_card_number_clean(dataframe):
      match_pattern = dataframe["card_number"].str.match(r"^\d+$")
      non_matching = dataframe[~match_pattern].index
      question_mark_card_numbers = dataframe.loc[non_matching]
      question_mark_card_numbers["card_number"] = question_mark_card_numbers["card_number"].str.replace(r"\?+", "", regex=True)
      dataframe.update(question_mark_card_numbers)
      return dataframe
    
    df = handle_card_number_clean(df)

    def handle_invalid_card_numbers(dataframe):

      def validate_diners_club(card_number):
        return len(card_number) == 14 and card_number.startswith(("300", "301", "302", "303", "304", "305", "36", "38"))

      def validate_visa(card_number):
        return len(card_number) in [13, 16, 19]  and card_number.startswith("4")

      def validate_jcb(card_number):
        if len(card_number) == 15 and card_number.startswith(("2131", "1800")):
          return True
        elif len(card_number) == 16 and card_number.startswith("35"):
          return True
        else:
          return False
        
      def validate_maestro(card_number):
        return 12 <= len(card_number) <= 19 and card_number.startswith(("50", "56", "57", "58", "6")) 

      def validate_mastercard(card_number):
        return len(card_number) == 16 and card_number.startswith(("22", "23", "24", "25", "26", "27", "51", "52", "53", "54", "55"))

      def validate_discover(card_number):
        return len(card_number) == 16 and card_number.startswith("6")

      def validate_amex(card_number):
        return len(card_number) in [15, 16] and card_number.startswith(("34", "37"))
      
      validation_dict = {
        "Diners Club / Carte Blanche": validate_diners_club,
        "VISA": validate_visa,
        "JCB": validate_jcb,
        "Maestro": validate_maestro,
        "Mastercard": validate_mastercard,
        "Discover": validate_discover,
        "American Express": validate_amex 
      }

      for provider, validate_function in validation_dict.items():
        provider_df = df[df["card_provider"].str.contains(provider)]
        invalid_cards = provider_df[~provider_df["card_number"].apply(validate_function)]
        
        if not invalid_cards.empty:
          df.drop(invalid_cards.index, inplace=True)
      
      dataframe = dataframe.reset_index(drop=True)

      return dataframe

    df = handle_invalid_card_numbers(df)

    return df