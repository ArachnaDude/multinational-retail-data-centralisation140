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

    # def handle_invalid_card_numbers(dataframe):

    #   def validate_diners_club(card_number):
    #     return len(card_number) == 14 and card_number.startswith(("300", "301", "302", "303", "304", "305", "36", "38"))

    #   def validate_visa(card_number):
    #     return len(card_number) in [13, 16, 19]  and card_number.startswith("4")

    #   def validate_jcb(card_number):
    #     if len(card_number) == 15 and card_number.startswith(("2131", "1800")):
    #       return True
    #     elif len(card_number) == 16 and card_number.startswith("35"):
    #       return True
    #     else:
    #       return False
        
    #   def validate_maestro(card_number):
    #     return 12 <= len(card_number) <= 19 and card_number.startswith(("50", "56", "57", "58", "6")) 

    #   def validate_mastercard(card_number):
    #     return len(card_number) == 16 and card_number.startswith(("22", "23", "24", "25", "26", "27", "51", "52", "53", "54", "55"))

    #   def validate_discover(card_number):
    #     return len(card_number) == 16 and card_number.startswith("6")

    #   def validate_amex(card_number):
    #     return len(card_number) in [15, 16] and card_number.startswith(("34", "37"))
      
    #   validation_dict = {
    #     "Diners Club / Carte Blanche": validate_diners_club,
    #     "VISA": validate_visa,
    #     "JCB": validate_jcb,
    #     "Maestro": validate_maestro,
    #     "Mastercard": validate_mastercard,
    #     "Discover": validate_discover,
    #     "American Express": validate_amex 
    #   }

    #   for provider, validate_function in validation_dict.items():
    #     provider_df = df[df["card_provider"].str.contains(provider)]
    #     invalid_cards = provider_df[~provider_df["card_number"].apply(validate_function)]
        
    #     if not invalid_cards.empty:
    #       df.drop(invalid_cards.index, inplace=True)
      
    #   dataframe = dataframe.reset_index(drop=True)

    #   return dataframe

    # df = handle_invalid_card_numbers(df)

    return df
  
  def clean_store_data(self, pandas_dataframe):
    
    df = pandas_dataframe

    def drop_useless_columns(dataframe):
      dataframe.drop(columns=["index", "lat"], inplace=True)
      return dataframe
    
    df = drop_useless_columns(df)

    def clean_continents(dataframe):
      dataframe.loc[dataframe["continent"] == "eeEurope", "continent"] = "Europe"
      dataframe.loc[dataframe["continent"] == "eeAmerica", "continent"] = "America"
      return dataframe
    
    df = clean_continents(df)

    def drop_junk_and_null_rows(dataframe):
      junk_and_null_index = dataframe.loc[~dataframe["continent"].isin(["Europe", "America"])].index
      dataframe.drop(junk_and_null_index, inplace=True)
      dataframe.reset_index(drop=True, inplace= True)
      return dataframe
    
    df = drop_junk_and_null_rows(df)

    def process_dates(date_str):
      formats = ["%Y %B %d", "%Y/%m/%d", "%B %Y %d", "%Y-%m-%d"]
      for format in formats:
        try:
          return datetime.strptime(date_str, format)
        except ValueError:
          continue
      return pd.NaT

    df["opening_date"] = df["opening_date"].apply(process_dates)

    def handle_staff_numbers(dataframe):
      dataframe["staff_numbers"] = dataframe["staff_numbers"].str.replace(r"[^0-9]", "", regex=True)
      dataframe["staff_numbers"] = pd.to_numeric(dataframe["staff_numbers"], errors="coerce")
      return dataframe
    
    df = handle_staff_numbers(df)

    def correct_mislabeled_columns(dataframe):
      incorrect_columns = ["latitude", "longitude"]
      corrected_columns = ["longitude", "latitude"]
      dataframe.rename(columns=dict(zip(incorrect_columns, corrected_columns)), inplace=True)
      new_column_order = ["address", "latitude", "longitude", "locality", "store_code", "staff_numbers", "opening_date", "store_type", "country_code", "continent" ]
      dataframe = dataframe[new_column_order]
      return dataframe
    
    df = correct_mislabeled_columns(df)
    
    def standardise_coordinates(coordinate):
      df.loc[0, ["latitude", "longitude"]] = "N/A"
      if coordinate == "N/A":
        return "0.00000"
      
      split_coord_str = coordinate.split(".")

      return f"{split_coord_str[0]}.{split_coord_str[1]:0<5}"

    df["latitude"] = df["latitude"].apply(standardise_coordinates)
    df["longitude"] = df["longitude"].apply(standardise_coordinates)

    return df
  
  def clean_products_data(self, pandas_dataframe):
    
    df = pandas_dataframe

    def handle_null_and_junk(dataframe):
      nulls_and_junk = dataframe.loc[dataframe["category"].str.contains(r"\d", regex=True, na=True)].index
      dataframe.drop(columns="Unnamed: 0", inplace=True)
      dataframe.drop(nulls_and_junk, inplace=True)
      dataframe.reset_index(drop=True, inplace=True)
      return dataframe
    
    df = handle_null_and_junk(df)

    def convert_product_weights(weight):

      if weight.endswith(" ."):
        weight = weight.replace(" .", "")

      def handle_multipacks(multi_value):
        w1, w2 = multi_value.split(" x ")
        multiplier_to_int = int(w1)
        weight_to_int = float(w2.replace("g", ""))
        grams_to_kilos = weight_to_int/1000
        combined_weight = multiplier_to_int * grams_to_kilos
        return round(combined_weight, 3)

      def convert_oz_to_kg(oz_value):
        format_and_split_weight = int(oz_value.replace("oz", ""))
        convert_to_kg = round(format_and_split_weight * 0.02834952, 3)
        return convert_to_kg
        
      def convert_ml_to_kg(ml_value):
        format_and_split = int(ml_value.replace("ml", ""))
        convert_to_kg = round(format_and_split/1000, 3)
        return convert_to_kg
      
      def convert_g_to_kg(g_value):
        format_and_split = float(g_value.replace("g", ""))
        convert_to_kg = round(format_and_split/1000, 3)
        return convert_to_kg
      
      def convert_kg(kg_value):
        format_and_split = float(kg_value.replace("kg", ""))
        return round(format_and_split, 3)

      if "x" in weight:
        return handle_multipacks(weight)
      elif weight.endswith("kg"):
        return convert_kg(weight)
      elif weight.endswith("oz"):
        return convert_oz_to_kg(weight)
      elif weight.endswith("ml"):
        return convert_ml_to_kg(weight)
      elif weight.endswith("g"):
        return convert_g_to_kg(weight)
      else:
        return None
      
    df["weight"] = df["weight"].apply(convert_product_weights)

    def rename_weight_column(dataframe):
      dataframe.rename(columns={"weight": "weight_kg"}, inplace=True)
      return dataframe
    
    df = rename_weight_column(df)

    def format_price(price):
      price = price.replace("£", "")
      return price

    df["product_price"] = df["product_price"].apply(format_price)

    return df
  
  def clean_orders_table(self, pandas_dataframe):
    
    df = pandas_dataframe

    def drop_unneeded_columns(dataframe):
      dataframe.drop(columns=["first_name", "last_name", "1", "level_0", "index"], inplace=True)
      return dataframe
    
    df = drop_unneeded_columns(df)

    return df
  
  def clean_date_times_data(self, pandas_dataframe):
    
    df = pandas_dataframe

    def drop_nulls_and_junk(dataframe):
      null_and_junk_index = dataframe.loc[~dataframe["date_uuid"].str.match(r"^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$")].index
      dataframe.drop(null_and_junk_index, inplace=True)
      dataframe.reset_index(drop=True, inplace=True)
      return dataframe

    df = drop_nulls_and_junk(df)

    return df