import pandas as pd
from datetime import datetime
from typing import Union


class DataCleaning():

  """
  DataCleaning class governs the DataFrames cleaning, each method tailored to a specific table.

  Methods:
    clean_user_data(pandas_dataframe) -> pd.DataFrame
      Cleans the dim_users table.

    
      clean_card_data(pandas_dataframe) -> pd.DataFrame
        Cleans the dim_card_details table.

      
      clean_store_data(pandas_dataframe) -> pd.DataFrame
        Cleans the dim_store_details table.


      clean_products_data(pandas_dataframe) -> pd.DataFrame
        Cleans the dim_products table.


      clean_orders_table(pandas_dataframe) -> pd.DataFrame
        Cleans the orders_table table.


      clean_date_times_data(pandas_dataframe) -> pd.DataFrame
        Cleans the dim_date_times table.
  """
  
  def clean_user_data(self, pandas_dataframe: pd.DataFrame) -> pd.DataFrame:

    """
    Cleans and returns the dim_users DataFrame.

    The function contains several sub-functions that handle individual cleaning steps.
    The DataFrame is passed between these sub-functions before being returned at the end.
    The sub-functions are not designed for use individually, as each sub-function relies on
    the DataFrame being in a form assigned by the previous one. 

    Args:
      pandas_dataframe (pd.DataFrame): Uncleaned DataFrame

    Returns:
      pd.DataFrame: Cleaned DataFrame
    """
    
    df = pandas_dataframe

    def handle_index(dataframe: pd.DataFrame) -> pd.DataFrame:
      """Sorts DataFrame by 'index' column, resets index and drops unneeded column"""
      sorted_df = dataframe.sort_values(by="index")
      sorted_df.reset_index(drop=True, inplace=True)
      sorted_df.drop(columns="index", inplace=True)
      return sorted_df
    
    df = handle_index(df)

    def drop_null_and_junk_entries(dataframe: pd.DataFrame) -> pd.DataFrame:
      """Drops rows containing "NULL" entries, and numeric characters in 'first_name' column, and resets index"""
      nulls_and_junk = dataframe.loc[(dataframe["user_uuid"] == "NULL") | (dataframe["first_name"].str.contains(r"\d"))]
      filtered_df = df.drop(nulls_and_junk.index)
      filtered_df.reset_index(drop=True, inplace=True)
      return filtered_df

    df = drop_null_and_junk_entries(df)

    def process_dates(date_str: str) -> Union[datetime, pd.NaT]:
      """Attempt to parse date strings into datetime objects, or NaT if no recognised format"""
      formats = ["%Y %B %d", "%Y/%m/%d", "%B %Y %d", "%Y-%m-%d"]
      for format in formats:
        try:
          return datetime.strptime(date_str, format)
        except ValueError:
          continue
      return pd.NaT
      
    df["date_of_birth"] = df["date_of_birth"].apply(process_dates)
    df["join_date"] = df["join_date"].apply(process_dates)

    def handle_country_code(dataframe: pd.DataFrame) -> pd.DataFrame:
      """Corrects erronious entry "GGB" to "GB" in country_code column"""
      dataframe.loc[dataframe["country_code"] == "GGB", "country_code"] = "GB"
      return dataframe

    df = handle_country_code(df)

    def handle_bad_emails(dataframe: pd.DataFrame) -> pd.DataFrame:
      """Corrects @@ to @ in email_address column"""
      dataframe["email_address"] = dataframe["email_address"].str.replace("@@", "@")
      return dataframe
    
    df = handle_bad_emails(df)

    return df

  def clean_card_data(self, pandas_dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and returns the dim_card_details dataframe.
    
    The function contains several sub-functions that handle individual cleaning steps.
    The DataFrame is passed between these sub-functions before being returned at the end.
    The sub-functions are not designed for use individually, as each sub-function relies on
    the DataFrame being in a form assigned by the previous one. 

    Args:
      pandas_dataframe (pd.DataFrame): Uncleaned DataFrame

    Returns:
      pd.DataFrame: Cleaned DataFrame
    """
    df = pandas_dataframe

    def drop_null_and_junk_values(dataframe: pd.DataFrame) -> pd.DataFrame:
      """Drops rows that contain null values in three or more columns, and rows that contain junk data"""
      dataframe = dataframe.drop(dataframe.loc[dataframe.isnull().sum(axis=1) >= 3].index)
      dataframe = dataframe.drop(dataframe.loc[dataframe["card_number"].str.match(r"[a-zA-Z]")].index)
      dataframe = dataframe.reset_index(drop=True)
      return dataframe
    
    df = drop_null_and_junk_values(df)

    def handle_merged_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
      """Handles rows where "card_number" and "expiry_date" columns are merged into one cell."""
      merged_rows = dataframe[(dataframe["card_number"].str.contains(r"/")) & (dataframe["expiry_date"].isnull())].index
      df_to_fix = dataframe.loc[merged_rows]
      df_to_fix[["card_number", "expiry_date"]] = df_to_fix["card_number"].str.split(" ", expand=True)
      dataframe.update(df_to_fix)
      return dataframe
    
    df = handle_merged_columns(df)

    def process_dates(date_str: str) -> Union[datetime, pd.NaT]:
      """Attempt to parse date strings into datetime objects, or NaT if no recognised format"""
      formats = ["%Y %B %d", "%Y/%m/%d", "%B %Y %d", "%Y-%m-%d"]
      for format in formats:
        try:
          return datetime.strptime(date_str, format)
        except ValueError:
          continue
      return pd.NaT

    df["date_payment_confirmed"] = df["date_payment_confirmed"].apply(process_dates)

    def handle_card_number_clean(dataframe: pd.DataFrame) -> pd.DataFrame:
      """Remove question marks from "card_number" column"""
      match_pattern = dataframe["card_number"].str.match(r"^\d+$")
      non_matching = dataframe[~match_pattern].index
      question_mark_card_numbers = dataframe.loc[non_matching]
      question_mark_card_numbers["card_number"] = question_mark_card_numbers["card_number"].str.replace(r"\?+", "", regex=True)
      dataframe.update(question_mark_card_numbers)
      return dataframe
    
    df = handle_card_number_clean(df)

    return df
  
  def clean_store_data(self, pandas_dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and returns the dim_store_details dataframe.

    The function contains several sub-functions that handle individual cleaning steps.
    The DataFrame is passed between these sub-functions before being returned at the end.
    The sub-functions are not designed for use individually, as each sub-function relies on
    the DataFrame being in a form assigned by the previous one. 

    Args:
      pandas_dataframe (pd.DataFrame): Uncleaned DataFrame

    Returns:
      pd.DataFrame: Cleaned DataFrame
    """
    
    df = pandas_dataframe

    def drop_useless_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
      """Drops unnecessary columns"""
      dataframe.drop(columns=["index", "lat"], inplace=True)
      return dataframe
    
    df = drop_useless_columns(df)

    def clean_continents(dataframe: pd.DataFrame) -> pd.DataFrame:
      """Fixes typographical errors in continent names"""
      dataframe.loc[dataframe["continent"] == "eeEurope", "continent"] = "Europe"
      dataframe.loc[dataframe["continent"] == "eeAmerica", "continent"] = "America"
      return dataframe
    
    df = clean_continents(df)

    def drop_junk_and_null_rows(dataframe: pd.DataFrame) -> pd.DataFrame:
      """ Drops rows comprised of null or junk values"""
      junk_and_null_index = dataframe.loc[~dataframe["continent"].isin(["Europe", "America"])].index
      dataframe.drop(junk_and_null_index, inplace=True)
      dataframe.reset_index(drop=True, inplace= True)
      return dataframe
    
    df = drop_junk_and_null_rows(df)

    def process_dates(date_str: str) -> Union[datetime, pd.NaT]:
      """Attempt to parse date strings into datetime objects, or NaT if no recognised format"""
      formats = ["%Y %B %d", "%Y/%m/%d", "%B %Y %d", "%Y-%m-%d"]
      for format in formats:
        try:
          return datetime.strptime(date_str, format)
        except ValueError:
          continue
      return pd.NaT

    df["opening_date"] = df["opening_date"].apply(process_dates)

    def handle_staff_numbers(dataframe: pd.DataFrame) -> pd.DataFrame:
      """Clean and convert "staff_numbers" column to numeric values"""
      dataframe["staff_numbers"] = dataframe["staff_numbers"].str.replace(r"[^0-9]", "", regex=True)
      dataframe["staff_numbers"] = pd.to_numeric(dataframe["staff_numbers"], errors="coerce")
      return dataframe
    
    df = handle_staff_numbers(df)

    def correct_mislabeled_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
      """Correct and reorder mislabelled columns"""
      incorrect_columns = ["latitude", "longitude"]
      corrected_columns = ["longitude", "latitude"]
      dataframe.rename(columns=dict(zip(incorrect_columns, corrected_columns)), inplace=True)
      new_column_order = ["address", "latitude", "longitude", "locality", "store_code", "staff_numbers", "opening_date", "store_type", "country_code", "continent" ]
      dataframe = dataframe[new_column_order]
      return dataframe
    
    df = correct_mislabeled_columns(df)
    
    def standardise_coordinates(coordinate: str) -> str:
      """Standardise coordinate values"""
      df.loc[0, ["latitude", "longitude"]] = "N/A"
      if coordinate == "N/A":
        return "0.00000"
      
      split_coord_str = coordinate.split(".")

      return f"{split_coord_str[0]}.{split_coord_str[1]:0<5}"

    df["latitude"] = df["latitude"].apply(standardise_coordinates)
    df["longitude"] = df["longitude"].apply(standardise_coordinates)

    return df
  
  def clean_products_data(self, pandas_dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and returns the dim_products dataframe.

    The function contains several sub-functions that handle individual cleaning steps.
    The DataFrame is passed between these sub-functions before being returned at the end.
    The sub-functions are not designed for use individually, as each sub-function relies on
    the DataFrame being in a form assigned by the previous one. 

    Args:
      pandas_dataframe (pd.DataFrame): Uncleaned DataFrame

    Returns:
      pd.DataFrame: Cleaned DataFrame
    """
    
    df = pandas_dataframe

    def handle_null_and_junk(dataframe: pd.DataFrame) -> pd.DataFrame:
      """Drop unnecessary columns, and rows that consist of null values and junk data"""
      nulls_and_junk = dataframe.loc[dataframe["category"].str.contains(r"\d", regex=True, na=True)].index
      dataframe.drop(columns="Unnamed: 0", inplace=True)
      dataframe.drop(nulls_and_junk, inplace=True)
      dataframe.reset_index(drop=True, inplace=True)
      return dataframe
    
    df = handle_null_and_junk(df)

    def convert_product_weights(weight: str) -> Union[float, None]:
      """Standardises all weights into their kilogram value, and converts to a float"""
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

    def rename_weight_column(dataframe: pd.DataFrame) -> pd.DataFrame:
      """Rename the weight column to weight_kg for clarity."""
      dataframe.rename(columns={"weight": "weight_kg"}, inplace=True)
      return dataframe
    
    df = rename_weight_column(df)

    def format_price(price: str) -> str:
      """Remove pound sign from price."""
      price = price.replace("£", "")
      return price

    df["product_price"] = df["product_price"].apply(format_price)

    return df
  
  def clean_orders_table(self, pandas_dataframe: pd.DataFrame) -> pd.DataFrame:
    """ 
    Cleans and returns the orders_table dataframe.
    
    Args:
      pandas_dataframe (pd.DataFrame): Uncleaned DataFrame

    Returns:
      pd.DataFrame: Cleaned DataFrame
    """
    
    df = pandas_dataframe

    def drop_unneeded_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
      """Drops the "first_name", "last_name", "1", "level_0" and "index" columns."""
      dataframe.drop(columns=["first_name", "last_name", "1", "level_0", "index"], inplace=True)
      return dataframe
    
    df = drop_unneeded_columns(df)

    return df
  
  def clean_date_times_data(self, pandas_dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and returns the dim_date_times DataFrame
    
    Args:
      pandas_dataframe (pd.DataFrame): Uncleaned DataFrame

    Returns:
      pd.DataFrame: Cleaned DataFrame
    """
    
    df = pandas_dataframe

    def drop_nulls_and_junk(dataframe):
      """Locates all rows that contain null and junk data, drops them, and resets the index"""
      null_and_junk_index = dataframe.loc[~dataframe["date_uuid"].str.match(r"^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$")].index
      dataframe.drop(null_and_junk_index, inplace=True)
      dataframe.reset_index(drop=True, inplace=True)
      return dataframe

    df = drop_nulls_and_junk(df)

    return df