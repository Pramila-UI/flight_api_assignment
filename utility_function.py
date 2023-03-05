import pandas as pd

"""
Calculating the hour and minutes from the provided minutes
"""
def calculate_hour_minute(min: int):
    total_minutes =  min
    hours = total_minutes // 60
    minutes = total_minutes % 60
    time = "{}h:{}m".format(hours, minutes)

    return time


def convert_list_dict_to_df(flight_list):
    """Convert a List of Dictionaries into dataframe by from_records method."""
    df = pd.DataFrame.from_records(flight_list)

    ### removing the duplicate rows from the dataframe
    df.drop_duplicates(inplace=True , keep='first')

    return df
