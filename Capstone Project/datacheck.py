def check_number_of_rows(df):
    """ counts the rows of a given dataframe
    """
    number_of_rows = df.count()
    if  number_of_rows == 0:
        print(f"Error: There are no records in {df}")
    else:
        print(f"Info: Current DF contains {number_of_rows} rows")

def check_number_of_columns(df, expected_colums):
    """ counts the columns of a given dataframe
    """
    
    number_of_columns = len(df.columns)

    if number_of_columns != expected_colums:
        print(f"Error: There are {number_of_columns} instead of {expected_colums} for the current Dataframe")
    else:
        print(f"Info: {number_of_columns} columnns for current DF.")