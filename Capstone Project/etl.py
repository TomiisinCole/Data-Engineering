from pyspark.sql import SparkSession, SQLContext, GroupedData
from pyspark.sql.functions import *
from pyspark.sql.functions import date_add as dt_add
from pyspark.sql import functions as F
from pyspark.sql.functions import year, month, to_date, col
from pyspark.sql.types import StructType, StructField as Fld, StringType as Str, IntegerType as Int, DoubleType as Dbl, ByteType as Bt, FloatType as Flt
from pyspark.sql.types import DateType
import re
from pyspark.sql.functions import udf
import pandas as pd
from datacheck import check_number_of_rows, check_number_of_columns
ports = {}


@udf
def city_to_port(city):
   
    for key in ports:
        if city.lower() in ports[key].lower():
            return key

@udf
def date_to_timestamp(date_sas: float) -> int:
    """
    transforms date to timestamp
    """

    if date_sas:
        datetime = pd.to_timedelta((date_sas), unit='D') + pd.Timestamp('1960-1-1')
        timestamp = datetime.timestamp()
        return timestamp
    
    

def ports_code():
    """
    Cities and their respective port codes
    """ 

    with open("I94_SAS_Labels_Descriptions.SAS") as f:
        lines = f.readlines()

    re_compiled = re.compile(r"\'(.*)\'.*\'(.*)\'")
    for line in lines[302:961]:
        results = re_compiled.search(line)
        ports[results.group(1)] = results.group(2)


""" 
Data Quality checks - number of rows and columns 

"""        

OUTPUT = "tables"

def create_spark_session():
    spark = SparkSession.builder().appName("Capstone").getOrCreate()
    return spark

# Creating dataframe

def get_df_immigration(spark):

    immigration_data = f'sas_data/part-00000-b9542815-7a8d-45fc-9c67-c9c5007ad0d4-c000.snappy.parquet'

    # Selecting only the columns required 
    df_immigration = spark.read.parquet(immigration_data)\
                    .select('cicid', 'arrdate', 'depdate', 'i94cit', 'i94res', 'i94port', 'fltno', 'biryear', 'gender', 'i94visa')

    df_immigration = df_immigration.dropDuplicates(['cicid']) # only column with unique data

    
    df_immigration = df_immigration.withColumn('arrival_ts', date_to_timestamp(df_immigration['arrdate']))
    df_immigration = df_immigration.withColumn('departure_ts',date_to_timestamp(df_immigration['depdate']))
    
    return df_immigration

def process_data(spark):

    df_immigrant = get_df_immigration(spark)
    
    create_fact_immigrant(df_immigrant)
    create_dimension_date(df_immigrant)
    create_city_dimension(spark)

# Creating immigration fact table

def create_fact_immigrant(df_immigration):
  
    fact_immigration = df_immigration.select('cicid', 'arrival_ts', 'departure_ts', 'i94cit', 'i94res', 'i94port', 'fltno').dropDuplicates()
        
    
    # writing to the fact-table

    fact_immigration.write.parquet(f'{OUTPUT}/fact_immigration', mode='overwrite')
    

# creating time dimension and doing some date cleaning

def create_dimension_date(df_immigration):
    
    df_time = df_immigration.select('arrival_ts', 'departure_ts')
    df_time = df_time.select('arrival_ts').unionAll(df_time.select('departure_ts'))
    df_time.dropDuplicates()
    df_time.dropna()
    df_time = df_time.withColumnRenamed('arrival_ts', 'ts')


    dimension_time = df_time.select('ts') \
                .withColumn('date', F.from_unixtime(F.col('ts')/1000)) \
                .withColumn('year', F.year('ts')) \
                .withColumn('month', F.month('ts')) \
                .withColumn('week', F.weekofyear('ts')) \
                .withColumn('weekday', F.dayofweek('ts')) \
                .withColumn('day', F.dayofyear('ts')) \
                .withColumn('hour', F.hour('ts'))
    
    
    check_number_of_rows(dimension_time)
    check_number_of_columns(dimension_time, 8)
    
    
    print('date dimension data validation complete')

    # writing to the table
    dimension_time.write.parquet(f'{OUTPUT}/dimension_time', mode='overwrite', partitionBy=['year', 'month'])
    


def create_city_dimension(spark):
    city_schema = StructType([
        Fld("city_name", Str()),
        Fld("state", Str()),
        Fld("median_age", Dbl()),
        Fld("male_population", Int()),
        Fld("total_population", Int()),
        Fld("foreign_born", Int()),
        Fld("average_householdsize", Dbl()),
        Fld("state_code", Int()),
    ])

    
    city = f'us-cities-demographics.csv'
    city_dimension =  spark.read.option("delimiter", ";").csv(city, schema=city_schema, header=True)

    ports_code() 
    city_dimension = city_dimension.withColumn("city_code", city_to_port(city_dimension["city_name"]))
    #city_to_port(city_dimension["city_name"])

    city_dimension.printSchema()
    
    #data validation
    
    #number_of_rows = city_dimension.count()
    #if  number_of_rows == 0:
        #print(f"Error: There are no records in {city_dimension}")
    #else:
        #print(f"Info: Current DF contains {number_of_rows} rows")


    check_number_of_rows(city_dimension)
    check_number_of_columns(city_dimension, 9)
    
    print('City dimension data validation complete')


    # writing to the table
    city_dimension.write.parquet(f'{OUTPUT}/city_dimension', mode='overwrite')
    
        
def create_immigrant_dimension(df_immigration):
    
    immigrant_dimension_indiv = df_immigration.select('cicid', 'biryear', 'gender', 'i94visa').dropDuplicates()
    
    #data validation

    check_number_of_rows(immigrant_dimension_indiv)
    check_number_of_columns(immigrant_dimension_indiv, 4)
    print('immigrant dimension data validation complete')

    # writing to the table
    immigrant_dimension_indiv.write.parquet(f'{OUTPUT}/immigrant_dimension_indiv', mode='overwrite')


def main():
    """
    create a spark session
    """
    spark = create_spark_session()    
    process_data(spark)    


if __name__ == "__main__":
    main()