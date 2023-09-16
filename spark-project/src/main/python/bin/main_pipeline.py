from pyspark.sql import SparkSession
import  get_enviornment_variables as var
import data_ingest as ingest
import data_filtering as data_filter
import data_aggregrations as data_agg
import load_to_sink as load_sink


#get enviornement variables
appname=var.appname
header=var.header
inferSchema=var.inferSchema
path_location=var.path_location
table_name1=var.table_name1
table_name2=var.table_name2
output_path1=var.output_path1
output_path2=var.output_path2


#creating the spark session
spark=SparkSession.builder.appName(appname).getOrCreate()





# Calling the data ingest script.
df=ingest.read_file(spark,header,inferSchema,path_location)

#fiter the data
filter_data=data_filter.data_filter(df,spark)

print("filtering is compelted")


#Aggregrations.
max_value_3_trips = data_agg.max_value_3_trips(filter_data,spark)
sum_fair_vendor_id=data_agg.sum_fair_vendor_id(filter_data,spark)

print("aggregrations are completed")
#loading to bigquery 
load_sink.load_to_bigquery(max_value_3_trips,table_name1)
load_sink.load_to_bigquery(sum_fair_vendor_id,table_name2)

print("loading to bigquery completed")

#loading to gcs

load_sink.load_to_gcs(max_value_3_trips,output_path1)
load_sink.load_to_gcs(sum_fair_vendor_id,output_path2)

print("loading to gcs completed")

print("The pyspark project completed")
print("ADIOS AMIGOS")