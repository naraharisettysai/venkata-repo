from pyspark.sql.window import Window
from pyspark.sql.functions import sum,max

def max_value_3_trips(df,spark):
	#creating the max fair amount value for every three trips
	window_spec=Window.partitionBy(df.VendorID,df.passenger_count).rowsBetween(-2,Window.currentRow)
	max_value_3_trips=df.withColumn("max_value_3_trips",max("fare_amount").over(window_spec))
	return max_value_3_trips

def sum_fair_vendor_id(df,spark):
	sum_fair_vendor_id=df.groupBy("VendorID").agg(sum("fare_amount").alias("total_fare_amount"),sum("tip_amount").alias("total_tip_amount"))
	return sum_fair_vendor_id