def data_filter(df,spark):
	df_filtered=df.select("VendorID","tpep_pickup_datetime","passenger_count","tip_amount","fare_amount")
	return df_filtered