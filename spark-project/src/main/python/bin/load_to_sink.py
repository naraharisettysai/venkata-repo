def load_to_bigquery(df,table_name):
	table_name=f"spark_load.{table_name}"
	df.write.format("Bigquery")\
	.option("table",table_name)\
	.option("temporaryGcsBucket","files_dataflow/staging_spark")\
	.save()
	return "Dataframe loaded successfully to bigquery"


def load_to_gcs(df,location_path):
	path=location_path
	df.write.mode("overwrite").parquet(path)
	return "Dataframe loaded successfully to GCS"
	

