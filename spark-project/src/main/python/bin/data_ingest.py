def read_file(spark,header,inferSchema,path_location):
	df=spark.read.format('csv').options(header=header).options(inferSchema=inferSchema).load(path_location)
	return df