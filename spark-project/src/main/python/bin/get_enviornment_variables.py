import os

#setting enviornement variables

os.environ['appname'] ='pyspark_project'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'
os.environ['path_location'] = 'gs://files_dataflow/uber_data_demo.csv'
os.environ['table_name1'] ='max_value_3_trips'
os.environ['table_name2']='sum_fair_vendor_id'
os.environ['output_path1'] = 'gs://files_dataflow/spark-project/output/max_value_3_trips'
os.environ['output_path2']= 'gs://files_dataflow/spark-project/output/sum_fair_vendor_id'


appname=os.environ['appname'] 
header=os.environ['header']
inferSchema=os.environ['inferSchema']
path_location=os.environ['path_location']
table_name1=os.environ['table_name1']
table_name2=os.environ['table_name2']
output_path1=os.environ['output_path1']
output_path2=os.environ['output_path2']