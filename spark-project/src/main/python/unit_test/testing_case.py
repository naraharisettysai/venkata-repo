import unittest
from pyspark.sql import SparkSession
from data_filtering import data_filter
from data_aggregrations import sum_fair_vendor_id

class pyspark_unit_testing(unittest.TestCase):
    def test_area_cal(self):
        spark=SparkSession.builder.appName("testing").getOrCreate()
        df=spark.read.csv("gs://files_dataflow/unit_testing_case.csv",header=True,inferSchema=True)
        filtered_data=data_filter(df,spark)
        aggregrated_data=sum_fair_vendor_id(filtered_data,spark)
        value=aggregrated_data.filter(df.VendorID==1).select("total_fare_amount").first().total_fare_amount
        # print(value)
        self.assertEqual(value,46)
        print("unit test case completed")



if __name__ == '__main__':
    unittest.main()