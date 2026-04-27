import unittest
from pyspark.sql import SparkSession
from job import process_data

class TestSparkJob(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("SparkUnitTests") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_process_data(self):
        # Mock data for bins
        bins_data = [(1, "Zone A"), (2, "Zone B"), (3, "Zone A")]
        df_bins = self.spark.createDataFrame(bins_data, ["id", "zone_id"])

        # Mock data for current state
        current_data = [(1, 10.5), (2, 20.0), (3, 15.5)]
        df_current = self.spark.createDataFrame(current_data, ["bin_id", "estimated_weight_kg"])

        # Run process_data
        result = process_data(df_bins, df_current)
        
        rows = result.collect()
        results_dict = {row['zone_id']: row['avg_weight'] for row in rows}
        
        self.assertEqual(results_dict["Zone A"], 13.0)
        self.assertEqual(results_dict["Zone B"], 20.0)

if __name__ == "__main__":
    unittest.main()
