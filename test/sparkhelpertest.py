from src.sparkhelpers import spark_start, clean_data, write_to_checkins
from pyspark.sql.functions import col

import unittest

class SparkHelpersTest(unittest.TestCase):
    def test_spark_start(self):
        ss1 = spark_start()
        df1 = ss1.read.csv('Hidden-Gems/test/test_resources/Tourpedia/amsterdam-accommodation.csv')

        ss2 = spark_start()
        row = [('address', 'category', 'id', 'lat', 'lng', 'location', 'name', 'originalId', 'polarity', 'subCategory', 'details', 'reviews')]
        df2 = ss2.createDataFrame(row)

        self.assertEqual(df1.limit(1).collect(), df2.collect())
        ss1.stop()
        ss2.stop()

    def test_clean_data(self):
        ss = spark_start()
        df = clean_data(ss, 'Hidden-Gems/test/test_resources/Tourpedia/amsterdam-accommodation.csv')
        # df = clean_data(ss, 'Hidden-Gems/test/test_resources/Tourpedia/*.csv')
        result = (
            df
            .select("name")
            .where(col("id") == "31598")
            .collect()[0]["name"]
        )
        ss.stop()
        
        self.assertEqual(result, "Meininger Hotel Amsterdam")

    
if __name__ == '__main__':
    unittest.main()