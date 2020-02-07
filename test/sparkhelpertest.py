from src.sparkhelpers import spark_start
from src.sparkhelpers import clean_data
from pyspark.sql.functions import col

import unittest

class SparkHelpersTest(unittest.TestCase):
    def test_spark_start(self):
        ss1 = spark_start()
        df1 = ss1.read.csv('Hidden-Gems/test/amsterdam-accommodation.csv')

        ss2 = spark_start()
        row = [('address', 'category', 'id', 'lat', 'lng', 'location', 'name', 'originalId', 'polarity', 'subCategory', 'details', 'reviews')]
        df2 = ss2.createDataFrame(row)
        self.assertEqual(df1.limit(1).collect(), df2.collect())

    def test_clean_data(self):
        ss = spark_start()
        df = clean_data(ss, 'Hidden-Gems/test/amsterdam-accommodation.csv')
        result = (
            df
            .select("name")
            .where(col("id") == "31598")
            .collect()[0]["name"]
        )

        self.assertEqual(result, "Meininger Hotel Amsterdam")

if __name__ == '__main__':
    unittest.main()