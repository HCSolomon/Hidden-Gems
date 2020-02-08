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

    def test_write_to_checkins(self):
        ss = spark_start("local[*]","write_to_checkins",["~/Hidden-Gems/postgresql-42.2.10.jar"],[],{})
        write_to_checkins(ss, 'Hidden-Gems/test/test_resources/Yelp/checkin.json')
        url = "jdbc:postgresql://localhost:5432/hiddengems_db"
        table = "checkins"
        mode = "overwrite"
        properties = {
            "user": "postgres", 
            "password": "password", 
            "driver": "org.postgresql.Driver"
            }
        df = ss.read.jdbc(url=url, table=table, properties=properties)
        result = df.schema.names[0]
        ss.stop()

        self.assertEqual("business_id", result)

if __name__ == '__main__':
    unittest.main()