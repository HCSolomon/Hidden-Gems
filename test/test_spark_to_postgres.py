from src.spark_to_postgres_helpers import write_to_checkins
from src.sparkhelpers import spark_start

import unittest

class TestSparkToPostgres(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()