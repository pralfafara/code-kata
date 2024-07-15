import unittest
import os
from pyspark.sql import SparkSession
from demyst_prob_2 import generate_data, write_to_file, hash_pii_data

class TestDemystProblem2(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestDemystProblem2") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.maxResultsSize", "1g") \
            .getOrCreate()

    def test_generate_data(self):
        num = 10
        df = generate_data(num)
        self.assertEqual(df.count(), num)
        self.assertEqual(len(df.columns), 4)
        self.assertListEqual(df.columns, ["first_name", "last_name", "address", "date_of_birth"])

    def test_write_to_file(self):
        df = generate_data(10)
        fileloc = '/Users/palfafara/Documents/Projects/demyst/prob-2-data-set/test/raw/' #change file location
        write_to_file(df, fileloc, 'N')
        self.assertTrue(os.path.exists(fileloc))

    def test_hash_pii_data(self):
        df = generate_data(10)
        hashed_df = hash_pii_data(df)
        fileloc = '/Users/palfafara/Documents/Projects/demyst/prob-2-data-set/test/transformed/' #change file location
        write_to_file(hashed_df, fileloc, 'N')
        for i in ["first_name", "last_name", "address"]:
            original_val = df.select(i).collect()
            hashed_val = hashed_df.select(i).collect()
            self.assertNotEqual(original_val, hashed_val)

if __name__ == '__main__':
    unittest.main()