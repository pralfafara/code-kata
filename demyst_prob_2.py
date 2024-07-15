from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2
import faker

def generate_data(num):
    fake = faker.Faker()
    fake_data = [(fake.first_name()
           , fake.last_name()
           , fake.address().replace('\n', ', ')
           , fake.date_of_birth(minimum_age=20, maximum_age=60).isoformat()) for _ in range(num)]

    spark = SparkSession.builder \
              .appName("DemystProblem2") \
              .config("spark.driver.memory", "4g") \
              .config("spark.executor.memory", "4g") \
              .config("spark.driver.maxResultsSize", "2g") \
              .getOrCreate()
    df = spark.createDataFrame(fake_data, ["first_name", "last_name", "address", "date_of_birth"])

    return df

def write_to_file(df, filename, parquet_flag):
    if parquet_flag == 'Y':
        df.write \
            .mode("overwrite") \
            .parquet(filename)
    else:
        df.write \
            .option("quoteAll",True) \
            .mode("overwrite") \
            .csv(filename, header=True)
        
def hash_pii_data(df):
    df = df.withColumn("first_name", sha2(df["first_name"], 256))
    df = df.withColumn("last_name", sha2(df["last_name"], 256))
    df = df.withColumn("address", sha2(df["address"], 256))

    return df

df = generate_data(2000) #change number of records
fileloc = '/Users/palfafara/Documents/Projects/demyst/prob-2-data-set/raw/' #change file location
write_to_file(df,fileloc,'Y') #change third parameter to 'Y' to generate parquet

df2 = hash_pii_data(df)
fileloc2 = '/Users/palfafara/Documents/Projects/demyst/prob-2-data-set/transformed/' #change file location
write_to_file(df2,fileloc2,'Y') #change third parameter to 'Y' to generate parquet