from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("local-spark").getOrCreate()
    print(f"Spark Version: {spark.version}")
    print("Hello from local!")


if __name__ == "__main__":
    main()
