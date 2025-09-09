from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum

from join_test.test_utils import create_spark_session, TimeTracker


def basic_inner_join():
    tracker = TimeTracker()
    spark: SparkSession = create_spark_session(False)
    
    try:
        # 데이터 읽기
        tracker.start_step("Loading Data")
        customers_df = spark.read.parquet("samples/customers_small.parquet")
        orders_df = spark.read.parquet("samples/orders_large.parquet")
        tracker.end_step()
        
        # 데이터 정보 확인
        tracker.start_step("Data Info Check")
        print("=== Customers DataFrame Info ===")
        print(f"Count: {customers_df.count():,}")
        customers_df.show(5)
        
        print("=== Orders DataFrame Info ===")
        print(f"Count: {orders_df.count():,}")
        orders_df.show(5)
        tracker.end_step()
        
        # Inner Join
        tracker.start_step("Inner Join")
        inner_result = customers_df.join(orders_df, "customer_id", "inner")
        print("=== Inner Join Result ===")
        inner_result.show(10)
        tracker.end_step()
        
        # Left Join
        tracker.start_step("Left Join")
        left_result = customers_df.join(orders_df, "customer_id", "left")
        print("=== Left Join Result ===")
        left_result.show(10)
        tracker.end_step()
        
        # 집계 예제
        tracker.start_step("Aggregation")
        summary = inner_result.groupBy("customer_name", "segment") \
            .agg(count("order_id").alias("order_count"),
                 spark_sum("amount").alias("total_amount"))
        
        print("=== Customer Summary ===")
        summary.show(20)
        tracker.end_step()
        
    finally:
        tracker.log_summary()
        spark.stop()

if __name__ == "__main__":
    basic_inner_join()