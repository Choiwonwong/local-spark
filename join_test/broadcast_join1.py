import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum as spark_sum, broadcast

from join_test.test_utils import create_spark_session, TimeTracker


def broadcast_join_test_1():
    tracker = TimeTracker()
    spark: SparkSession = create_spark_session("Broadcast Join Test 1",False)
    
    # Spark 설정 출력
    print("=== Spark Configuration ===")
    print(f"App Name: {spark.sparkContext.appName}")
    print(f"Master: {spark.sparkContext.master}")
    print(f"Driver Memory: {spark.conf.get('spark.driver.memory', 'default')}")
    print(f"Driver Cores: {spark.conf.get('spark.driver.cores', 'default')}")
    print("=" * 50)
    
    try:
        # 데이터 읽기
        tracker.start_step("Loading Data")
        customers_df = spark.read.parquet("samples/customers_small.parquet")
        orders_df = spark.read.parquet("samples/orders_large.parquet")
        tracker.end_step()
        
        # 데이터 정보 확인
        print("=== Customers DataFrame Info ===")
        print(f"Count: {customers_df.count():,}")
        customers_df.show(5)
        
        print("=== Orders DataFrame Info ===")
        print(f"Count: {orders_df.count():,}")
        orders_df.show(5)

        # 집계 예제
        tracker.start_step("Aggregation with Inner")
        summary_with_inner = orders_df.join(customers_df, "customer_id", "inner").groupBy("customer_name") \
            .agg(count("order_id").alias("order_count"),
                 spark_sum("amount").alias("total_amount"))
        
        print("=== Customer Summary with Inner ===")
        summary_with_inner.show(20)
        tracker.end_step()

        tracker.start_step("Aggregation with Left")
        summary_with_left = broadcast(customers_df).join(orders_df, "customer_id", "left").groupBy("customer_name") \
            .agg(count("order_id").alias("order_count"),
                 spark_sum("amount").alias("total_amount"))
        # Wrong Case - need more memory
        # summary_with_left = customers_df.join(broadcast(orders_df), "customer_id", "left").groupBy("customer_name") \
        #     .agg(count("order_id").alias("order_count"),
        #          spark_sum("amount").alias("total_amount"))
        print("=== Customer Summary with Left ===")
        summary_with_left.show(20)
        tracker.end_step()
        
    finally:
        tracker.log_summary()
        time.sleep(6000)
        spark.stop()

if __name__ == "__main__":
    broadcast_join_test_1()