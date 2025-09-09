import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum as spark_sum, avg, date_format

from join_test.test_utils import create_spark_session, TimeTracker


def sortmerge_join_test_1():
    tracker = TimeTracker()
    spark: SparkSession = create_spark_session("Sortmerge Join Test 1", False)
    
    # Spark 설정 출력
    print("=== Spark Configuration ===")
    print(f"App Name: {spark.sparkContext.appName}")
    print(f"Master: {spark.sparkContext.master}")
    print(f"Driver Memory: {spark.conf.get('spark.driver.memory', 'default')}")
    print("=" * 50)
    
    try:
        # 데이터 읽기 - orders와 shipments로 셔플 조인
        tracker.start_step("Loading Data")
        orders_df = spark.read.parquet("samples/orders_large.parquet")
        shipments_df = spark.read.parquet("samples/shipments_large.parquet")
        tracker.end_step()
        
        # 데이터 정보 확인
        print("=== Orders DataFrame Info ===")
        print(f"Count: {orders_df.count():,}")
        orders_df.show(5)
        
        print("=== Shipments DataFrame Info ===")
        print(f"Count: {shipments_df.count():,}")
        shipments_df.show(5)

        # 조인 - order_id 기준
        tracker.start_step("Sortmerge Join by Order ID")
        joined_result = orders_df.alias("o").join(
            shipments_df.alias("s"), 
            "order_id",
            "inner"
        )
        print("=== Sortmerge Join Result ===")
        print(f"Count: {joined_result.count():,}")
        joined_result.select("order_id", "o.amount", "o.category", "s.carrier", "s.status", "s.shipping_cost").show(10)
        tracker.end_step()

        # 간단한 집계 - 배송 상태별 분석
        tracker.start_step("Aggregation by Status")
        status_summary = joined_result.groupBy("s.status") \
            .agg(count("order_id").alias("order_count"),
                 spark_sum("o.amount").alias("total_amount"),
                 avg("s.shipping_cost").alias("avg_shipping_cost"))
        
        print("=== Status Summary ===")
        status_summary.orderBy("total_amount", ascending=False).show()
        tracker.end_step()
        
    finally:
        tracker.log_summary()
        time.sleep(6000)
        spark.stop()

if __name__ == "__main__":
    sortmerge_join_test_1()