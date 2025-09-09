# simple_join_data.py - 간단한 PySpark 조인 테스트 데이터

import pandas as pd
import numpy as np
import os

def generate_customers_small(num_customers=10000):
    """브로드캐스트 조인용 - 작은 고객 테이블"""
    np.random.seed(42)

    countries = ["KR", "JP", "US", "CN", "DE"]
    segments = ["Premium", "Gold", "Silver", "Bronze"]

    data = {
        'customer_id': range(1, num_customers + 1),
        'customer_name': [f"Customer_{i:05d}" for i in range(1, num_customers + 1)],
        'country': np.random.choice(countries, num_customers),
        'segment': np.random.choice(segments, num_customers),
        'credit_limit': np.round(np.random.uniform(1000, 50000, num_customers), 2)
    }

    return pd.DataFrame(data)

def generate_orders_large(num_orders=2000000):
    """큰 주문 테이블"""
    np.random.seed(123)

    categories = ["Electronics", "Clothing", "Books", "Home", "Sports"]

    data = {
        'order_id': range(1, num_orders + 1),
        'customer_id': np.random.randint(1, 10001, num_orders),
        'order_date': pd.date_range(start='2023-01-01', end='2024-12-31', periods=num_orders).date,
        'category': np.random.choice(categories, num_orders),
        'quantity': np.random.randint(1, 11, num_orders),
        'amount': np.round(np.random.uniform(10.0, 1000.0, num_orders), 2)
    }

    return pd.DataFrame(data)

def generate_products_medium(num_products=100000):
    """중간 크기 제품 테이블 - 브로드캐스트 임계값 테스트용"""
    np.random.seed(456)

    brands = [f"Brand_{i}" for i in range(1, 101)]
    categories = ["Electronics", "Clothing", "Books", "Home", "Sports"]

    data = {
        'product_id': range(1, num_products + 1),
        'product_name': [f"Product_{i:06d}" for i in range(1, num_products + 1)],
        'brand': np.random.choice(brands, num_products),
        'category': np.random.choice(categories, num_products),
        'price': np.round(np.random.uniform(5.0, 500.0, num_products), 2)
    }

    return pd.DataFrame(data)

def generate_sales_large(num_sales=3000000):
    """셔플 조인용 - 큰 매출 테이블"""
    np.random.seed(789)

    data = {
        'sale_id': range(1, num_sales + 1),
        'product_id': np.random.randint(1, 100001, num_sales),
        'store_id': np.random.randint(1, 1001, num_sales),
        'sale_date': pd.date_range(start='2023-01-01', end='2024-12-31', periods=num_sales).date,
        'quantity': np.random.randint(1, 6, num_sales),
        'revenue': np.round(np.random.uniform(10.0, 2000.0, num_sales), 2)
    }

    return pd.DataFrame(data)

def generate_orders_skewed(num_orders=1500000):
    """스큐 데이터셋 - 일부 고객에게 주문이 몰린 상황"""
    np.random.seed(999)

    # 파레토 법칙: 20%의 고객이 80%의 주문을 차지
    num_customers = 10000
    hot_customers = int(num_customers * 0.2)  # 상위 20% 고객
    hot_orders = int(num_orders * 0.8)        # 전체 주문의 80%

    # 스큐된 customer_id 생성
    customer_ids = []

    # 상위 20% 고객에게 80%의 주문 할당
    hot_customer_ids = np.random.choice(range(1, hot_customers + 1), hot_orders, replace=True)
    customer_ids.extend(hot_customer_ids)

    # 나머지 80% 고객에게 20%의 주문 할당
    cold_orders = num_orders - hot_orders
    cold_customer_ids = np.random.choice(range(hot_customers + 1, num_customers + 1), cold_orders, replace=True)
    customer_ids.extend(cold_customer_ids)

    # 셔플해서 순서 섞기
    np.random.shuffle(customer_ids)

    categories = ["Electronics", "Clothing", "Books", "Home", "Sports"]

    data = {
        'order_id': range(1, num_orders + 1),
        'customer_id': customer_ids,
        'order_date': pd.date_range(start='2023-01-01', end='2024-12-31', periods=num_orders).date,
        'category': np.random.choice(categories, num_orders),
        'quantity': np.random.randint(1, 11, num_orders),
        'amount': np.round(np.random.uniform(10.0, 1000.0, num_orders), 2)
    }

    return pd.DataFrame(data)

def save_data(base_path: str):
    """데이터 생성 및 저장"""
    os.makedirs(base_path, exist_ok=True)

    print("데이터 생성 중...")

    # 브로드캐스트 조인용
    customers = generate_customers_small(10000)
    customers.to_parquet(f"{base_path}/customers_small.parquet", index=False)
    print(f"✓ customers_small.parquet ({len(customers):,}건, {os.path.getsize(f'{base_path}/customers_small.parquet')/1024/1024:.1f}MB)")

    orders = generate_orders_large(20000000)
    orders.to_parquet(f"{base_path}/orders_large.parquet", index=False)
    print(f"✓ orders_large.parquet ({len(orders):,}건, {os.path.getsize(f'{base_path}/orders_large.parquet')/1024/1024:.1f}MB)")

    # 셔플 조인용
    sales = generate_sales_large(5000000)
    sales.to_parquet(f"{base_path}/sales_large.parquet", index=False)
    print(f"✓ sales_large.parquet ({len(sales):,}건, {os.path.getsize(f'{base_path}/sales_large.parquet')/1024/1024:.1f}MB)")

    orders_skewed = generate_orders_skewed(10000000)
    orders_skewed.to_parquet(f"{base_path}/orders_skewed.parquet", index=False)
    print(f"✓ orders_skewed.parquet ({len(orders_skewed):,}건, {os.path.getsize(f'{base_path}/orders_skewed.parquet')/1024/1024:.1f}MB)")

    print("\n테스트 시나리오:")
    print("🔹 브로드캐스트 조인: customers_small ⋈ orders_large")
    print("🔹 셔플 조인: products_medium ⋈ sales_large")
    print("🔹 스큐 조인 문제: customers_small ⋈ orders_skewed (20%고객이 80%주문)")

if __name__ == "__main__":
    base_path = 'samples'
    save_data(base_path)