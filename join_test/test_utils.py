import time
import logging
from functools import wraps
from typing import Callable, Any

from pyspark.sql import SparkSession

def setup_logger(name: str = "spark_join_test") -> logging.Logger:
    """로거 설정"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger

def measure_time(operation_name: str = None):
    """함수 실행 시간을 측정하는 데코레이터"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            logger = setup_logger()
            op_name = operation_name or func.__name__
            
            logger.info(f"🚀 Starting: {op_name}")
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                end_time = time.time()
                duration = end_time - start_time
                
                logger.info(f"✅ Completed: {op_name} - Duration: {duration:.2f}s")
                return result
                
            except Exception as e:
                end_time = time.time()
                duration = end_time - start_time
                logger.error(f"❌ Failed: {op_name} - Duration: {duration:.2f}s - Error: {str(e)}")
                raise
                
        return wrapper
    return decorator

class TimeTracker:
    """단계별 시간 추적 클래스"""
    
    def __init__(self):
        self.logger = setup_logger()
        self.steps = {}
        self.current_step = None
        self.step_start_time = None
    
    def start_step(self, step_name: str):
        """단계 시작"""
        if self.current_step:
            self.end_step()
        
        self.current_step = step_name
        self.step_start_time = time.time()
        self.logger.info(f"🚀 Starting step: {step_name}")
    
    def end_step(self):
        """현재 단계 종료"""
        if self.current_step and self.step_start_time:
            duration = time.time() - self.step_start_time
            self.steps[self.current_step] = duration
            self.logger.info(f"✅ Completed step: {self.current_step} - Duration: {duration:.2f}s")
            self.current_step = None
            self.step_start_time = None
    
    def log_summary(self):
        """전체 실행 시간 요약"""
        if self.current_step:
            self.end_step()
        
        total_time = sum(self.steps.values())
        self.logger.info("\n" + "="*50)
        self.logger.info("⏱️  EXECUTION SUMMARY")
        self.logger.info("="*50)
        
        for step, duration in self.steps.items():
            percentage = (duration / total_time * 100) if total_time > 0 else 0
            self.logger.info(f"  {step}: {duration:.2f}s ({percentage:.1f}%)")
        
        self.logger.info(f"  TOTAL: {total_time:.2f}s")
        self.logger.info("="*50)

def create_spark_session(app_name: str, aqe_enable: bool = True) -> SparkSession:
    builder = SparkSession.builder \
        .appName(app_name) \
        .master("local[2]") \
        .config("spark.driver.memory", "4g")
        # .config("spark.driver.cores", 2) \ # Can't set in standalone mode
        # .config("spark.executor.memory", "2g") \
        # .config("spark.executor.instances", 2) \
        # .config("spark.executor.cores", 1)

    if aqe_enable:
        builder = builder \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    return builder.getOrCreate()