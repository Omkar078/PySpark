import os
import sys

os.environ['JAVA_HOME'] = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.20.8-hotspot'
os.environ['PATH'] = os.environ['JAVA_HOME'] + r'\bin;' + os.environ['PATH']

# Fix for "Python worker exited unexpectedly (crashed)"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

try:
    from pyspark.sql import SparkSession
    print("PySpark imported successfully.")
except ImportError as e:
    print(f"Import Error: {e}")
    sys.exit(1)

try:
    print("Initializing SparkSession...")
    spark = SparkSession.builder \
        .appName("TestApp") \
        .master("local[*]") \
        .getOrCreate()
    
    print("SparkSession created successfully.")
    
    data = [(1,), (2,), (3,)]
    df = spark.createDataFrame(data, ["Numbers"])
    df.show()
    
    print("SUCCESS: PySpark is working properly!")
    spark.stop()
    
except Exception as e:
    print(f"Runtime Error: {e}")
    sys.exit(1)
