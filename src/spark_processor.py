from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os
import sys
import glob

class SparkHadoopProcessor:
    """Process data using Spark (Windows-compatible)"""
    
    def __init__(self):
        """Initialize Spark session with Windows settings"""
        # Set Windows-specific configurations
        os.environ['HADOOP_HOME'] = os.getcwd()
        os.environ['hadoop.home.dir'] = os.getcwd()
        
        self.spark = SparkSession.builder \
            .appName("ICU_Forecasting_Spark") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.driver.host", "127.0.0.1") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")
        print("✓ Spark session initialized")
    
    def read_data(self):
        """Read data from local filesystem"""
        print("\n📂 Looking for data to process...")
        
        # Option 1: Try hadoop_output folder
        hadoop_output_path = os.path.join(os.getcwd(), "data", "hadoop_output")
        
        if os.path.exists(hadoop_output_path):
            csv_files = glob.glob(os.path.join(hadoop_output_path, "*.csv"))
            
            if csv_files:
                print(f"✓ Found {len(csv_files)} files in hadoop_output")
                
                # Read first file to get schema
                first_df = self.spark.read.option("header", "true") \
                    .option("inferSchema", "true") \
                    .csv(csv_files[0])
                
                # Read all files if more than one
                if len(csv_files) > 1:
                    dfs = [first_df]
                    for csv_file in csv_files[1:]:
                        df_temp = self.spark.read.option("header", "true") \
                            .option("inferSchema", "true") \
                            .csv(csv_file)
                        dfs.append(df_temp)
                    
                    # Union all dataframes
                    df = dfs[0]
                    for df_temp in dfs[1:]:
                        df = df.union(df_temp)
                else:
                    df = first_df
                
                print(f"  Loaded {df.count()} records from Kafka output")
                return df, "kafka"
        
        # Option 2: Read processed ICU data
        processed_file = os.path.join(os.getcwd(), "processed_icu_data.csv")
        
        if os.path.exists(processed_file):
            print("✓ Found processed_icu_data.csv")
            df = self.spark.read.option("header", "true") \
                .option("inferSchema", "true") \
                .csv(processed_file)
            
            print(f"  Loaded {df.count()} records")
            return df, "processed"
        
        print("✗ No data found!")
        sys.exit(1)
    
    def process_data(self, df, source_type):
        """Process the dataframe"""
        print("\n⚡ Processing data with Spark...")
        
        # Show schema
        print("\nData Schema:")
        df.printSchema()
        
        # Show sample data
        print("\nSample Data:")
        df.show(5)
        
        # Basic statistics for icu_count
        print("\n📊 ICU Count Statistics:")
        df.select("icu_count").describe().show()
        
        # Convert icu_count to integer if needed
        df = df.withColumn("icu_count", col("icu_count").cast("integer"))
        
        # Day of week analysis
        if "day_of_week" in df.columns:
            dow_stats = df.groupBy("day_of_week").agg(
                avg("icu_count").alias("avg_occupancy"),
                max("icu_count").alias("max_occupancy"),
                min("icu_count").alias("min_occupancy")
            ).orderBy("day_of_week")
            
            print("\n📅 Day of Week Analysis:")
            dow_stats.show()
        
        # Hourly analysis
        if "hour" in df.columns:
            hour_stats = df.groupBy("hour").agg(
                avg("icu_count").alias("avg_occupancy"),
                count("*").alias("count")
            ).orderBy("hour")
            
            print("\n🕐 Hourly Analysis (Top 10 hours):")
            hour_stats.show(10)
        
        # Add computed features
        df = df.withColumn("is_high_occupancy", 
                          when(col("icu_count") > 30, 1).otherwise(0))
        
        df = df.withColumn("is_low_occupancy", 
                          when(col("icu_count") < 20, 1).otherwise(0))
        
        # Calculate occupancy levels
        high_pct = df.filter(col("is_high_occupancy") == 1).count() / df.count() * 100
        low_pct = df.filter(col("is_low_occupancy") == 1).count() / df.count() * 100
        
        print(f"\n📈 Occupancy Levels:")
        print(f"  High occupancy (>30): {high_pct:.1f}% of time")
        print(f"  Low occupancy (<20): {low_pct:.1f}% of time")
        
        return df
    
    def save_results(self, df):
        """Save processed results"""
        print("\n💾 Saving processed data...")
        
        # Convert to Pandas and save
        output_file = "spark_features.csv"
        
        # Select important columns
        columns_to_keep = []
        for col in ["datetime", "timestamp", "hour", "day_of_week", "icu_count", 
                   "is_high_occupancy", "is_low_occupancy"]:
            if col in df.columns:
                columns_to_keep.append(col)
        
        if columns_to_keep:
            pandas_df = df.select(columns_to_keep).toPandas()
            pandas_df.to_csv(output_file, index=False)
            print(f"✅ Saved features to {output_file}")
            
            # Show summary
            print(f"\n📋 Summary of saved data:")
            print(f"  Records: {len(pandas_df)}")
            print(f"  Columns: {', '.join(columns_to_keep)}")
            print(f"  Avg ICU count: {pandas_df['icu_count'].mean():.1f}")
    
    def run(self):
        """Main execution"""
        try:
            # Read data
            df, source_type = self.read_data()
            
            # Process data
            processed_df = self.process_data(df, source_type)
            
            # Save results
            self.save_results(processed_df)
            
            print("\n" + "="*60)
            print("✅ SPARK PROCESSING COMPLETE!")
            print("="*60)
            
        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.spark.stop()
            print("✓ Spark session closed")

if __name__ == "__main__":
    print("="*60)
    print("SPARK PROCESSING FOR ICU FORECASTING")
    print("="*60)
    
    processor = SparkHadoopProcessor()
    processor.run()