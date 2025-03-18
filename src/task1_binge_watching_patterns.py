from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round as spark_round

def initialize_spark(app_name="Task1_Binge_Watching_Patterns"):
    """
    Initialize and return a SparkSession.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    """
    Load the movie ratings data from a CSV file into a Spark DataFrame.
    """
    schema = """
        UserID INT, MovieID INT, MovieTitle STRING, Genre STRING, Rating FLOAT, ReviewCount INT, 
        WatchedYear INT, UserLocation STRING, AgeGroup STRING, StreamingPlatform STRING, 
        WatchTime INT, IsBingeWatched BOOLEAN, SubscriptionStatus STRING
    """
    
    # Load CSV with schema enforcement
    df = spark.read.csv(file_path, header=True, schema=schema)

    # Debug: Show a few rows to confirm data is loaded
    print("Loaded Data Preview:")
    df.show(5)
    
    return df

def detect_binge_watching_patterns(df):
    """
    Identify the percentage of users in each age group who binge-watch movies.
    """

    # Step 1: Filter users who have `IsBingeWatched = True`
    binge_watchers = df.filter(col("IsBingeWatched") == True)

    # Debug: Show filtered dataset
    print("Filtered Binge-Watchers:")
    binge_watchers.show(5)

    if binge_watchers.count() == 0:
        print("No binge-watching data found!")
        return None

    # Step 2: Count binge-watchers per age group
    binge_watch_count = binge_watchers.groupBy("AgeGroup").agg(count("*").alias("BingeWatchers"))

    # Step 3: Count total users per age group
    total_users_count = df.groupBy("AgeGroup").agg(count("*").alias("TotalUsers"))

    # Step 4: Calculate binge-watching percentage
    result_df = binge_watch_count.join(total_users_count, "AgeGroup", "inner") \
        .withColumn("Percentage", spark_round((col("BingeWatchers") / col("TotalUsers")) * 100, 2)) \
        .orderBy(col("Percentage").desc())

    # Debug: Show final result
    print("Final Computed Results:")
    result_df.show()

    return result_df

def write_output(result_df, output_path):
    """
    Write the result DataFrame to a CSV file.
    """
    if result_df is None:
        print("No valid results to write.")
        return

    # Ensure directory exists before writing
    import os
    output_dir = os.path.dirname(output_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Save CSV properly
    result_df.coalesce(1).write.option("header", True).csv(output_path, mode="overwrite")

    print(f"Output successfully written to {output_path}")

def main():
    """
    Main function to execute Task 1.
    """
    spark = initialize_spark()

    input_file = "/input/movie_ratings_data.csv"
    output_file = "./outputs/binge_watching_patterns.csv"

    df = load_data(spark, input_file)
    result_df = detect_binge_watching_patterns(df)  # Call function here
    write_output(result_df, output_file)

    spark.stop()

if __name__ == "__main__":
    main()
