from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def initialize_spark(app_name="Task3_Trend_Analysis"):
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
    
    df = spark.read.csv(file_path, header=True, schema=schema)

    # Debugging: Show data preview
    print("Loaded Data Preview:")
    df.show(5)
    
    return df

def analyze_movie_watching_trends(df):
    """
    Analyze trends in movie watching over the years.
    """

    # Step 1: Group by `WatchedYear` and count the number of movies watched.
    trends_df = df.groupBy("WatchedYear").agg(count("*").alias("Movies Watched"))

    # Step 2: Order the results by `WatchedYear`
    trends_df = trends_df.orderBy(col("WatchedYear").asc())

    # Debugging: Show computed results
    print("Movie Watching Trends:")
    trends_df.show()

    return trends_df

def write_output(result_df, output_path):
    """
    Write the result DataFrame to a CSV file.
    """
    if result_df is None or result_df.count() == 0:
        print("No valid trends data to write.")
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
    Main function to execute Task 3.
    """
    spark = initialize_spark()

    input_file = "/input/movie_ratings_data.csv"
    output_file = "./outputs/movie_watching_trends.csv"

    df = load_data(spark, input_file)
    result_df = analyze_movie_watching_trends(df)  # Call function here
    write_output(result_df, output_file)

    spark.stop()

if __name__ == "__main__":
    main()
