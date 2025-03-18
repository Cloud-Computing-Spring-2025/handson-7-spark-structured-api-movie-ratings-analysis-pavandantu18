from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit

def initialize_spark(app_name="Task2_Churn_Risk_Users"):
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

    # Debugging: Show some data
    print("Loaded Data Preview:")
    df.show(5)
    
    return df

def identify_churn_risk_users(df):
    """
    Identify users with canceled subscriptions and low watch time (<100 minutes).
    """
    
    # Step 1: Filter users with `SubscriptionStatus = 'Canceled'` and `WatchTime < 100`
    churn_risk_users = df.filter((col("SubscriptionStatus") == "Canceled") & (col("WatchTime") < 100))

    # Debugging: Show filtered users
    print("Filtered Churn Risk Users:")
    churn_risk_users.show(5)

    # Step 2: Count the total number of at-risk users
    churn_risk_count = churn_risk_users.agg(count("*").alias("Total Users")) \
        .withColumn("Churn Risk Users", lit("Users with low watch time & canceled subscriptions"))

    # Debugging: Show final results
    print("Final Computed Results:")
    churn_risk_count.show()

    return churn_risk_count

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
    Main function to execute Task 2.
    """
    spark = initialize_spark()

    input_file = "/input/movie_ratings_data.csv"
    output_file = "./outputs/churn_risk_users.csv"

    df = load_data(spark, input_file)
    result_df = identify_churn_risk_users(df)  # Call function here
    write_output(result_df, output_file)

    spark.stop()

if __name__ == "__main__":
    main()
