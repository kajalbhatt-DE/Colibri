from pyspark.sql import SparkSession
from pyspark.sql import functions as F


class Turbine:
    """
        A class to encapsulate the data pipeline for processing wind turbine data.
    This class reads data from the Bronze layer, detects outliers, handles missing values, writes the cleaned data to the Silver layer,
    summarizes the power output, detects anomalies, and stores results in the Gold layer.
    """

    def __init__(self, acc_name, container_name, access_key):
        """
        Initializes the WindTurbineDataPipeline object.

        :param acc_name: Name of the Azure storage account.
        :param container_name: Name of the container in the storage account.
        :param access_key: Access key for the storage account.
        """
        self.acc_name = acc_name
        self.container_name = container_name
        self.base_bronze_path = f"wasbs://{container_name}@{acc_name}.blob.core.windows.net/Bronze/"
        self.silver_layer_path = f"wasbs://{container_name}@{acc_name}.blob.core.windows.net/Silver"
        self.gold_layer_path = f"wasbs://{container_name}@{acc_name}.blob.core.windows.net/Gold"

        self.spark = self.init_spark_session()
        self.conf_accesskey(access_key)

    def init_spark_session(self, app_name="Data Load"):
        """
        Initializes a Spark session.

        :param app_name: Name of the Spark application.
        :return: A SparkSession object.
        """
        try:
            return SparkSession.builder.appName(app_name).getOrCreate()
        except Exception as e:
            print(f"Error in initializing Spark session: {e}")
            return None

    def conf_accesskey(self, access_key):
        """
        Configures the Azure storage account access key.

        :param access_key: The access key for the Azure storage account.
        """
        self.spark.conf.set(f"fs.azure.account.key.{self.acc_name}.blob.core.windows.net",
                            access_key)

    def read_bronze(self):
        """
        Reads CSV files from the Bronze layer.

        :return: A DataFrame containing the data read from the Bronze layer.
        """
        try:
            df = self.spark.read.csv(self.base_bronze_path + "/*.csv", header=True, inferSchema=True)
            return df
        except Exception as e:
            print(f"Error reading CSV from bronze layer: {e}")
            return None

    def imputed_missing_value(self, df):
        """
        Handles missing values in the DataFrame by filling null values with the median or a placeholder.

        :param df: Input DataFrame containing wind turbine data.
        :return: DataFrame with missing values handled.
        """
        try:
            # Fill numeric columns with median values
            numeric_columns = [col for col, dtype in df.dtypes if dtype in ['int', 'double']]
            for col in numeric_columns:
                median_value = df.approxQuantile(col, [0.5], 0.05)[0]
                df = df.fillna({col: median_value})

            # Fill string columns with a placeholder
            string_columns = [col for col, dtype in df.dtypes if dtype == 'string']
            df = df.fillna({col: 'Unknown' for col in string_columns})

            return df
        except Exception as e:
            print(f"Error in handling missing values: {e}")
            return None

    def outliers(self, df):
        """
        Detects outliers in the wind turbine data based on the interquartile range (IQR).

        :param df: Input DataFrame containing wind turbine data.
        :return: DataFrame with outlier detection columns added.
        """
        try:
            columns_to_check = ['wind_speed', 'power_output', 'wind_direction']
            quantiles = df.select(*[F.percentile_approx(c, [0.25, 0.75], 10000).alias(f"{c}_quantiles") for c in
                                    columns_to_check]).first()

            bounds = {}
            for col in columns_to_check:
                Q1, Q3 = quantiles[f"{col}_quantiles"]
                IQR = Q3 - Q1
                bounds[col] = {'lower': Q1 - 1.5 * IQR, 'upper': Q3 + 1.5 * IQR}

            for col in columns_to_check:
                df = df.withColumn(f"{col}_is_outlier",
                                   (F.col(col) < bounds[col]['lower']) | (F.col(col) > bounds[col]['upper']))

            df = df.withColumn("is_any_outlier",
                               F.array_contains(F.array(*[F.col(f"{col}_is_outlier") for col in columns_to_check]),
                                                True))
            return df
        except Exception as e:
            print(f"Error in detecting outliers: {e}")
            return None

    def anomalies(self, df):
        """
        Detects anomalies in power output based on deviation from the mean.

        Parameters:
        df (DataFrame): The cleaned DataFrame containing turbine data.

        Returns:
        DataFrame: A DataFrame containing detected anomalies across turbines.
        """
        try:
            # Calculate mean and standard deviation for each turbine
            stats_df = df.groupBy("turbine_id").agg(
                F.mean("power_output").alias("mean_output"),
                F.stddev("power_output").alias("std_dev")
            )

            # Join the stats back to the original DataFrame
            df_with_stats = df.join(stats_df, on="turbine_id")

            # Identify anomalies: outputs outside ±2 standard deviations from the mean
            anomalies = df_with_stats.filter(
                (df_with_stats["power_output"] < (df_with_stats["mean_output"] - 2 * df_with_stats["std_dev"])) |
                (df_with_stats["power_output"] > (df_with_stats["mean_output"] + 2 * df_with_stats["std_dev"]))
            )

            return anomalies.select(df.columns)  # Return only the original columns
        except Exception as e:
            print(f"Error detecting anomalies: {e}")
            return None

    def write_to_silver_layer(self, df):
        """
        Writes the cleaned DataFrame (with outliers detected) to the Silver layer in Parquet format.

        :param df: DataFrame to be written to the Silver layer.
        """
        try:
            df.write.mode("overwrite").parquet(self.silver_layer_path)
        except Exception as e:
            print(f"Error writing to silver layer: {e}")

    def summary_statistics(self, df):
        """
        Summarizes the power output (min, max, avg) for each turbine by date, and saves the summary to the Gold layer.

        :param df: DataFrame containing wind turbine data.
        """
        try:
            df_with_date = df.withColumn("date", F.to_date("timestamp"))
            summary_stats = df_with_date.groupBy("date", "turbine_id").agg(
                F.min("power_output").alias("min_power_output"),
                F.max("power_output").alias("max_power_output"),
                F.avg("power_output").alias("avg_power_output")
            ).orderBy("date")
            summary_stats.write.mode("append").parquet(self.gold_layer_path)
            # TODO  Write to datawarehouse
            """
            summary_stats.write \
            .format("com.databricks.spark.sqldw") \
            .option("url", "url") \
            .option("dbtable", "stats") \
            .option("forward_spark_azure_storage_credentials", "True") \
            .option("tempdir", temp_dir) \
            .mode("append") \
            .save()
            """
        except Exception as e:
            print(f"Error in summarizing power output: {e}")

    def process_turbine(self):
        """
        Runs the complete pipeline:
        - Reads data from the Bronze layer
        - Imputed missing values
        - Detects outliers and anomalies in the data
        - Writes cleaned data to the Silver layer
        - Summarizes power output and writes the summary to the Gold layer.
        """
        # Read from bronze layer
        df = self.read_bronze()
        if df is not None:
            df.show(5)

        # Imputed missing values
        df = self.imputed_missing_value(df)

        # Detects outliers & anomalies in the data and write to Silver Layer
        outliers_df = self.outliers(df)
        anomalies_df = self.anomalies(outliers_df)
        self.write_to_silver_layer(anomalies_df)

        # Summarize and write to Gold Layer
        self.summary_statistics(anomalies_df)


if __name__ == "__main__":
    acc_name = "collibri"  # Storage account name
    container_name = "codingc"
    access_key = " "  # Access key of Storage account

    processor = Turbine(acc_name, container_name, access_key)
    processor.process_turbine()
