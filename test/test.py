import pytest
from pyspark.sql import Row
from pyspark.sql import SparkSession

# Import the Turbine class from your main script
from src.cleaning import Turbine


@pytest.fixture(scope="module")
def spark():
    """Fixture to initialize and provide a Spark session for the tests."""
    spark_session = SparkSession.builder.appName("Unit Test").getOrCreate()
    yield spark_session
    spark_session.stop()


@pytest.fixture(scope="module")
def test_df(spark):
    """Fixture to create and provide a sample DataFrame for testing."""
    data = [
        Row(turbine_id=1, wind_speed=10.0, power_output=100.0, wind_direction=180, timestamp="2023-09-28 00:00:00"),
        Row(turbine_id=1, wind_speed=None, power_output=120.0, wind_direction=185, timestamp="2023-09-28 01:00:00"),
        Row(turbine_id=2, wind_speed=15.0, power_output=130.0, wind_direction=190, timestamp="2023-09-28 02:00:00"),
        Row(turbine_id=2, wind_speed=20.0, power_output=140.0, wind_direction=195, timestamp="2023-09-28 03:00:00")
    ]
    return spark.createDataFrame(data)


def test_spark_session_initialization():
    """Test if Spark session is initialized properly."""
    process_turbine = Turbine("acc_name", "container_name", "access_key")
    assert process_turbine.spark is not None, "Spark session should be initialized properly"


def test_read_bronze(spark):
    """Test the reading from the Bronze layer."""
    process_turbine = Turbine("acc_name", "container_name", "access_key")
    df = process_turbine.read_bronze()
    assert df is not None, "Data should be read from the Bronze layer"
    assert len(df.columns) > 0, "DataFrame should have some columns"


def test_imputed_missing_value(test_df):
    """Test missing value imputation."""
    process_turbine = Turbine("acc_name", "container_name", "access_key")
    df_filled = process_turbine.imputed_missing_value(test_df)
    assert df_filled.filter(df_filled.wind_speed.isNull()).count() == 0, "Missing wind_speed values should be filled"


def test_outlier_detection(test_df):
    """Test outlier detection logic."""
    process_turbine = Turbine("acc_name", "container_name", "access_key")
    df_outliers = process_turbine.outliers(test_df)
    assert "wind_speed_is_outlier" in df_outliers.columns, "Outlier column should be added for wind_speed"
    assert "is_any_outlier" in df_outliers.columns, "is_any_outlier column should be added"


def test_anomaly_detection(test_df):
    """Test anomaly detection based on power output."""
    process_turbine = Turbine("acc_name", "container_name", "access_key")
    df_with_outliers = process_turbine.outliers(test_df)
    anomalies_df = process_turbine.anomalies(df_with_outliers)
    assert anomalies_df is not None, "Anomalies DataFrame should not be None"
    assert len(anomalies_df.columns) == len(test_df.columns), "Anomalies DataFrame should have the same columns as input"


def test_write_to_silver_layer(test_df):
    """Test writing cleaned DataFrame to the Silver layer."""
    process_turbine = Turbine("acc_name", "container_name", "access_key")
    try:
        process_turbine.write_to_silver_layer(test_df)
        assert True, "Writing to Silver layer should succeed"
    except Exception as e:
        pytest.fail(f"Writing to Silver layer failed: {e}")


def test_summary_statistics(test_df):
    """Test summarizing power output and writing to Gold layer."""
    process_turbine = Turbine("acc_name", "container_name", "access_key")
    try:
        process_turbine.summary_statistics(test_df)
        assert True, "Summarizing power output and saving to Gold layer should succeed"
    except Exception as e:
        pytest.fail(f"Summary statistics failed: {e}")
