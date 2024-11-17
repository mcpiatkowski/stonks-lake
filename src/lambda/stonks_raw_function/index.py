import pandas as pd
import boto3
from io import StringIO, BytesIO
import uuid
from typing import Dict, Any, Tuple


def extract_s3_event_details(event: Dict[str, Any]) -> Tuple[str, str]:
    """Extract bucket name and key from S3 event."""
    s3_event = event["Records"][0]["s3"]

    return s3_event["bucket"]["name"], s3_event["object"]["key"]


def read_csv_from_s3(s3_client: boto3.client, bucket: str, key: str) -> pd.DataFrame:
    """Read CSV file from S3 and return as DataFrame."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    file_content = response["Body"].read().decode("utf-8")

    return pd.read_csv(StringIO(file_content))


def preprocess_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply common preprocessing steps to the DataFrame."""
    df["Currency (Currency conversion fee)"] = df["Currency (Currency conversion fee)"].astype(str).replace("nan", None)
    df["Time"] = pd.to_datetime(df["Time"], format="mixed").dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    df["year_month"] = pd.to_datetime(df["Time"]).dt.strftime("%Y%m")
    df["record_dt"] = pd.to_datetime(df["Time"])
    df["record_date"] = df["record_dt"].dt.date

    return df


def extract_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Extract and transform orders data from DataFrame."""
    column_mapping = {
        "No. of shares": "number_of_shares",
        "Price / share": "price_per_share",
        "Currency (Price / share)": "price_per_share_currency",
        "Exchange rate": "exchange_rate",
        "Currency (Result)": "result_currency",
        "Currency (Total)": "total_currency",
        "Currency conversion fee": "currency_conversion_fee",
        "Currency (Currency conversion fee)": "currency_conversion_fee_currency",
    }

    orders = (
        df[df["Action"].isin(["Market buy", "Market sell"])][
            [
                "Action",
                "record_date",
                "record_dt",
                "year_month",
                "Ticker",
                "Name",
                "No. of shares",
                "Price / share",
                "Currency (Price / share)",
                "Exchange rate",
                "Result",
                "Currency (Result)",
                "Total",
                "Currency (Total)",
                "Currency conversion fee",
                "Currency (Currency conversion fee)",
                "ID",
                "ISIN",
            ]
        ]
        .rename(columns=column_mapping)
        .reset_index(drop=True)
    )

    orders.columns = orders.columns.str.upper()

    return orders


def extract_dividends(df: pd.DataFrame) -> pd.DataFrame:
    """Extract and transform dividends data from DataFrame."""

    column_mapping = {
        "No. of shares": "number_of_shares",
        "Price / share": "price_per_share",
        "Currency (Price / share)": "price_per_share_currency",
        "Currency (Total)": "total_currency",
        "Withholding tax": "withholding_tax",
        "Currency (Withholding tax)": "withholding_tax_currency",
    }

    dividends = (
        df[df["Action"].str.contains("dividend", case=False)][
            [
                "Action",
                "Time",
                "year_month",
                "Ticker",
                "Name",
                "No. of shares",
                "Price / share",
                "Currency (Price / share)",
                "Total",
                "Currency (Total)",
                "Withholding tax",
                "Currency (Withholding tax)",
                "ISIN",
            ]
        ]
        .rename(columns=column_mapping)
        .reset_index(drop=True)
    )

    dividends.columns = dividends.columns.str.upper()

    return dividends


def write_parquet_to_s3(
    df: pd.DataFrame, s3_client: boto3.client, bucket: str, base_path: str, year_month: str
) -> None:
    """Write DataFrame to S3 as a parquet file with partitioning."""
    buffer = BytesIO()
    output_key = f"{base_path}/{year_month}/{uuid.uuid4().hex}.parquet"
    df.to_parquet(buffer, index=False)
    s3_client.put_object(Bucket=bucket, Key=output_key, Body=buffer.getvalue())


def save_monthly_partitions(df: pd.DataFrame, s3_client: boto3.client, bucket: str, base_path: str) -> None:
    """Process DataFrame and write partitioned parquet files to S3."""
    for year_month, group in df.groupby("YEAR_MONTH"):
        write_parquet_to_s3(group, s3_client, bucket, base_path, year_month)


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Main handler function for processing S3 events."""
    print(event)

    s3_client = boto3.client("s3")
    bucket, key = extract_s3_event_details(event)

    df: pd.DataFrame = preprocess_dataframe(read_csv_from_s3(s3_client, bucket, key))

    orders: pd.DataFrame = extract_orders(df)
    save_monthly_partitions(orders, s3_client, bucket, "partitioned/orders")

    dividends: pd.DataFrame = extract_dividends(df)
    save_monthly_partitions(dividends, s3_client, bucket, "partitioned/dividends")

    return {"status_code": 200, "body": event}
