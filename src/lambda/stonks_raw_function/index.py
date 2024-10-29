import pandas as pd
import boto3
from io import StringIO, BytesIO
import uuid


def handler(event, context):

    print(event)

    s3_event = event['Records'][0]['s3']
    bucket = s3_event['bucket']['name']
    key = s3_event['object']['key']

    s3 = boto3.client('s3')

    response = s3.get_object(Bucket=bucket, Key=key)

    file_content = response['Body'].read().decode('utf-8')

    df = pd.read_csv(StringIO(file_content))
    df["year_month"] = pd.to_datetime(df["Time"], format="mixed").dt.strftime("%Y%m")

    dividends: pd.DataFrame = df[df["Action"].str.contains("dividend", case=False)][[
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
        "ISIN"
    ]].reset_index(drop=True)

    orders: pd.DataFrame = df[df["Action"].isin(["Market buy", "Market sell"])][[
        "Action",
        "Time",
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
        "ISIN"
    ]]

    for year_month, group in dividends.groupby("year_month"):
        buffer = BytesIO()
        output_key = f"partitioned/dividends/{year_month}/{uuid.uuid4().hex}.parquet"
        group.to_parquet(buffer, index=False)
        s3.put_object(Bucket=bucket, Key=output_key, Body=buffer.getvalue())

    return {
        "status_code": 200,
        "body": event
    }