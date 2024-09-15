import aws_cdk as cdk
import os

from stonks_lake.stonks_lake_stack import StonksLakeStack

app = cdk.App()
StonksLakeStack(
    app,
    construct_id="StonksLakeStack",
    env=cdk.Environment(account=os.getenv("CDK_DEFAULT_ACCOUNT"), region=os.getenv("CDK_DEFAULT_REGION")),
)

app.synth()
