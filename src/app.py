import aws_cdk as cdk
import os

from stacks.stonks_lake_stack import StonksLakeStack
from stacks.stonks_api_stack import StonksApiStack

env = cdk.Environment(account=os.getenv("CDK_DEFAULT_ACCOUNT"), region=os.getenv("CDK_DEFAULT_REGION"))

app = cdk.App()
StonksLakeStack(
    app,
    construct_id="StonksLakeStack",
    env=env,
)

StonksApiStack(
    app,
    construct_id="StonksApiStack",
    env=env,
)

app.synth()
