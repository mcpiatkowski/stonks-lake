from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_s3_notifications,
)
from constructs import Construct

inline_code: str = """
def handler(event, context):

    print(event)

    return {
        "status_code": 200,
        "body": event
    }
"""


class StonksLakeStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket = s3.Bucket(self, id="StonksBucket")

        function = lambda_.Function(
            self,
            id="StonksRawFunction",
            handler="index.handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.Code.from_inline(inline_code),
        )

        function.add_layers(
            lambda_.LayerVersion.from_layer_version_arn(
                self,
                id="PandasLayer",
                layer_version_arn="arn:aws:lambda:eu-central-1:336392948345:layer:AWSSDKPandas-Python312:13",
            )
        )

        bucket.add_event_notification(s3.EventType.OBJECT_CREATED, aws_s3_notifications.LambdaDestination(function))
