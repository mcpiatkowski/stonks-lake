from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_s3_notifications,
    aws_iam as iam,
    Duration,
    CfnOutput,
)
from constructs import Construct


class StonksLakeStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket = s3.Bucket(self, id="StonksBucket")

        function = lambda_.Function(
            self,
            id="StonksRawFunction",
            handler="index.handler",
            memory_size=256,
            timeout=Duration.seconds(15),
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.Code.from_asset("src/lambda/stonks_raw_function"),
        )

        function.add_layers(
            lambda_.LayerVersion.from_layer_version_arn(
                self,
                id="PandasLayer",
                layer_version_arn="arn:aws:lambda:eu-central-1:336392948345:layer:AWSSDKPandas-Python312:13",
            )
        )

        bucket.grant_read_write(function)

        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            aws_s3_notifications.LambdaDestination(function),
            s3.NotificationKeyFilter(prefix="raw/"),
        )

        snowflake_policy = iam.Policy(
            self,
            "SnowflakeS3Policy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["s3:GetObject", "s3:GetObjectVersion"],
                    resources=[f"{bucket.bucket_arn}/partitioned/*"],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["s3:ListBucket", "s3:GetBucketLocation"],
                    resources=[bucket.bucket_arn],
                    conditions={"StringLike": {"s3:prefix": ["partitioned/*"]}},
                ),
            ],
        )

        snowflake_role = iam.Role(
            self,
            "SnowflakeS3Role",
            # assumed_by=iam.AccountPrincipal(self.account),  # Temporarily use the own account
            # external_ids=["0000"],  # Placeholder external ID
            assumed_by=iam.ArnPrincipal("arn:aws:iam::697266982738:user/vldc0000-s"),
            external_ids=["JL27296_SFCRole=2_6ouv5pdETSL2JzdmNpRJHijTgW0=0"],
        )

        snowflake_role.attach_inline_policy(snowflake_policy)

        CfnOutput(
            self, "SnowflakeRoleArn", value=snowflake_role.role_arn, description="ARN of the IAM role for Snowflake"
        )

        CfnOutput(self, "BucketArn", value=bucket.bucket_arn, description="ARN of the S3 bucket")
