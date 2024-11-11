from constructs import Construct
from aws_cdk import (
    Duration,
    Aws,
    RemovalPolicy,
    Stack,
    aws_apigateway as apigateway,
    aws_lambda as lambda_,
    aws_iam as iam,
    CfnOutput
)


class StonksApiStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        requests_layer = lambda_.LayerVersion(
            self, "RequestsLayer",
            code=lambda_.Code.from_asset("src/lambda/layers/requests-layer.zip"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_11],
            description="Layer containing requests library"
        )

        lambda_role = iam.Role(
            self, "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ]
        )

        lambda_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "ssm:GetParameter",
                "ssm:GetParameters"
            ],
            resources=[
                f"arn:aws:ssm:{Stack.of(self).region}:{Stack.of(self).account}:parameter/financial-modeling-prep/*"
            ]
        ))

        api_role = iam.Role(
            self, "ApiGatewayRole",
            assumed_by=iam.AccountPrincipal(Aws.ACCOUNT_ID),
            role_name="sf-stonks-api-role"
        )

        lambda_fn = lambda_.Function(
            self, "ExternalFunctionLambda",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="index.handler",
            code=lambda_.Code.from_asset("src/lambda/snowflake_external_function"),
            role=lambda_role,
            timeout=Duration.seconds(30),
            layers=[requests_layer]
        )

        api = apigateway.RestApi(
            self, "ExternalFunctionApi",
            description="Snowflake external functions Gateway",
            endpoint_types=[apigateway.EndpointType.REGIONAL],
            deploy=True,
            deploy_options=apigateway.StageOptions(
                stage_name="sf-dev"
            ),
            policy=iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        principals=[iam.ArnPrincipal(
                            # f"arn:aws:sts::{Stack.of(self).account}:assumed-role/ext-func-api-role/snowflake"
                            api_role.role_arn
                        )],
                        actions=["execute-api:Invoke"],
                        resources=["execute-api:/*"]
                    )
                ]
            )
        )

        api.apply_removal_policy(RemovalPolicy.DESTROY)

        echo_resource = api.root.add_resource("echo")
        echo_resource.add_method(
            "POST",
            apigateway.LambdaIntegration(
                lambda_fn,
                proxy=True
            ),
            authorization_type=apigateway.AuthorizationType.IAM
        )

        lambda_fn.add_permission(
            "ApiGatewayInvoke",
            principal=iam.ServicePrincipal("apigateway.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=f"arn:aws:execute-api:{Stack.of(self).region}:{Stack.of(self).account}:{api.rest_api_id}/*/*/*"
        )

        CfnOutput(
            self, "ResourceInvocationUrl",
            value=f"https://{api.rest_api_id}.execute-api.{Stack.of(self).region}.amazonaws.com/sf-dev/echo"
        )

        CfnOutput(
            self, "AwsRoleArn",
            value=api_role.role_arn
        )