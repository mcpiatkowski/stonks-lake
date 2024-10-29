from constructs import Construct
from aws_cdk import (
    Stack,
    aws_apigateway as apigateway,
    aws_lambda as lambda_,
    aws_iam as iam,
    CfnOutput
)

lambda_code: str = """
import json

def handler(event, context):
    status_code = 200
    array_of_rows_to_return = []

    try:
        event_body = event["body"]
        payload = json.loads(event_body)
        rows = payload["data"]

        for row in rows:
            row_number = row[0]
            input_value_1 = row[1]
            input_value_2 = row[2]

            output_value = ["Echoing inputs:", input_value_1, input_value_2]
            row_to_return = [row_number, output_value]
            array_of_rows_to_return.append(row_to_return)

        json_compatible_string_to_return = json.dumps({"data" : array_of_rows_to_return})

    except Exception as err:
        status_code = 400
        json_compatible_string_to_return = event_body

    return {
        'statusCode': status_code,
        'body': json_compatible_string_to_return
    }
"""


class StonksApiStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        lambda_role = iam.Role(
            self, "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ]
        )

        api_role = iam.Role(
            self, "ApiGatewayRole",
            assumed_by=iam.CompositePrincipal(
                iam.AccountRootPrincipal()
            )
        )

        lambda_fn = lambda_.Function(
            self, "ExternalFunctionLambda",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="index.handler",
            code=lambda_.Code.from_inline(lambda_code),
            role=lambda_role,
            timeout=Duration.seconds(10)
        )

        api = apigateway.RestApi(
            self, "ExternalFunctionApi",
            description="Snowflake external functions Gateway",
            endpoint_types=[apigateway.EndpointType.REGIONAL],
            policy=iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        principals=[iam.ArnPrincipal(
                            f"arn:aws:sts::{Stack.of(self).account}:assumed-role/ext-func-api-role/snowflake")],
                        actions=["execute-api:Invoke"],
                        resources=["execute-api:/*"]
                    )
                ]
            )
        )

        echo_resource = api.root.add_resource("echo")
        echo_resource.add_method(
            "POST",
            apigateway.LambdaIntegration(
                lambda_fn,
                proxy=True
            ),
            authorization_type=apigateway.AuthorizationType.IAM
        )

        deployment = apigateway.Deployment(
            self, "Deployment",
            api=api
        )

        stage = apigateway.Stage(
            self, "Stage",
            deployment=deployment
        )

        api.deployment_stage = stage

        lambda_fn.add_permission(
            "ApiGatewayInvoke",
            principal=iam.ServicePrincipal("apigateway.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=f"arn:aws:execute-api:{Stack.of(self).region}:{Stack.of(self).account}:{api.rest_api_id}/*/*/*"
        )

        CfnOutput(
            self, "ResourceInvocationUrl",
            value=f"https://{api.rest_api_id}.execute-api.{Stack.of(self).region}.amazonaws.com/ext-func-stage/echo"
        )

        CfnOutput(
            self, "AwsRoleArn",
            value=api_role.role_arn
        )