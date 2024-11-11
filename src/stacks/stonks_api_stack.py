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

lambda_code: str = """
import json
import requests
import boto3
from botocore.exceptions import ClientError

def get_secret():
    ssm = boto3.client('ssm')
    try:
        parameter = ssm.get_parameter(
            Name='/financial-modeling-prep/api-key',  # Parameter name in SSM
            WithDecryption=True
        )
        return parameter['Parameter']['Value']
    except ClientError as e:
        print(f"Error retrieving API key: {e}")
        raise

def handler(event, context):
    status_code = 200
    array_of_rows_to_return = []
    BASE_URL = "https://financialmodelingprep.com/api/v3/quote-short"

    try:
        # Get API key from Parameter Store
        API_KEY = get_secret()
        
        event_body = event["body"]
        payload = json.loads(event_body)
        rows = payload["data"]

        for row in rows:
            row_number = row[0]
            ticker_symbol = row[1]
            
            url = f"{BASE_URL}/{ticker_symbol}?apikey={API_KEY}"
            response = requests.get(url)
            
            if response.status_code == 200:
                stock_data = response.json()
                if stock_data:
                    current_price = stock_data[0]["price"]
                    output_value = [ticker_symbol, current_price]
                else:
                    output_value = [ticker_symbol, "No data found"]
            else:
                output_value = [ticker_symbol, f"API Error: {response.status_code}"]

            row_to_return = [row_number, output_value]
            array_of_rows_to_return.append(row_to_return)

        json_compatible_string_to_return = json.dumps({"data": array_of_rows_to_return})

    except ClientError as e:
        status_code = 500
        json_compatible_string_to_return = json.dumps({"error": f"Secret retrieval failed: {str(e)}"})
    except requests.RequestException as e:
        status_code = 500
        json_compatible_string_to_return = json.dumps({"error": f"API request failed: {str(e)}"})
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
            code=lambda_.Code.from_inline(lambda_code),
            role=lambda_role,
            timeout=Duration.seconds(10)
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