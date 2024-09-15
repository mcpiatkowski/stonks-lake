# stonks-lake

The goal of the application is to automate the creation of the portfolio dashboard.

The application is using the following services:
- S3
- Lambda
- Glue
- Athena
- Quicksight

# Notes

The `cdk.json` file has been created using the `cdk init` command and is copied into the repository.

To manage the application dependencies and the environment, Python PDM is used.
To run the app correctly, the `cdk.json` file is modified to accommodate PDM.

## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk destroy`     remove all the resources created by the stack
 * `cdk docs`        open CDK documentation

## Environment info

Account ID
```
aws sts get-caller-identity --query "Account" --output text
```

Region
```
aws configure get region
```