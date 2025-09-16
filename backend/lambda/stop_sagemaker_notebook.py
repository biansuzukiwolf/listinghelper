import boto3

def lambda_handler(event, context):
    # IAM Role ARN in Account B with permissions to stop the SageMaker notebook instance
    target_account_role_arn = 'arn:aws:iam::ACCOUNT_B_ID:role/RoleNameInAccountB'

    # SageMaker notebook instance name to stop
    notebook_instance_name = 'YOUR_NOTEBOOK_INSTANCE_NAME'

    # Assume the IAM Role in Account B
    sts_client = boto3.client('sts')
    assumed_role = sts_client.assume_role(
        RoleArn=target_account_role_arn,
        RoleSessionName='AssumedRoleSession'
    )

    # Use the temporary credentials to create a SageMaker client in Account B
    sagemaker_client = boto3.client(
        'sagemaker',
        aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
        aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
        aws_session_token=assumed_role['Credentials']['SessionToken']
    )

    try:
        # Stop the SageMaker notebook instance in Account B
        sagemaker_client.stop_notebook_instance(NotebookInstanceName=notebook_instance_name)
        return {
            'statusCode': 200,
            'body': f'Successfully stopped {notebook_instance_name} in Account B.'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
