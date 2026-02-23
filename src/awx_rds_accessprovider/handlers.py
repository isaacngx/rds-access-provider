import json
import logging
from typing import Any, MutableMapping, Optional
from cloudformation_cli_python_lib import (
    Action,
    OperationStatus,
    ProgressEvent,
    Resource,
    SessionProxy,
    exceptions,
)

from botocore.exceptions import ClientError

from .models import ResourceHandlerRequest, ResourceModel

# Use this logger to forward log messages to CloudWatch Logs.
LOG = logging.getLogger(__name__)
TYPE_NAME = "AWX::RDS::AccessProvider"

resource = Resource(TYPE_NAME, ResourceModel)
test_entrypoint = resource.test_entrypoint

# Temporary hardcoded values.
SSO_INSTANCE_ARN = 'arn:aws:sso:::instance/ssoins-7223b18c2c5ea126'
IDENTITY_STORE_ID = 'd-9067ae954e'

@resource.handler(Action.CREATE)
def create_handler(
    session: Optional[SessionProxy],
    request: ResourceHandlerRequest,
    callback_context: MutableMapping[str, Any],
) -> ProgressEvent:
    model = request.desiredResourceState
    ssoClient = session.client("sso-admin", region_name="us-east-1")
    identityStoreClient = session.client("identitystore", region_name="us-east-1")

    try:
        ssoResponse = ssoClient.create_permission_set(
            Name=f"DB_Access_For_{model.Username}",
            InstanceArn=SSO_INSTANCE_ARN
        )
        ssoClient.put_inline_policy_to_permission_set(
            InstanceArn=SSO_INSTANCE_ARN,
            PermissionSetArn=ssoResponse['PermissionSet']['PermissionSetArn'],
            InlinePolicy=json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "rds-db:connect"
                        ],
                        "Resource": [
                            f"arn:aws:rds-db:ap-southeast-1:891376986941:dbuser:*/{model.Username}"
                        ]
                    }
                ]
            })
        )
        identityStoreResponse = identityStoreClient.get_user_id(
            IdentityStoreId=IDENTITY_STORE_ID,
            AlternateIdentifier={
                'UniqueAttribute': {
                    'AttributePath': 'userName',
                    'AttributeValue': model.Username
                }
            }
        )
        ssoClient.create_account_assignment(
            InstanceArn=SSO_INSTANCE_ARN,
            PermissionSetArn=ssoResponse['PermissionSet']['PermissionSetArn'],
            PrincipalType='USER',
            PrincipalId=identityStoreResponse['UserId'],
            TargetType='AWS_ACCOUNT',
            TargetId='891376986941'
        )
    except ClientError as error:
        raise exceptions.InternalFailure(f"Failed to create permission set with error {error}")
    
    model.PermissionSetArn = ssoResponse['PermissionSet']['PermissionSetArn']

    return ProgressEvent(
        message="Successfully set up RDS access.",
        status=OperationStatus.SUCCESS,
        resourceModel=model,
    )

# TRADE-OFF
# We either loop through all permission sets to find the one with the correct name and delete it.
# Or the ARN of the permission set needs to be part of the model's indentifier.
@resource.handler(Action.DELETE)
def delete_handler(
    session: Optional[SessionProxy],
    request: ResourceHandlerRequest,
    callback_context: MutableMapping[str, Any],
) -> ProgressEvent:
    model = request.desiredResourceState
    ssoClient = session.client("sso-admin", region_name="us-east-1")
    try:
        ssoClient.delete_permission_set(InstanceArn=SSO_INSTANCE_ARN, PermissionSetArn=model.PermissionSetArn)
    except ClientError as error:
        raise exceptions.InternalFailure(f"Failed to delete permission set with error {error}")
    return ProgressEvent(
        message="Successfully deleted permission set for RDS access",
        status=OperationStatus.SUCCESS,
    )


@resource.handler(Action.READ)
def read_handler(
    session: Optional[SessionProxy],
    request: ResourceHandlerRequest,
    callback_context: MutableMapping[str, Any],
) -> ProgressEvent:
    model = request.desiredResourceState
    ssoClient = session.client("sso-admin", region_name="us-east-1")
    try:
        response = ssoClient.describe_permission_set(InstanceArn=SSO_INSTANCE_ARN, PermissionSetArn=model.PermissionSetArn)
    except ClientError as error:
        raise exceptions.InternalFailure(f"Failed to read permission set with error {error}")

    model.PermissionSetArn = response['PermissionSet']['PermissionSetArn']

    return ProgressEvent(
        message="Successfully read permission set for RDS access",
        status=OperationStatus.SUCCESS,
        resourceModel=model,
    )
