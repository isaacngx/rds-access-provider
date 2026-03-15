"""Permission set lifecycle operations."""
from botocore.exceptions import ClientError
from cloudformation_cli_python_lib import exceptions
from ..utils.builders import build_instance_arn, build_rds_connect_policy


def create_permission_set(
    model,
    sso_client,
) -> str:
    """Create permission set, attach policy, and resolve user.

    Returns the permission set ARN.
    Cleans up the permission set on failure.
    """
    instance_arn = build_instance_arn(model.IamIdentityCenterId)
    
    permission_set = sso_client.create_permission_set(
        Name=f"DB_Access_For_{model.Username[:17]}",
        InstanceArn=instance_arn,
    )
    permission_set_arn = permission_set["PermissionSet"]["PermissionSetArn"]
    
    try:
        sso_client.put_inline_policy_to_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set_arn,
            InlinePolicy=build_rds_connect_policy(model.Username, model.Targets),
        )
    except ClientError as error:
        delete_permission_set(sso_client, instance_arn, permission_set_arn)
        raise exceptions.InternalFailure(f"Failed with error {error}")
    
    return permission_set_arn


def delete_permission_set(sso_client, instance_arn: str, permission_set_arn: str) -> None:
    """Delete a permission set."""
    sso_client.delete_permission_set(
        InstanceArn=instance_arn,
        PermissionSetArn=permission_set_arn,
    )