"""Account assignment operations."""
from awx_rds_authenticator.operations.state import load_resource_state


def create_assignments(
    sso_client,
    identity_store_client,
    instance_arn: str,
    permission_set_arn: str,
    username: str,
    target_accounts: set[str],
) -> tuple[str, list[dict]]:
    """Resolve user and create account assignments for a permission set.
    
    Returns (user_id, list of account assignment creation statuses).
    """
    sso_instance = sso_client.describe_instance(InstanceArn=instance_arn)
    user_id = identity_store_client.get_user_id(
        IdentityStoreId=sso_instance["IdentityStoreId"],
        AlternateIdentifier={
            "UniqueAttribute": {
                "AttributePath": "Username",
                "AttributeValue": username,
            }
        },
    )["UserId"]
    
    account_assignments = [
        sso_client.create_account_assignment(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set_arn,
            PrincipalType="USER",
            PrincipalId=user_id,
            TargetType="AWS_ACCOUNT",
            TargetId=target_account,
        )["AccountAssignmentCreationStatus"]
        for target_account in target_accounts
    ]
    
    return account_assignments


def delete_assignments(
    sso_client,
    ssm_client,
    identity_store_client,
    username: str,
    instance_arn: str,
) -> list[dict]:
    """Delete account assignments for a permission set by retrieving info from SSM.
    
    Retrieves the stored resource state from SSM Parameter Store and deletes
    all account assignments for the user's permission set.
    
    Args:
        sso_client: SSO Admin client
        ssm_client: SSM client
        username: Username to look up the SSM parameter
        instance_arn: IAM Identity Center instance ARN
    
    Returns the list of account assignment deletion statuses.
    """
    load_resource_state(ssm_client, username)
    resource_state = load_resource_state(ssm_client, username)
    
    # Extract stored information
    permission_set_arn = resource_state["PermissionSetArn"]
    account_ids = [assignment["TargetId"] for assignment in resource_state["AccountAssignments"]]

    sso_instance = sso_client.describe_instance(InstanceArn=instance_arn)
    user_id = identity_store_client.get_user_id(
        IdentityStoreId=sso_instance["IdentityStoreId"],
        AlternateIdentifier={
            "UniqueAttribute": {
                "AttributePath": "Username",
                "AttributeValue": username,
            }
        },
    )["UserId"]

    return (
        permission_set_arn,
        [
            sso_client.delete_account_assignment(
                InstanceArn=instance_arn,
                PermissionSetArn=permission_set_arn,
                PrincipalType="USER",
                PrincipalId=user_id,
                TargetType="AWS_ACCOUNT",
                TargetId=account_id,
            )["AccountAssignmentDeletionStatus"]
            for account_id in account_ids
        ]
    )

