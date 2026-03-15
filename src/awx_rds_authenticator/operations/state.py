"""SSM Parameter Store state management."""
import json
import logging
from botocore.exceptions import ClientError
from cloudformation_cli_python_lib import exceptions


def store_resource_state(
    ssm_client,
    username: str,
    permission_set_arn: str,
    account_assignments: list[dict],
) -> None:
    """Persist resource state to SSM Parameter Store."""
    ssm_client.put_parameter(
        Name=f"/awx/rds/authenticator/{username}",
        Value=json.dumps({
            "PermissionSetArn": permission_set_arn,
            "AccountAssignments": account_assignments,
        }),
        Type="String",
        Overwrite=True,
    )


def load_resource_state(ssm_client, username: str) -> dict:
    """Load resource state from SSM Parameter Store."""
    response = ssm_client.get_parameter(
        Name=f"/awx/rds/authenticator/{username}",
    )
    return json.loads(response["Parameter"]["Value"])


def delete_resource_state(ssm_client, username: str) -> None:
    """Remove resource state from SSM Parameter Store."""
    ssm_client.delete_parameter(Name=f"/awx/rds/authenticator/{username}")