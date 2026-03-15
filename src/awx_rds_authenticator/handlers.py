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
from .models import (
    ResourceHandlerRequest,
    ResourceModel,
)
from .utils.polling import poll_assignment_status, OperationType
from .utils.builders import build_instance_arn
from .operations.permission_set import create_permission_set, delete_permission_set
from .operations.assignment import create_assignments, delete_assignments
from .operations.state import store_resource_state, delete_resource_state

# Use this logger to forward log messages to CloudWatch Logs.
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)
TYPE_NAME = "AWX::RDS::Authenticator"

resource = Resource(TYPE_NAME, ResourceModel)
test_entrypoint = resource.test_entrypoint


def _apply_defaults(model: ResourceModel) -> None:
    """Apply default values to target properties not set by the caller."""
    for target in model.Targets:
        target.DbInstanceResourceId = "*" if target.DbInstanceResourceId is None else target.DbInstanceResourceId


@resource.handler(Action.CREATE)
def create_handler(
    session: Optional[SessionProxy],
    request: ResourceHandlerRequest,
    callback_context: MutableMapping[str, Any],
) -> ProgressEvent:
    model = request.desiredResourceState
    _apply_defaults(model)

    sso_client = session.client("sso-admin", region_name="us-east-1")
    identity_store_client = session.client("identitystore", region_name="us-east-1")
    ssm_client = session.client("ssm", region_name="us-east-1")

    overall_status = callback_context.get("overall_status")
    account_assignments = callback_context.get("account_assignments", [])
    permission_set_arn = callback_context.get("permission_set_arn", None)
    instance_arn = build_instance_arn(model.IamIdentityCenterId)

    if not (overall_status or account_assignments):
        permission_set_arn = create_permission_set(
            model, sso_client
        )

        try:
            account_assignments = create_assignments(
                sso_client,
                identity_store_client,
                instance_arn,
                permission_set_arn,
                model.Username,
                {target.AccountId for target in model.Targets},
            )
            return ProgressEvent(
                message=f"Creating RDS access for user {model.Username}",
                status=OperationStatus.IN_PROGRESS,
                resourceModel=model,
                callbackContext={
                    "overall_status": "IN_PROGRESS",
                    "account_assignments": account_assignments,
                    "permission_set_arn": permission_set_arn,
                },
            )
        except ClientError as error:
            delete_permission_set(sso_client, instance_arn, permission_set_arn)
            raise exceptions.InternalFailure(
                f"Failed to create permission set or assignments with error {error}"
            )

    # Phase 2: All assignments succeeded — store state and finish
    if overall_status == "SUCCEEDED":
        store_resource_state(
            ssm_client, model.Username, permission_set_arn, account_assignments
        )
        return ProgressEvent(
            message=f"Created RDS access for user {model.Username}",
            status=OperationStatus.SUCCESS,
            resourceModel=model,
        )

    # Phase 3: Still in progress — poll assignment statuses
    overall_status, current_assignments = poll_assignment_status(
        sso_client, instance_arn, account_assignments, OperationType.CREATE,
    )
    return ProgressEvent(
        message=f"Creating RDS access for user {model.Username}",
        status=OperationStatus.IN_PROGRESS,
        resourceModel=model,
        callbackContext={
            "overall_status": overall_status,
            "account_assignments": current_assignments,
            "permission_set_arn": permission_set_arn,
        },
    )


@resource.handler(Action.DELETE)
def delete_handler(
    session: Optional[SessionProxy],
    request: ResourceHandlerRequest,
    callback_context: MutableMapping[str, Any],
) -> ProgressEvent:
    model = request.desiredResourceState
    sso_client = session.client("sso-admin", region_name="us-east-1")
    ssm_client = session.client("ssm", region_name="us-east-1")
    identity_store_client = session.client("identitystore", region_name="us-east-1")

    overall_status = callback_context.get("overall_status")
    account_assignments = callback_context.get("account_assignments", [])
    permission_set_arn = callback_context.get("permission_set_arn")
    instance_arn = build_instance_arn(model.IamIdentityCenterId)

    # Phase 1: Load state and initiate account assignment deletions
    if not (overall_status or account_assignments):
        permission_set_arn, account_assignments = delete_assignments(
            sso_client,
            ssm_client,
            identity_store_client,
            model.Username,
            instance_arn,
        )
        return ProgressEvent(
            message=f"Deleting RDS access for user {model.Username}",
            status=OperationStatus.IN_PROGRESS,
            resourceModel=model,
            callbackContext={
                "overall_status": "IN_PROGRESS",
                "account_assignments": account_assignments,
                "permission_set_arn": permission_set_arn,
            },
        )

    # Phase 2: All deletions succeeded — clean up permission set and SSM state
    if overall_status == "SUCCEEDED":
        delete_permission_set(sso_client, instance_arn, permission_set_arn)
        delete_resource_state(ssm_client, model.Username)
        return ProgressEvent(
            message=f"Deleted RDS access for user {model.Username}",
            status=OperationStatus.SUCCESS,
            resourceModel=model,
        )

    # Phase 3: Still in progress — poll deletion statuses
    overall_status, current_assignments = poll_assignment_status(
        sso_client, instance_arn, account_assignments, OperationType.DELETE,
    )
    return ProgressEvent(
        message=f"Deleting RDS access for user {model.Username}",
        status=OperationStatus.IN_PROGRESS,
        resourceModel=model,
        callbackContext={
            "overall_status": overall_status,
            "account_assignments": current_assignments,
            "permission_set_arn": permission_set_arn,
        },
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
        ssoClient.describe_permission_set(
            InstanceArn=f"arn:aws:sso:::instance/{model.IamIdentityCenterId}",
            PermissionSetArn=model.PermissionSetArn,
        )
    except ClientError as error:
        raise exceptions.InternalFailure(
            f"Failed to read permission set with error {error}"
        )

    return ProgressEvent(
        message="Successfully read permission set for RDS access",
        status=OperationStatus.SUCCESS,
        resourceModel=model,
    )
