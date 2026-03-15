"""Generic polling utilities for async operations."""
import logging
import time
from enum import Enum


class OperationType(Enum):
    """Types of operations that can be polled."""
    CREATE = "creation"
    DELETE = "deletion"


def poll_assignment_status(
    sso_client,
    instance_arn: str,
    account_assignments: list[dict],
    operation_type: OperationType,
) -> tuple[str, list[dict]]:
    """Poll assignment statuses and return aggregated result.
    
    Args:
        sso_client: Boto3 SSO admin client
        instance_arn: IAM Identity Center instance ARN
        account_assignments: List of assignment objects with RequestId
        operation_type: Whether this is a CREATE or DELETE operation
    
    Returns:
        Tuple of (overall_status, current_assignments) where:
        - overall_status: "SUCCEEDED", "IN_PROGRESS", or "FAILED"
        - current_assignments: Updated list of assignment status objects
    """
    time.sleep(2)  # Avoid API throttling
    
    # Map operation type to API call
    describe_fn = {
        OperationType.CREATE: sso_client.describe_account_assignment_creation_status,
        OperationType.DELETE: sso_client.describe_account_assignment_deletion_status,
    }[operation_type]
    
    # Map operation type to response key and request parameter
    config = {
        OperationType.CREATE: {
            "status_key": "AccountAssignmentCreationStatus",
            "request_param": "AccountAssignmentCreationRequestId",
        },
        OperationType.DELETE: {
            "status_key": "AccountAssignmentDeletionStatus",
            "request_param": "AccountAssignmentDeletionRequestId",
        },
    }[operation_type]
    
    overall_status = "SUCCEEDED"
    current_assignments = []
    
    for assignment in account_assignments:
        status = describe_fn(
            InstanceArn=instance_arn,
            **{config["request_param"]: assignment["RequestId"]},
        )[config["status_key"]]

        logging.error(f"status: {status}")
        
        current_assignments.append(status)
        
        if status["Status"] == "FAILED":
            return "FAILED", current_assignments
        elif status["Status"] != "SUCCEEDED":
            overall_status = "IN_PROGRESS"
    
    return overall_status, current_assignments
