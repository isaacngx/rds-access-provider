"""Utility functions for building ARNs and policies."""
import json


def build_instance_arn(identity_center_id: str) -> str:
    """Build the IAM Identity Center instance ARN."""
    return f"arn:aws:sso:::instance/{identity_center_id}"


def build_rds_connect_policy(username: str, targets: list) -> str:
    """Build the IAM inline policy granting rds-db:connect for all targets."""
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["rds-db:connect"],
            "Resource": [
                f"arn:aws:rds-db:{target.Region}:{target.AccountId}:"
                f"dbuser:{target.DbInstanceResourceId}/{username}"
                for target in targets
            ],
        }],
    })
