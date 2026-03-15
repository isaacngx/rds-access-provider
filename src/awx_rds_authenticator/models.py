# DO NOT modify this file by hand, changes will be overwritten
from dataclasses import dataclass

from cloudformation_cli_python_lib.interface import (
    BaseModel,
    BaseResourceHandlerRequest,
)
from cloudformation_cli_python_lib.recast import recast_object
from cloudformation_cli_python_lib.utils import deserialize_list

import sys
from inspect import getmembers, isclass
from typing import (
    AbstractSet,
    Any,
    Generic,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Type,
    TypeVar,
)

T = TypeVar("T")


def set_or_none(value: Optional[Sequence[T]]) -> Optional[AbstractSet[T]]:
    if value:
        return set(value)
    return None


@dataclass
class ResourceHandlerRequest(BaseResourceHandlerRequest):
    # pylint: disable=invalid-name
    desiredResourceState: Optional["ResourceModel"]
    previousResourceState: Optional["ResourceModel"]
    typeConfiguration: Optional["TypeConfigurationModel"]


@dataclass
class ResourceModel(BaseModel):
    Username: Optional[str]
    IamIdentityCenterId: Optional[str]
    Targets: Optional[Sequence["_Targets"]]

    @classmethod
    def _deserialize(
        cls: Type["_ResourceModel"],
        json_data: Optional[Mapping[str, Any]],
    ) -> Optional["_ResourceModel"]:
        if not json_data:
            return None
        dataclasses = {n: o for n, o in getmembers(sys.modules[__name__]) if isclass(o)}
        recast_object(cls, json_data, dataclasses)
        return cls(
            Username=json_data.get("Username"),
            IamIdentityCenterId=json_data.get("IamIdentityCenterId"),
            Targets=deserialize_list(json_data.get("Targets"), Targets),
        )


# work around possible type aliasing issues when variable has same name as a model
_ResourceModel = ResourceModel


@dataclass
class Targets(BaseModel):
    AccountId: Optional[str]
    Region: Optional[str]
    DbInstanceResourceId: Optional[str]

    @classmethod
    def _deserialize(
        cls: Type["_Targets"],
        json_data: Optional[Mapping[str, Any]],
    ) -> Optional["_Targets"]:
        if not json_data:
            return None
        return cls(
            AccountId=json_data.get("AccountId"),
            Region=json_data.get("Region"),
            DbInstanceResourceId=json_data.get("DbInstanceResourceId"),
        )


# work around possible type aliasing issues when variable has same name as a model
_Targets = Targets


@dataclass
class TypeConfigurationModel(BaseModel):

    @classmethod
    def _deserialize(
        cls: Type["_TypeConfigurationModel"],
        json_data: Optional[Mapping[str, Any]],
    ) -> Optional["_TypeConfigurationModel"]:
        if not json_data:
            return None
        return cls(
        )


# work around possible type aliasing issues when variable has same name as a model
_TypeConfigurationModel = TypeConfigurationModel


