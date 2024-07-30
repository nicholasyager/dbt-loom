from typing import Mapping, Optional
from dbt.contracts.graph.nodes import GraphMemberNode, ModelNode
from dbt.contracts.graph.manifest import MaybeNonSource

try:
    from dbt.artifacts.resources.types import NodeType, AccessType
except ModuleNotFoundError:
    from dbt.node_types import NodeType, AccessType  # type: ignore


def is_invalid_protected_ref(
    self,
    node: GraphMemberNode,
    target_model: MaybeNonSource,
    dependencies: Optional[Mapping],
) -> bool:
    dependencies = dependencies or {}
    if not isinstance(target_model, ModelNode):
        return False

    is_protected_ref = (
        target_model.access == AccessType.Protected
        # don't raise this reference error for ad hoc 'preview' queries
        and node.resource_type != NodeType.SqlOperation
        and node.resource_type != NodeType.RPCCall  # TODO: rm
    )
    target_dependency = dependencies.get(target_model.package_name)
    restrict_package_access = (
        target_dependency.restrict_access if target_dependency else False
    )

    return is_protected_ref and (
        node.package_name != target_model.package_name and restrict_package_access
    )


def is_invalid_private_ref(
    self,
    node: GraphMemberNode,
    target_model: MaybeNonSource,
    dependencies: Optional[Mapping],
) -> bool:
    dependencies = dependencies or {}
    if not isinstance(target_model, ModelNode):
        return False

    is_private_ref = (
        target_model.access == AccessType.Private
        # don't raise this reference error for ad hoc 'preview' queries
        and node.resource_type != NodeType.SqlOperation
        and node.resource_type != NodeType.RPCCall  # TODO: rm
    )
    target_dependency = dependencies.get(target_model.package_name)
    restrict_package_access = (
        target_dependency.restrict_access if target_dependency else False
    )

    return is_private_ref and (
        # Invalid reference because the group does not match
        (hasattr(node, "group") and node.group and node.group != target_model.group)  # type: ignore
        # Or, invalid because these are different namespaces (project/package) and restrict-access is enforced
        or (node.package_name != target_model.package_name and restrict_package_access)
    )
