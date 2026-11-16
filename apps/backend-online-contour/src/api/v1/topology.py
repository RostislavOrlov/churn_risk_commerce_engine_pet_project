from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from ...schemas.response import TopologyCapacitiesResponse, NodeHealthPassportResponse
from ...core.security import RoleChecker
from ..dependencies import (
    get_replica_session,
    get_topology_health_service,
)
from ...services.topology_health import TopologyHealthService

router = APIRouter(prefix="/topology", tags=["Network Topology Contour"])


@router.get(
    "/nodes/capacities",
    response_model=TopologyCapacitiesResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(RoleChecker(["ROUTING_ENGINE", "DISPATCHER"]))],
)
async def get_topology_capacities(
    replica_session: AsyncSession = Depends(get_replica_session),
    health_svc: TopologyHealthService = Depends(get_topology_health_service),
) -> TopologyCapacitiesResponse:
    return await health_svc.get_restricted_nodes_capacities(replica_session=replica_session)


@router.get(
    "/nodes/{node_id}/health-passport",
    response_model=NodeHealthPassportResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(RoleChecker(["DISPATCHER", "CRITICAL_OPS"]))],
)
async def get_node_health_passport(
    node_id: str,
    replica_session: AsyncSession = Depends(get_replica_session),
    health_svc: TopologyHealthService = Depends(get_topology_health_service),
) -> NodeHealthPassportResponse:
    return await health_svc.get_node_health_passport(
        node_id=node_id,
        replica_session=replica_session,
    )
