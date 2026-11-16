import uuid
from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from ...schemas.request import IncidentRerouteRequest
from ...schemas.response import ActiveIncidentsListResponse, IncidentRerouteResponse
from ...core.security import RoleChecker
from ..dependencies import (
    get_master_session,
    get_replica_session,
    get_logistics_ops_service,
)
from ...services.logistics_ops import LogisticsOperationsService

router = APIRouter(prefix="/logistics", tags=["Logistics Operations Contour"])


@router.get(
    "/incidents/active",
    response_model=ActiveIncidentsListResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(RoleChecker(["DISPATCHER", "CRITICAL_OPS"]))],
)
async def get_active_incidents(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    replica_session: AsyncSession = Depends(get_replica_session),
    logistics_svc: LogisticsOperationsService = Depends(get_logistics_ops_service),
) -> ActiveIncidentsListResponse:
    return await logistics_svc.get_active_incidents(
        replica_session=replica_session,
        limit=limit,
        offset=offset,
    )


@router.post(
    "/incidents/{incident_id}/reroute",
    response_model=IncidentRerouteResponse,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(RoleChecker(["DISPATCHER", "CRITICAL_OPS"]))],
)
async def post_incident_reroute(
    incident_id: uuid.UUID,
    payload: IncidentRerouteRequest,
    master_session: AsyncSession = Depends(get_master_session),
    logistics_svc: LogisticsOperationsService = Depends(get_logistics_ops_service),
) -> IncidentRerouteResponse:
    return await logistics_svc.reroute_incident(
        incident_id=incident_id,
        assigned_team=payload.assigned_team,
        bypass_hub_id=payload.bypass_hub_id,
        master_session=master_session,
    )
