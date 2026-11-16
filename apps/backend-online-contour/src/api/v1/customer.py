from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from ...schemas.request import ChurnInferenceRequest
from ...schemas.response import ChurnInferenceResponse, CustomerCompensationsListResponse
from ...core.security import RoleChecker
from ..dependencies import (
    get_master_session,
    get_replica_session,
    get_action_engine_service,
)
from ...services.action_engine import ActionEngineService

router = APIRouter(tags=["Customer & Inference Contour"])


@router.post(
    "/inference/churn-risk",
    response_model=ChurnInferenceResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(RoleChecker(["OPERATOR", "DISPATCHER"]))],
)
async def post_churn_risk(
    payload: ChurnInferenceRequest,
    master_session: AsyncSession = Depends(get_master_session),
    replica_session: AsyncSession = Depends(get_replica_session),
    action_svc: ActionEngineService = Depends(get_action_engine_service),
) -> ChurnInferenceResponse:
    return await action_svc.run_churn_inference_pipeline(
        user_id=payload.user_id,
        order_id=payload.order_id,
        master_session=master_session,
        replica_session=replica_session,
    )


@router.get(
    "/customer/{user_id}/compensations",
    response_model=CustomerCompensationsListResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(RoleChecker(["OPERATOR", "DISPATCHER"]))],
)
async def get_customer_compensations(
    user_id: str,
    replica_session: AsyncSession = Depends(get_replica_session),
    action_svc: ActionEngineService = Depends(get_action_engine_service),
) -> CustomerCompensationsListResponse:
    return await action_svc.get_customer_compensations(
        user_id=user_id,
        replica_session=replica_session,
    )
