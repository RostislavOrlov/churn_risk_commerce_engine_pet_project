from fastapi import APIRouter

from .v1.auth import router as auth_router
from .v1.customer import router as customer_router
from .v1.logistics import router as logistics_router
from .v1.topology import router as topology_router
from .v1.ws import router as ws_router

api_router = APIRouter()

api_router.include_router(auth_router)
api_router.include_router(customer_router)
api_router.include_router(logistics_router)
api_router.include_router(topology_router)
api_router.include_router(ws_router)
