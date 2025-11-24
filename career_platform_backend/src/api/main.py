from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers import data_preview

app = FastAPI(
    title="Career Platform Backend",
    description="Backend API providing job platform features and data preview endpoints.",
    version="0.2.0",
    openapi_tags=[
        {"name": "admin", "description": "Administrative actions"},
        {"name": "preview", "description": "Dataset preview endpoints"},
    ],
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this via env
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", summary="Health Check", tags=["preview"])
def health_check():
    """Simple health check endpoint.
    Returns:
        JSON message confirming service health.
    """
    return {"message": "Healthy"}


# Register routers
app.include_router(data_preview.router)
