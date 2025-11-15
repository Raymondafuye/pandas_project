from fastapi import FastAPI
from routers import health_router
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Nigeria Health Data Analysis API",
    description="Asynchronous RESTful API for analyzing Nigeria's public health indicators",
    version="1.0.0"
)

app.include_router(health_router.router)

@app.get("/")
async def root():
    return {"message": "Nigeria Health Data Analysis API"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8006)