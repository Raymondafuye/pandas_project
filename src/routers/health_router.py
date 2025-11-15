from fastapi import APIRouter, UploadFile, File, Query
from controllers.health_controller import health_controller
from models.schemas import JobResponse, SchemaResponse, UploadResponse

router = APIRouter(
    prefix="/api/v1",
    tags=["health-data"],
    responses={404: {"description": "Not found"}}
)

@router.post("/upload", response_model=UploadResponse, summary="Upload CSV dataset")
async def upload_dataset(file: UploadFile = File(...)):
    """
    Upload and ingest CSV dataset asynchronously.
    Handles file size efficiently and validates CSV format.
    """
    return await health_controller.upload_csv(file)

@router.get("/schema", response_model=SchemaResponse, summary="Get dataset schema")
def get_dataset_schema():
    """
    Inspect dataset schema including columns, data types, and sample values.
    Returns comprehensive metadata about the loaded dataset.
    """
    return health_controller.get_schema()

@router.get("/query/sex", summary="Query data by sex")
def query_by_sex(
    sex: str = Query(..., description="Sex filter (Male, Female, Both sexes)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum records to return")
):
    """
    Query health data filtered by sex category.
    Supports filtering by Male, Female, or Both sexes.
    """
    return health_controller.query_by_sex(sex, limit)

@router.get("/query/year", summary="Query data by year")
def query_by_year(
    year: str = Query(..., description="Year to filter by"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum records to return")
):
    """
    Query health data filtered by specific year.
    Returns mortality and health indicators for the specified year.
    """
    return health_controller.query_by_year(year, limit)


@router.post("/summarize", response_model=JobResponse, summary="Create data summarization job")
def create_summary_job():
    """
    Create asynchronous background job for data summarization.
    Generates basic statistics, missing value analysis, and distributions.
    Returns job ID for tracking progress.
    """
    return health_controller.create_summary_job()

@router.get("/jobs/{job_id}", response_model=JobResponse, summary="Get job status and results")
def get_job_status(job_id: str):
    """
    Retrieve job status and results by job ID.
    Returns current status, completion time, and results if available.
    """
    return health_controller.get_job_result(job_id)