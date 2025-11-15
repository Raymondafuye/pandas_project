from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from enum import Enum

class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class FilterRequest(BaseModel):
    column: str = Field(..., description="Column name to filter on")
    value: str = Field(..., description="Value to filter by")
    
class QueryRequest(BaseModel):
    filters: List[FilterRequest] = Field(default=[], description="List of filters to apply")
    limit: Optional[int] = Field(default=100, ge=1, le=10000, description="Maximum number of records to return")

class JobResponse(BaseModel):
    job_id: str = Field(..., description="Unique job identifier")
    status: JobStatus = Field(..., description="Current job status")
    created_at: str = Field(..., description="Job creation timestamp")
    completed_at: Optional[str] = Field(None, description="Job completion timestamp")
    result: Optional[Dict[str, Any]] = Field(None, description="Job result data")
    error: Optional[str] = Field(None, description="Error message if job failed")

class SchemaResponse(BaseModel):
    columns: List[str] = Field(..., description="List of column names")
    dtypes: Dict[str, str] = Field(..., description="Column data types")
    shape: tuple = Field(..., description="Dataset dimensions (rows, columns)")
    sample_data: List[Dict[str, Any]] = Field(..., description="Sample records from dataset")

class UploadResponse(BaseModel):
    message: str = Field(..., description="Upload status message")
    filename: str = Field(..., description="Name of uploaded file")
    rows: int = Field(..., description="Number of rows in dataset")
    columns: int = Field(..., description="Number of columns in dataset")