from fastapi import UploadFile, HTTPException
from services.data_service import data_service
from models.schemas import QueryRequest, JobResponse, SchemaResponse, UploadResponse
import tempfile
import os
import logging

logger = logging.getLogger(__name__)

class HealthController:
    
    async def upload_csv(self, file: UploadFile) -> UploadResponse:
        """Handle CSV file upload"""
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="Only CSV files are allowed")
        
        try:
            # Save uploaded file temporarily
            with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
                content = await file.read()
                tmp_file.write(content)
                tmp_path = tmp_file.name
            
            # Load data using service
            df = await data_service.load_csv_async(tmp_path)
            
            # Clean up temp file
            os.unlink(tmp_path)
            
            return UploadResponse(
                message="File uploaded successfully",
                filename=file.filename,
                rows=df.shape[0],
                columns=df.shape[1]
            )
            
        except Exception as e:
            logger.error(f"Upload failed: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")
    
    def get_schema(self) -> SchemaResponse:
        """Get dataset schema"""
        try:
            schema_data = data_service.get_schema()
            return SchemaResponse(**schema_data)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Schema retrieval failed: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to retrieve schema")
    
    def query_by_sex(self, sex: str, limit: int = 100):
        """Query data by sex filter"""
        try:
            filters = [{"column": "SEX (DISPLAY)", "value": sex}]
            result = data_service.query_data(filters, limit)
            return {"data": result, "count": len(result)}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Sex query failed: {str(e)}")
            raise HTTPException(status_code=500, detail="Query failed")
    
    def query_by_year(self, year: str, limit: int = 100):
        """Query data by year filter"""
        try:
            filters = [{"column": "YEAR (DISPLAY)", "value": year}]
            result = data_service.query_data(filters, limit)
            return {"data": result, "count": len(result)}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Year query failed: {str(e)}")
            raise HTTPException(status_code=500, detail="Query failed")
    
    def create_summary_job(self) -> JobResponse:
        """Create background summarization job"""
        try:
            job_id = data_service.create_summary_job()
            job_data = data_service.get_job_status(job_id)
            return JobResponse(job_id=job_id, **job_data)
        except Exception as e:
            logger.error(f"Job creation failed: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to create job")
    
    def get_job_result(self, job_id: str) -> JobResponse:
        """Get job status and results"""
        try:
            job_data = data_service.get_job_status(job_id)
            return JobResponse(job_id=job_id, **job_data)
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            logger.error(f"Job retrieval failed: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to retrieve job")

health_controller = HealthController()