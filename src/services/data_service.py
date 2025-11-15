import polars as pl
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime
import uuid
import logging

logger = logging.getLogger(__name__)

class DataService:
    def __init__(self):
        self.data: Optional[pl.DataFrame] = None
        self.jobs: Dict[str, Dict[str, Any]] = {}
    
    async def load_csv_async(self, file_path: str) -> pl.DataFrame:
        """Load CSV file asynchronously using Polars"""
        loop = asyncio.get_event_loop()
        df = await loop.run_in_executor(None, pl.read_csv, file_path)
        self.data = df
        logger.info(f"Loaded CSV with {df.shape[0]} rows and {df.shape[1]} columns")
        return df
    
    def get_schema(self) -> Dict[str, Any]:
        """Get dataset schema information"""
        if self.data is None:
            raise ValueError("No data loaded")
        
        return {
            "columns": self.data.columns,
            "dtypes": {col: str(dtype) for col, dtype in zip(self.data.columns, self.data.dtypes)},
            "shape": self.data.shape,
            "sample_data": self.data.head(5).to_dicts()
        }
    
    def query_data(self, filters: List[Dict[str, str]], limit: int = 100) -> List[Dict[str, Any]]:
        """Query data with filters"""
        if self.data is None:
            raise ValueError("No data loaded")
        
        df = self.data
        for filter_item in filters:
            col = filter_item["column"]
            val = filter_item["value"]
            if col in df.columns:
                df = df.filter(pl.col(col).cast(pl.Utf8).str.contains(val, literal=True))
        
        return df.head(limit).to_dicts()
    
    def create_summary_job(self) -> str:
        """Create background job for data summarization"""
        job_id = str(uuid.uuid4())
        self.jobs[job_id] = {
            "status": "pending",
            "created_at": datetime.now().isoformat(),
            "completed_at": None,
            "result": None,
            "error": None
        }
        
        # Start background task
        asyncio.create_task(self._run_summary_job(job_id))
        return job_id
    
    async def _run_summary_job(self, job_id: str):
        """Run data summarization in background"""
        try:
            self.jobs[job_id]["status"] = "running"
            
            if self.data is None:
                raise ValueError("No data loaded")
            
            # Simulate processing time
            await asyncio.sleep(2)
            
            # Generate summaries
            basic_stats = self._get_basic_statistics()
            missing_values = self._get_missing_values()
            distributions = self._get_distributions()
            
            result = {
                "basic_statistics": basic_stats,
                "missing_values": missing_values,
                "distributions": distributions
            }
            
            self.jobs[job_id].update({
                "status": "completed",
                "completed_at": datetime.now().isoformat(),
                "result": result
            })
            
        except Exception as e:
            self.jobs[job_id].update({
                "status": "failed",
                "completed_at": datetime.now().isoformat(),
                "error": str(e)
            })
    
    def _get_basic_statistics(self) -> Dict[str, Any]:
        """Generate basic statistics"""
        numeric_cols = [col for col, dtype in zip(self.data.columns, self.data.dtypes) 
                       if dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]]
        
        if not numeric_cols:
            return {"message": "No numeric columns found"}
        
        stats = self.data.select(numeric_cols).describe()
        return stats.to_dicts()
    
    def _get_missing_values(self) -> Dict[str, Any]:
        """Calculate missing values per column"""
        missing = {}
        for col in self.data.columns:
            null_count = self.data.select(pl.col(col).is_null().sum()).item()
            missing[col] = {
                "null_count": null_count,
                "null_percentage": (null_count / self.data.shape[0]) * 100
            }
        return missing
    
    def _get_distributions(self) -> Dict[str, Any]:
        """Get value distributions for categorical columns"""
        distributions = {}
        for col in self.data.columns[:5]:  # Limit to first 5 columns
            try:
                value_counts = self.data.select(pl.col(col)).to_series().value_counts()
                distributions[col] = value_counts.head(10).to_dicts()
            except:
                distributions[col] = {"error": "Could not compute distribution"}
        return distributions
    
    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get job status and results"""
        if job_id not in self.jobs:
            raise ValueError("Job not found")
        return self.jobs[job_id]

# Global instance
data_service = DataService()