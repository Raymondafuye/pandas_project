import pytest
from fastapi.testclient import TestClient
from src.main import app
import io

client = TestClient(app)

class TestHealthAPI:
    
    def test_root_endpoint(self):
        """Test root endpoint"""
        response = client.get("/")
        assert response.status_code == 200
        assert "Nigeria Health Data Analysis API" in response.json()["message"]
    
    def test_upload_invalid_file(self):
        """Test upload with invalid file type"""
        files = {"file": ("test.txt", io.StringIO("test content"), "text/plain")}
        response = client.post("/api/v1/upload", files=files)
        assert response.status_code == 400
        assert "Only CSV files are allowed" in response.json()["detail"]
    
    def test_upload_valid_csv(self):
        """Test upload with valid CSV"""
        csv_content = "col1,col2,col3\n1,2,3\n4,5,6"
        files = {"file": ("test.csv", io.StringIO(csv_content), "text/csv")}
        response = client.post("/api/v1/upload", files=files)
        assert response.status_code == 200
        data = response.json()
        assert data["filename"] == "test.csv"
        assert data["rows"] == 2
        assert data["columns"] == 3
    
    def test_schema_without_data(self):
        """Test schema endpoint without loaded data"""
        response = client.get("/api/v1/schema")
        assert response.status_code == 400
    
    def test_query_sex_without_data(self):
        """Test sex query without loaded data"""
        response = client.get("/api/v1/query/sex?sex=Male")
        assert response.status_code == 400
    
    def test_query_year_without_data(self):
        """Test year query without loaded data"""
        response = client.get("/api/v1/query/year?year=2000")
        assert response.status_code == 400
    
    def test_create_summary_job_without_data(self):
        """Test summary job creation without data"""
        response = client.post("/api/v1/summarize")
        assert response.status_code == 500
    
    def test_get_nonexistent_job(self):
        """Test getting non-existent job"""
        response = client.get("/api/v1/jobs/nonexistent-id")
        assert response.status_code == 404
    
    def test_query_parameters_validation(self):
        """Test query parameter validation"""
        # Test invalid limit
        response = client.get("/api/v1/query/sex?sex=Male&limit=0")
        assert response.status_code == 422
        
        response = client.get("/api/v1/query/sex?sex=Male&limit=2000")
        assert response.status_code == 422

class TestWithData:
    """Tests that require data to be loaded first"""
    
    @pytest.fixture(autouse=True)
    def setup_data(self):
        """Load test data before each test"""
        csv_content = "SEX (DISPLAY),YEAR (DISPLAY),Numeric\nMale,2000,10\nFemale,2001,20\nBoth sexes,2002,30"
        files = {"file": ("test.csv", io.StringIO(csv_content), "text/csv")}
        response = client.post("/api/v1/upload", files=files)
        assert response.status_code == 200
    
    def test_schema_with_data(self):
        """Test schema endpoint with loaded data"""
        response = client.get("/api/v1/schema")
        assert response.status_code == 200
        data = response.json()
        assert "columns" in data
        assert "dtypes" in data
        assert "shape" in data
        assert "sample_data" in data
    
    def test_query_sex_with_data(self):
        """Test sex query with loaded data"""
        response = client.get("/api/v1/query/sex?sex=Male")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "count" in data
    
    def test_query_year_with_data(self):
        """Test year query with loaded data"""
        response = client.get("/api/v1/query/year?year=2000")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "count" in data
    
    @pytest.mark.asyncio
    async def test_summary_job_workflow(self):
        """Test complete summary job workflow"""
        # Create job
        response = client.post("/api/v1/summarize")
        assert response.status_code == 200
        job_data = response.json()
        job_id = job_data["job_id"]
        assert job_data["status"] in ["pending", "running"]
        
        # Check job status
        response = client.get(f"/api/v1/jobs/{job_id}")
        assert response.status_code == 200
        
        # Wait and check completion (in real scenario, would poll)
        import asyncio
        await asyncio.sleep(3)
        
        response = client.get(f"/api/v1/jobs/{job_id}")
        assert response.status_code == 200
        final_data = response.json()
        assert final_data["status"] in ["completed", "failed"]