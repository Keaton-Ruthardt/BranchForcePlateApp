import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import uuid
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn
import subprocess
import sys
import os
from pathlib import Path

# Import existing modules
from token_generator import get_access_token
from VALDapiHelpers import get_profiles, FD_Tests_by_Profile, get_FD_results
from enhanced_cmj_processor import process_cmj_test_with_composite
from process_ppu import process_json_to_pivoted_df
from process_hj import process_json_to_pivoted_df as process_hj_json
from process_imtp import get_FD_results as get_imtp_results

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_automation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(title="VALD Test Automation Server", version="1.0.0")

# Pydantic models for request/response
class TestCompletionEvent(BaseModel):
    test_id: str
    athlete_id: str
    test_type: str
    completion_time: str
    tenant_id: str

class ProcessingStatus(BaseModel):
    test_id: str
    status: str
    message: str
    timestamp: str
    report_url: Optional[str] = None

# Global storage for processing status (in production, use a database)
processing_status: Dict[str, ProcessingStatus] = {}

class TestProcessor:
    """Handles automatic processing of different test types"""
    
    def __init__(self):
        self.token = get_access_token()
        self.test_processors = {
            'CMJ': self.process_cmj_test,
            'PPU': self.process_ppu_test,
            'HJ': self.process_hj_test,
            'IMTP': self.process_imtp_test
        }
    
    async def process_test(self, test_id: str, athlete_id: str, test_type: str) -> Dict:
        """Main method to process any test type"""
        logger.info(f"Starting processing for test {test_id} (type: {test_type})")
        
        if test_type not in self.test_processors:
            raise ValueError(f"Unsupported test type: {test_type}")
        
        try:
            # Get athlete info
            profiles = get_profiles(self.token)
            athlete_info = profiles[profiles['profileId'] == athlete_id].iloc[0] if not profiles.empty else None
            
            if athlete_info is None:
                raise ValueError(f"Athlete {athlete_id} not found")
            
            # Process the test
            result = await self.test_processors[test_type](test_id, athlete_info)
            
            # Generate report
            report_data = await self.generate_report(test_id, test_type, athlete_info, result)
            
            return {
                "success": True,
                "test_id": test_id,
                "test_type": test_type,
                "athlete_name": athlete_info['fullName'],
                "processed_data": result,
                "report": report_data
            }
            
        except Exception as e:
            logger.error(f"Error processing test {test_id}: {str(e)}")
            return {
                "success": False,
                "test_id": test_id,
                "error": str(e)
            }
    
    async def process_cmj_test(self, test_id: str, athlete_info: pd.Series) -> Dict:
        """Process CMJ test with composite scoring"""
        logger.info(f"Processing CMJ test {test_id}")
        
        # Use the existing enhanced CMJ processor
        assessment_id = str(uuid.uuid4())
        result_df, composite_score = process_cmj_test_with_composite(test_id, self.token, assessment_id)
        
        if result_df is None:
            raise ValueError("No CMJ data found for processing")
        
        return {
            "assessment_id": assessment_id,
            "composite_score": composite_score,
            "metrics": result_df.to_dict('records'),
            "test_type": "CMJ"
        }
    
    async def process_ppu_test(self, test_id: str, athlete_info: pd.Series) -> Dict:
        """Process PPU test"""
        logger.info(f"Processing PPU test {test_id}")
        
        # Fetch raw data
        url = f"https://api.vald.com/v2019q3/teams/{os.getenv('TENANT_ID')}/tests/{test_id}/trials"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status != 200:
                    raise ValueError(f"Failed to fetch PPU data: {response.status}")
                
                json_data = await response.json()
                pivoted_df = process_json_to_pivoted_df(json_data)
                
                if pivoted_df is None or pivoted_df.empty:
                    raise ValueError("No PPU data found for processing")
                
                # Extract key metrics
                metrics = {}
                for _, row in pivoted_df.iterrows():
                    metric_id = row['metric_id']
                    trial_values = [row[col] for col in row.index if 'trial' in col and pd.notna(row[col])]
                    if trial_values:
                        metrics[metric_id] = {
                            "best_value": max(trial_values),
                            "all_values": trial_values,
                            "num_trials": len(trial_values)
                        }
                
                return {
                    "assessment_id": str(uuid.uuid4()),
                    "metrics": metrics,
                    "test_type": "PPU"
                }
    
    async def process_hj_test(self, test_id: str, athlete_info: pd.Series) -> Dict:
        """Process Horizontal Jump test"""
        logger.info(f"Processing HJ test {test_id}")
        
        # Fetch raw data
        url = f"https://api.vald.com/v2019q3/teams/{os.getenv('TENANT_ID')}/tests/{test_id}/trials"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status != 200:
                    raise ValueError(f"Failed to fetch HJ data: {response.status}")
                
                json_data = await response.json()
                pivoted_df = process_hj_json(json_data)
                
                if pivoted_df is None or pivoted_df.empty:
                    raise ValueError("No HJ data found for processing")
                
                # Calculate RSI metrics
                rsi_metrics = {}
                for _, row in pivoted_df.iterrows():
                    metric_id = row['metric_id']
                    if 'RSI' in metric_id:
                        trial_values = [row[col] for col in row.index if 'trial' in col and pd.notna(row[col])]
                        if trial_values:
                            # Get best 5 RSI values
                            best_5 = sorted(trial_values, reverse=True)[:5]
                            avg_rsi = sum(best_5) / len(best_5)
                            rsi_metrics[metric_id] = {
                                "best_5_avg": avg_rsi,
                                "all_values": trial_values,
                                "best_5_values": best_5
                            }
                
                return {
                    "assessment_id": str(uuid.uuid4()),
                    "rsi_metrics": rsi_metrics,
                    "test_type": "HJ"
                }
    
    async def process_imtp_test(self, test_id: str, athlete_info: pd.Series) -> Dict:
        """Process IMTP test"""
        logger.info(f"Processing IMTP test {test_id}")
        
        # Use existing IMTP processor
        result_df = get_imtp_results(test_id, self.token)
        
        if result_df is None or result_df.empty:
            raise ValueError("No IMTP data found for processing")
        
        # Extract key metrics
        metrics = {}
        for _, row in result_df.iterrows():
            metric_id = row['metric_id']
            trial_values = [row[col] for col in row.index if 'trial' in col and pd.notna(row[col])]
            if trial_values:
                metrics[metric_id] = {
                    "best_value": max(trial_values),
                    "all_values": trial_values,
                    "num_trials": len(trial_values)
                }
        
        return {
            "assessment_id": str(uuid.uuid4()),
            "metrics": metrics,
            "test_type": "IMTP"
        }
    
    async def generate_report(self, test_id: str, test_type: str, athlete_info: pd.Series, result: Dict) -> Dict:
        """Generate a comprehensive report for the processed test"""
        logger.info(f"Generating report for test {test_id}")
        
        # Calculate athlete age
        test_date = datetime.now().date()
        dob = pd.to_datetime(athlete_info['dateOfBirth']).date()
        age = test_date.year - dob.year - ((test_date.month, test_date.day) < (dob.month, dob.day))
        
        # Generate insights based on test type
        insights = self.generate_insights(test_type, result)
        
        # Create performance summary
        performance_summary = self.create_performance_summary(test_type, result)
        
        report = {
            "report_id": str(uuid.uuid4()),
            "generated_at": datetime.now().isoformat(),
            "athlete_info": {
                "name": athlete_info['fullName'],
                "age": age,
                "profile_id": athlete_info['profileId']
            },
            "test_info": {
                "test_id": test_id,
                "test_type": test_type,
                "test_date": test_date.isoformat()
            },
            "performance_summary": performance_summary,
            "insights": insights,
            "recommendations": self.generate_recommendations(test_type, result, insights)
        }
        
        # Save report to file
        report_filename = f"reports/{test_id}_{test_type}_report.json"
        os.makedirs("reports", exist_ok=True)
        with open(report_filename, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        return report
    
    def generate_insights(self, test_type: str, result: Dict) -> List[str]:
        """Generate insights based on test results"""
        insights = []
        
        if test_type == "CMJ":
            composite_score = result.get("composite_score", 0)
            if composite_score > 0.8:
                insights.append("Excellent composite performance score")
            elif composite_score > 0.6:
                insights.append("Good composite performance score")
            else:
                insights.append("Composite performance needs improvement")
        
        elif test_type == "PPU":
            metrics = result.get("metrics", {})
            peak_force = metrics.get("PEAK_CONCENTRIC_FORCE_Trial_N", {})
            if peak_force:
                best_value = peak_force.get("best_value", 0)
                if best_value > 1000:  # Example threshold
                    insights.append("Strong peak concentric force")
                else:
                    insights.append("Peak force could be improved")
        
        elif test_type == "HJ":
            rsi_metrics = result.get("rsi_metrics", {})
            for metric_id, data in rsi_metrics.items():
                avg_rsi = data.get("best_5_avg", 0)
                if avg_rsi > 0.8:
                    insights.append(f"Excellent {metric_id} performance")
                elif avg_rsi > 0.6:
                    insights.append(f"Good {metric_id} performance")
                else:
                    insights.append(f"{metric_id} needs improvement")
        
        return insights
    
    def create_performance_summary(self, test_type: str, result: Dict) -> Dict:
        """Create a summary of key performance metrics"""
        summary = {
            "test_type": test_type,
            "key_metrics": {},
            "overall_rating": "Good"  # Default rating
        }
        
        if test_type == "CMJ":
            composite_score = result.get("composite_score", 0)
            summary["key_metrics"]["composite_score"] = composite_score
            summary["overall_rating"] = "Excellent" if composite_score > 0.8 else "Good" if composite_score > 0.6 else "Needs Improvement"
        
        elif test_type == "PPU":
            metrics = result.get("metrics", {})
            for metric_id, data in metrics.items():
                if "PEAK" in metric_id or "FORCE" in metric_id:
                    summary["key_metrics"][metric_id] = data.get("best_value", 0)
        
        elif test_type == "HJ":
            rsi_metrics = result.get("rsi_metrics", {})
            for metric_id, data in rsi_metrics.items():
                summary["key_metrics"][metric_id] = data.get("best_5_avg", 0)
        
        return summary
    
    def generate_recommendations(self, test_type: str, result: Dict, insights: List[str]) -> List[str]:
        """Generate training recommendations based on results"""
        recommendations = []
        
        if test_type == "CMJ":
            composite_score = result.get("composite_score", 0)
            if composite_score < 0.6:
                recommendations.append("Focus on plyometric training to improve jump performance")
                recommendations.append("Include strength training for lower body power development")
            else:
                recommendations.append("Maintain current training program")
                recommendations.append("Consider advanced plyometric variations")
        
        elif test_type == "PPU":
            recommendations.append("Continue with upper body strength training")
            recommendations.append("Focus on explosive push-up variations")
        
        elif test_type == "HJ":
            recommendations.append("Incorporate horizontal jump training")
            recommendations.append("Focus on landing mechanics and force absorption")
        
        return recommendations

# Initialize processor
processor = TestProcessor()

@app.post("/webhook/test-completion", response_model=ProcessingStatus)
async def handle_test_completion(event: TestCompletionEvent, background_tasks: BackgroundTasks):
    """Webhook endpoint to handle test completion events"""
    logger.info(f"Received test completion event: {event.test_id} ({event.test_type})")
    
    # Create initial status
    status = ProcessingStatus(
        test_id=event.test_id,
        status="processing",
        message="Test processing started",
        timestamp=datetime.now().isoformat()
    )
    processing_status[event.test_id] = status
    
    # Add background task for processing
    background_tasks.add_task(process_test_background, event)
    
    return status

async def process_test_background(event: TestCompletionEvent):
    """Background task to process the test"""
    try:
        # Process the test
        result = await processor.process_test(event.test_id, event.athlete_id, event.test_type)
        
        if result["success"]:
            # Update status
            processing_status[event.test_id] = ProcessingStatus(
                test_id=event.test_id,
                status="completed",
                message=f"Successfully processed {event.test_type} test",
                timestamp=datetime.now().isoformat(),
                report_url=f"/reports/{event.test_id}_{event.test_type}_report.json"
            )
            logger.info(f"Successfully processed test {event.test_id}")
        else:
            # Update status with error
            processing_status[event.test_id] = ProcessingStatus(
                test_id=event.test_id,
                status="failed",
                message=f"Failed to process test: {result.get('error', 'Unknown error')}",
                timestamp=datetime.now().isoformat()
            )
            logger.error(f"Failed to process test {event.test_id}: {result.get('error')}")
    
    except Exception as e:
        # Update status with exception
        processing_status[event.test_id] = ProcessingStatus(
            test_id=event.test_id,
            status="failed",
            message=f"Exception during processing: {str(e)}",
            timestamp=datetime.now().isoformat()
        )
        logger.error(f"Exception processing test {event.test_id}: {str(e)}")

@app.get("/status/{test_id}", response_model=ProcessingStatus)
async def get_processing_status(test_id: str):
    """Get the processing status of a test"""
    if test_id not in processing_status:
        raise HTTPException(status_code=404, detail="Test not found")
    return processing_status[test_id]

@app.get("/reports/{test_id}_{test_type}_report.json")
async def get_test_report(test_id: str, test_type: str):
    """Get the generated report for a test"""
    report_filename = f"reports/{test_id}_{test_type}_report.json"
    if not os.path.exists(report_filename):
        raise HTTPException(status_code=404, detail="Report not found")
    
    with open(report_filename, 'r') as f:
        report_data = json.load(f)
    
    return JSONResponse(content=report_data)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    # Create reports directory
    os.makedirs("reports", exist_ok=True)
    
    # Run the server
    uvicorn.run(
        "test_automation_server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    ) 