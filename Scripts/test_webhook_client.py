#!/usr/bin/env python3
"""
Test client for the VALD Test Automation Server
Simulates webhook calls to test the automation system
"""

import asyncio
import aiohttp
import json
from datetime import datetime
import random

# Test data
TEST_EVENTS = [
    {
        "test_id": "test_cmj_001",
        "athlete_id": "athlete_001",
        "test_type": "CMJ",
        "completion_time": datetime.now().isoformat(),
        "tenant_id": "tenant_001"
    },
    {
        "test_id": "test_ppu_001",
        "athlete_id": "athlete_002",
        "test_type": "PPU",
        "completion_time": datetime.now().isoformat(),
        "tenant_id": "tenant_001"
    },
    {
        "test_id": "test_hj_001",
        "athlete_id": "athlete_003",
        "test_type": "HJ",
        "completion_time": datetime.now().isoformat(),
        "tenant_id": "tenant_001"
    },
    {
        "test_id": "test_imtp_001",
        "athlete_id": "athlete_004",
        "test_type": "IMTP",
        "completion_time": datetime.now().isoformat(),
        "tenant_id": "tenant_001"
    }
]

async def test_webhook_endpoint():
    """Test the webhook endpoint with sample data"""
    server_url = "http://localhost:8000"
    
    async with aiohttp.ClientSession() as session:
        print("Testing VALD Test Automation Server...")
        
        # Test health endpoint
        try:
            async with session.get(f"{server_url}/health") as response:
                if response.status == 200:
                    health_data = await response.json()
                    print(f"✓ Health check passed: {health_data}")
                else:
                    print(f"✗ Health check failed: {response.status}")
                    return
        except Exception as e:
            print(f"✗ Cannot connect to server: {e}")
            print("Make sure the server is running on http://localhost:8000")
            return
        
        # Test webhook endpoint with each test type
        for event in TEST_EVENTS:
            print(f"\n--- Testing {event['test_type']} webhook ---")
            
            try:
                async with session.post(
                    f"{server_url}/webhook/test-completion",
                    json=event
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        print(f"✓ Webhook accepted: {result}")
                        
                        # Wait a bit and check status
                        await asyncio.sleep(2)
                        
                        # Check processing status
                        async with session.get(f"{server_url}/status/{event['test_id']}") as status_response:
                            if status_response.status == 200:
                                status_data = await status_response.json()
                                print(f"  Status: {status_data['status']} - {status_data['message']}")
                                
                                # If completed, try to get report
                                if status_data['status'] == 'completed' and status_data.get('report_url'):
                                    async with session.get(f"{server_url}{status_data['report_url']}") as report_response:
                                        if report_response.status == 200:
                                            report_data = await report_response.json()
                                            print(f"  ✓ Report generated: {report_data['athlete_info']['name']} - {report_data['performance_summary']['overall_rating']}")
                                        else:
                                            print(f"  ✗ Failed to get report: {report_response.status}")
                            else:
                                print(f"  ✗ Failed to get status: {status_response.status}")
                    else:
                        print(f"✗ Webhook failed: {response.status}")
                        error_text = await response.text()
                        print(f"  Error: {error_text}")
                        
            except Exception as e:
                print(f"✗ Exception during webhook test: {e}")

async def test_real_test_ids():
    """Test with real test IDs from your system"""
    # You can modify this to use real test IDs from your VALD system
    real_test_events = [
        {
            "test_id": "your_real_test_id_here",
            "athlete_id": "your_real_athlete_id_here",
            "test_type": "CMJ",
            "completion_time": datetime.now().isoformat(),
            "tenant_id": "your_tenant_id"
        }
    ]
    
    server_url = "http://localhost:8000"
    
    async with aiohttp.ClientSession() as session:
        print("\n--- Testing with real test IDs ---")
        
        for event in real_test_events:
            if event['test_id'] == "your_real_test_id_here":
                print("Skipping placeholder test ID. Update the script with real test IDs.")
                continue
                
            print(f"Testing real {event['test_type']} test: {event['test_id']}")
            
            try:
                async with session.post(
                    f"{server_url}/webhook/test-completion",
                    json=event
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        print(f"✓ Real test webhook accepted: {result}")
                    else:
                        print(f"✗ Real test webhook failed: {response.status}")
                        
            except Exception as e:
                print(f"✗ Exception during real test: {e}")

def main():
    """Main function to run tests"""
    print("VALD Test Automation Server - Test Client")
    print("=" * 50)
    
    # Run the tests
    asyncio.run(test_webhook_endpoint())
    asyncio.run(test_real_test_ids())
    
    print("\n" + "=" * 50)
    print("Test completed!")
    print("\nTo test with real data:")
    print("1. Update test_real_test_ids() with actual test IDs")
    print("2. Make sure your .env file has the correct VALD API credentials")
    print("3. Run this script again")

if __name__ == "__main__":
    main() 