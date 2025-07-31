#!/usr/bin/env python3
"""
Startup script for the VALD Test Automation Server
"""

import os
import sys
import subprocess
from pathlib import Path

def main():
    """Start the automation server"""
    
    # Change to the Scripts directory
    script_dir = Path(__file__).parent
    os.chdir(script_dir)
    
    # Check if required files exist
    required_files = [
        "test_automation_server.py",
        "token_generator.py",
        "VALDapiHelpers.py",
        "enhanced_cmj_processor.py",
        "process_ppu.py",
        "process_hj.py",
        "process_imtp.py"
    ]
    
    missing_files = [f for f in required_files if not Path(f).exists()]
    if missing_files:
        print(f"ERROR: Missing required files: {missing_files}")
        sys.exit(1)
    
    # Check if .env file exists
    if not Path(".env").exists():
        print("WARNING: .env file not found. Make sure environment variables are set.")
    
    # Create reports directory
    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)
    
    print("Starting VALD Test Automation Server...")
    print("Server will be available at: http://localhost:8000")
    print("Webhook endpoint: http://localhost:8000/webhook/test-completion")
    print("Health check: http://localhost:8000/health")
    print("API docs: http://localhost:8000/docs")
    print("\nPress Ctrl+C to stop the server")
    
    try:
        # Start the server
        subprocess.run([
            sys.executable, "-m", "uvicorn", 
            "test_automation_server:app",
            "--host", "0.0.0.0",
            "--port", "8000",
            "--reload"
        ])
    except KeyboardInterrupt:
        print("\nServer stopped by user")
    except Exception as e:
        print(f"Error starting server: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 