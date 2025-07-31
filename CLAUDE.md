# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Athlete Performance Dashboard that allows trainers to look up athletes and visualize their performance in force plate tests (IMTP, HJ, PPU, CMJ). The system consists of Python data processing scripts, a Node.js backend API, and a React TypeScript frontend with executive-level PDF report generation.

## Architecture

- **Python Scripts (`Scripts/`)**: Core data processing and automation
  - `VALDapiHelpers.py`: Main API client for VALD ForceDecks platform
  - `buildAthleteRefData.py`: Data ingestion and BigQuery integration
  - `generate_report.py`: Executive-level PDF report generation with dynamic circular composite score and professional spider charts
  - `start_automation_server.py`: Main automation server startup
  - Test processors: `enhanced_cmj_processor.py`, `process_ppu.py`, `process_hj.py`, `process_imtp.py`

- **Backend API (`backend-api/`)**: Node.js Express server
  - Google BigQuery integration for data storage and database-wide analytics
  - Database averages and max value queries for proper chart scaling
  - PDF report generation endpoint with comprehensive metrics
  - Google OAuth authentication

- **Frontend (`performance_data_app/`)**: React TypeScript application
  - Light corporate theme with Material-UI components
  - 3-section executive dashboard layout (20% composite score, 60% metrics grid, 20% spider chart)
  - Recharts for professional data visualization
  - Database-wide performance comparisons

## Common Commands

### Python Environment
```bash
# Install Python dependencies
pip install -r requirements.txt

# Start automation server
cd Scripts
python start_automation_server.py

# Run individual test processors
python enhanced_cmj_processor.py
python process_ppu.py
python process_hj.py  
python process_imtp.py

# Generate executive-level PDF reports (called via Node.js API)
# Requires athlete data, averages, and max values for proper scaling
python generate_report.py --athlete-name "Name" --test-date "2024-01-01" --composite-score 85.5 [full parameter set]
```

### Backend API
```bash
cd backend-api
npm install
npm run dev    # Development server with nodemon
npm start      # Production server
```

### Frontend React App
```bash
cd performance_data_app
npm install
npm start      # Development server
npm run build  # Production build
npm test       # Run tests
```

## Data Flow & Key Concepts

### Test Types and Metrics
- **CMJ (Countermovement Jump)**: 19 key metrics including jump height, power, force asymmetries
- **IMTP (Isometric Mid-Thigh Pull)**: Peak force and body mass relative measurements
- **PPU (Push-up)**: Force, RFD, and asymmetry metrics
- **HJ (Hop Jump)**: RSI (Reactive Strength Index) measurements

### Authentication & APIs
- Uses OAuth2 client credentials flow for VALD ForceDecks API
- Token caching in `.token_cache.json`
- Google BigQuery for data warehouse (`vald-ref-data` project)
- Service account authentication via `gcp_credentials.json`

### Data Processing Pipeline
1. API polling for new test results
2. Metric extraction and normalization
3. Composite score calculation
4. BigQuery data ingestion
5. Database-wide analytics (averages, max values)
6. Executive PDF report generation with dynamic scaling

### PDF Report Features
- **Single-page executive layout** optimized for stakeholder presentations
- **Dynamic circular composite score** with progress indicator
- **Professional spider chart** with database-wide comparisons
- **Corporate light theme** with Push Performance Analytics branding
- **Automatic scaling** based on actual database min/max values

## Environment Configuration

Required environment variables (typically in `.env`):
- `FORCEDECKS_URL`: VALD API endpoint
- `CLIENT_ID` / `CLIENT_SECRET`: OAuth credentials  
- `TENANT_ID`: VALD tenant identifier
- `AUTH_URL`: OAuth token endpoint

Google Cloud credentials file: `Scripts/gcp_credentials.json`

## Testing

Test files are prefixed with `test_` in the Scripts directory:
- `test_automation_server.py`
- `test_webhook_client.py` 
- `test_ppu_minimal.py`
- `test_metric_extraction.py`

No specific test runner configured - execute Python test files directly.

## Development Notes

- Frontend proxies API calls to `http://localhost:4000`
- BigQuery dataset: `athlete_performance_db` 
- PDF reports use ReportLab with matplotlib for executive-level visualizations
- UI features 3-section layout: composite score (20%), metrics grid (60%), spider chart (20%)
- Database queries include averages and max values for proper chart scaling
- CSV files in root contain sample/exported data for development

## Key API Endpoints

- `GET /athletes` - Search athletes by name
- `GET /athletes/:name/cmj-test-dates` - Get test dates for athlete
- `GET /athletes/:name/cmj-metric-percentiles` - Get metrics with database percentiles
- `GET /database-averages` - Get database-wide metric averages
- `POST /generate-report` - Generate executive PDF report

## Recent Updates

- Transformed from dark to light corporate theme
- Added dynamic circular composite score with progress indicator
- Implemented professional spider chart with database-wide comparisons
- Optimized single-page PDF layout for executive presentations
- Enhanced data scaling using actual database min/max values
- Updated branding to "Push Performance Analytics"

## Current PDF Layout (Latest Design)

### **Executive Single-Page Format**
- **Ultra-compact header**: "Performance Report" with athlete name/date on single line
- **Side-by-side main content**: 
  - Left: 1.6" circular composite score (no text header)
  - Right: Compact metrics table (Metric/Test/Avg columns)
- **Professional spider chart**: Clean design with minimal legend, corporate blue fill
- **No redundant headers**: All section titles removed for data-focused presentation

### **Key Styling Details**
- Margins: 0.6" sides, 0.5" top, 0.4" bottom for maximum content area
- Composite score: 40px font in 1.6" circle with progress indicator
- Metrics table: 8px font, compact spacing, alternating row colors
- Spider chart: 340x240px, frameless legend, corporate blue (#1976d2) and orange (#ff6f00)
- Database-wide scaling: All charts use actual min/max values from database queries

### **Backend API Enhancements**
- `/database-averages`: Returns database-wide metric averages
- Max value queries: Provides proper scaling ranges for spider chart
- Enhanced error handling and debugging for date formatting
- Comprehensive parameter passing to Python script (17 total parameters)

## Troubleshooting

### **Common Issues**
- **Backend restart required**: When modifying Node.js API code
- **Date formatting**: Uses proper string conversion to avoid [object Object] errors  
- **Spider chart scaling**: Automatically adjusts based on database min/max values
- **Single page fitting**: Optimized spacing and component sizes ensure everything fits

### **PDF Generation Parameters**
The Python script requires comprehensive parameters including:
- Athlete data (name, date, all 7 metrics)
- Database averages (7 metrics) 
- Database max values (7 metrics) for proper chart scaling