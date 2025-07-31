# Athlete Performance Dashboard

## Overview
A modern web application for trainers to look up athletes and visualize their performance in force plate tests (IMTP, HJ, PPU, CMJ). The app features composite scores, dynamic charts, and printable reports, all powered by data from Google BigQuery.

## Architecture

- **Frontend:**
  - Location: `push-performance-insights-engine/`
  - Stack: React (TypeScript), Material-UI, modern charting libraries
  - Features: Trainer login (Google OAuth), athlete search, test visualizations, composite scores, PDF export

- **Backend:**
  - Location: `backend-api/` (to be created)
  - Stack: Node.js (Express), Google BigQuery client, Google OAuth
  - Features: Auth, athlete/test data API, composite score calculation, PDF report generation

## Getting Started
- Frontend and backend will be developed in parallel.
- See respective folders for setup instructions (to be added).