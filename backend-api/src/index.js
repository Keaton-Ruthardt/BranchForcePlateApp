const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const { spawn } = require('child_process');
const path = require('path');
require('dotenv').config();

const { login, authenticateJWT } = require('./auth');
const { BigQuery } = require('@google-cloud/bigquery');
const VALDAPIService = require('./vald-service');
const AnalyticsService = require('./analytics-service');

const app = express();
const PORT = process.env.PORT || 4000;

app.use(cors());
app.use(bodyParser.json());

// Login endpoint
app.post('/login', login);

// Example of a protected route
app.get('/protected', authenticateJWT, (req, res) => {
  res.json({ message: 'You are authenticated!', user: req.user });
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'ok', message: 'Athlete Performance API is running.' });
});

// Initialize services
const valdAPI = new VALDAPIService();
const analytics = new AnalyticsService();

// Legacy BigQuery utility (keeping for backwards compatibility)
const bqClient = new BigQuery({
  keyFilename: path.join(__dirname, '../../Scripts/gcp_credentials.json'),
  projectId: 'vald-ref-data',
});
const DATASET = 'athlete_performance_db';

// Hybrid athlete search - VALD API primary, BigQuery fallback
app.get('/athletes', async (req, res) => {
  const search = req.query.search || '';
  
  try {
    // Primary: Search live athletes from VALD API
    console.log(`Searching athletes for: "${search}"`);
    const liveResults = await valdAPI.searchAthletes(search);
    
    if (liveResults && liveResults.length > 0) {
      console.log(`Found ${liveResults.length} athletes from VALD API`);
      return res.json(liveResults);
    }
    
    // Fallback: Search cached athletes from BigQuery
    console.log('VALD API returned no results, falling back to BigQuery');
    const cachedResults = await analytics.searchAthletesFallback(search);
    res.json(cachedResults);
    
  } catch (err) {
    console.error('Athlete search error:', err);
    
    try {
      // Final fallback to BigQuery
      const cachedResults = await analytics.searchAthletesFallback(search);
      res.json(cachedResults.map(r => ({ ...r, source: 'cached', error: 'API unavailable' })));
    } catch (fallbackErr) {
      console.error('Fallback search also failed:', fallbackErr);
      res.status(500).json({ error: 'Failed to search athletes' });
    }
  }
});

// Get all test results for an athlete by athlete name
app.get('/athletes/:athleteName/results', async (req, res) => {
  const athleteName = decodeURIComponent(req.params.athleteName);
  try {
    const query = `
      SELECT result_id, assessment_id, FORMAT_DATE('%Y-%m-%d', DATE(test_date)) AS test_date, cmj_composite_score, CONCENTRIC_IMPULSE_Trial_Ns, ECCENTRIC_BRAKING_RFD_Trial_N_s, PEAK_CONCENTRIC_FORCE_Trial_N, BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg, RSI_MODIFIED_Trial_RSI_mod, ECCENTRIC_BRAKING_IMPULSE_Trial_Ns
      FROM \`${DATASET}.cmj_results\`
      WHERE athlete_name = @athleteName
      ORDER BY test_date DESC, result_id DESC
      LIMIT 100
    `;
    const options = {
      query,
      params: { athleteName },
      location: 'US',
    };
    const [rows] = await bqClient.query(options);
    res.json(rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to fetch athlete results' });
  }
});

// Get composite scores for an athlete by athleteId
app.get('/athletes/:athleteId/composite-scores', async (req, res) => {
  const athleteId = req.params.athleteId;
  try {
    const query = `
      SELECT result_id, assessment_id, cmj_composite_score, CONCENTRIC_IMPULSE_Trial_Ns, ECCENTRIC_BRAKING_RFD_Trial_N_s, PEAK_CONCENTRIC_FORCE_Trial_N, BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg, RSI_MODIFIED_Trial_RSI_mod, ECCENTRIC_BRAKING_IMPULSE_Trial_Ns
      FROM \`${DATASET}.cmj_results\`
      WHERE athlete_id = @athleteId
      ORDER BY test_date DESC, result_id DESC
      LIMIT 100
    `;
    const options = {
      query,
      params: { athleteId },
      location: 'US',
    };
    const [rows] = await bqClient.query(options);
    res.json(rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to fetch composite scores' });
  }
});

// Get CMJ metric averages for an athlete by athleteId
app.get('/athletes/:athleteId/cmj-averages', async (req, res) => {
  const athleteId = req.params.athleteId;
  try {
    const query = `
      SELECT
        AVG(cmj_composite_score) AS cmj_composite_score,
        AVG(CONCENTRIC_IMPULSE_Trial_Ns) AS CONCENTRIC_IMPULSE_Trial_Ns,
        AVG(ECCENTRIC_BRAKING_RFD_Trial_N_s) AS ECCENTRIC_BRAKING_RFD_Trial_N_s,
        AVG(PEAK_CONCENTRIC_FORCE_Trial_N) AS PEAK_CONCENTRIC_FORCE_Trial_N,
        AVG(BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg) AS BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg,
        AVG(RSI_MODIFIED_Trial_RSI_mod) AS RSI_MODIFIED_Trial_RSI_mod,
        AVG(ECCENTRIC_BRAKING_IMPULSE_Trial_Ns) AS ECCENTRIC_BRAKING_IMPULSE_Trial_Ns
      FROM \`${DATASET}.cmj_results\`
      WHERE athlete_id = @athleteId
    `;
    const options = {
      query,
      params: { athleteId },
      location: 'US',
    };
    const [rows] = await bqClient.query(options);
    res.json(rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to fetch CMJ averages' });
  }
});

// Get all test dates for an athlete by athleteId (with debug logging)
app.get('/athletes/:athleteId/cmj-test-dates', async (req, res) => {
  const athleteId = req.params.athleteId;
  try {
    const query = `
      SELECT result_id, assessment_id, FORMAT_DATE('%Y-%m-%d', DATE(test_date)) AS test_date
      FROM \`${DATASET}.cmj_results\`
      WHERE athlete_id = @athleteId
      ORDER BY test_date DESC, result_id DESC
      LIMIT 100
    `;
    const options = {
      query,
      params: { athleteId },
      location: 'US',
    };
    const [rows] = await bqClient.query(options);
    console.log('cmj-test-dates for athleteId', athleteId, rows);
    res.json(rows);
  } catch (err) {
    console.error('Failed to fetch cmj-test-dates:', err);
    res.status(500).json({ error: 'Failed to fetch cmj-test-dates' });
  }
});

// Hybrid test dates - VALD API primary, BigQuery fallback
app.get('/athletes/:athleteId/test-dates', async (req, res) => {
  const athleteId = req.params.athleteId;
  
  try {
    // Primary: Get live test dates from VALD API
    console.log(`Getting test dates for athlete: ${athleteId}`);
    const liveTestDates = await valdAPI.getAthleteTestDates(athleteId);
    
    if (liveTestDates && liveTestDates.length > 0) {
      console.log(`Found ${liveTestDates.length} test dates from VALD API`);
      return res.json(liveTestDates.map(date => ({ ...date, source: 'live' })));
    }
    
    // Fallback: Get cached test dates from BigQuery
    console.log('VALD API returned no test dates, falling back to BigQuery');
    const cachedDates = await analytics.getTestDatesFallback(athleteId);
    res.json(cachedDates);
    
  } catch (err) {
    console.error('Test dates error:', err);
    
    try {
      // Final fallback to BigQuery
      const cachedDates = await analytics.getTestDatesFallback(athleteId);
      res.json(cachedDates.map(d => ({ ...d, source: 'cached', error: 'API unavailable' })));
    } catch (fallbackErr) {
      console.error('Fallback test dates also failed:', fallbackErr);
      res.status(500).json({ error: 'Failed to fetch test dates' });
    }
  }
});

// Legacy endpoint for backward compatibility
app.get('/athletes/:athleteName/cmj-test-dates', async (req, res) => {
  const athleteName = decodeURIComponent(req.params.athleteName);
  try {
    const query = `
      SELECT result_id, assessment_id, FORMAT_DATE('%Y-%m-%d', DATE(test_date)) AS test_date
      FROM \`${DATASET}.cmj_results\`
      WHERE athlete_name = @athleteName
      ORDER BY test_date DESC, result_id DESC
      LIMIT 100
    `;
    const options = {
      query,
      params: { athleteName },
      location: 'US',
    };
    const [rows] = await bqClient.query(options);
    console.log('cmj-test-dates for athleteName', athleteName, rows);
    res.json(rows);
  } catch (err) {
    console.error('Failed to fetch cmj-test-dates:', err);
    res.status(500).json({ error: 'Failed to fetch cmj-test-dates' });
  }
});

// Get metrics for a specific result ID
app.get('/athletes/:athleteName/cmj-metric-percentiles', async (req, res) => {
  const athleteName = decodeURIComponent(req.params.athleteName);
  const resultId = req.query.resultId;
  
  if (!resultId) {
    return res.status(400).json({ error: 'Result ID is required' });
  }
  
  try {
    // First get the specific result data
    const resultQuery = `
      SELECT 
        result_id,
        test_date,
        cmj_composite_score,
        CONCENTRIC_IMPULSE_Trial_Ns,
        ECCENTRIC_BRAKING_RFD_Trial_N_s,
        PEAK_CONCENTRIC_FORCE_Trial_N,
        BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg,
        RSI_MODIFIED_Trial_RSI_mod,
        ECCENTRIC_BRAKING_IMPULSE_Trial_Ns
      FROM \`${DATASET}.cmj_results\`
      WHERE athlete_name = @athleteName AND result_id = @resultId
      LIMIT 1
    `;
    
    const resultOptions = {
      query: resultQuery,
      params: { athleteName, resultId },
      location: 'US',
    };
    
    const [resultRows] = await bqClient.query(resultOptions);
    
    if (resultRows.length === 0) {
      return res.status(404).json({ error: 'Result not found' });
    }
    
    const resultData = resultRows[0];
    
    // Calculate percentiles for each metric
    const metrics = {};
    const metricColumns = [
      'cmj_composite_score',
      'CONCENTRIC_IMPULSE_Trial_Ns',
      'ECCENTRIC_BRAKING_RFD_Trial_N_s',
      'PEAK_CONCENTRIC_FORCE_Trial_N',
      'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg',
      'RSI_MODIFIED_Trial_RSI_mod',
      'ECCENTRIC_BRAKING_IMPULSE_Trial_Ns'
    ];
    
    for (const metric of metricColumns) {
      if (resultData[metric] !== null && resultData[metric] !== undefined) {
        // Calculate percentile for this metric
        const percentileQuery = `
          SELECT 
            COUNT(*) as total_count,
            SUM(CASE WHEN ${metric} <= @metricValue THEN 1 ELSE 0 END) as rank
          FROM \`${DATASET}.cmj_results\`
          WHERE ${metric} IS NOT NULL
        `;
        
        const percentileOptions = {
          query: percentileQuery,
          params: { metricValue: resultData[metric] },
          location: 'US',
        };
        
        const [percentileRows] = await bqClient.query(percentileOptions);
        const percentileData = percentileRows[0];
        
        const percentile = percentileData.total_count > 0 
          ? percentileData.rank / percentileData.total_count 
          : null;
        
        metrics[metric] = {
          value: resultData[metric],
          percentile: percentile
        };
      } else {
        metrics[metric] = {
          value: null,
          percentile: null
        };
      }
    }
    
    res.json({
      metrics: metrics,
      test_date: resultData.test_date
    });
    
  } catch (err) {
    console.error('Failed to fetch metrics:', err);
    res.status(500).json({ error: 'Failed to fetch metrics' });
  }
});

// NEW: Hybrid performance data endpoint - Live VALD + Analytics
app.get('/athletes/:athleteId/performance-data', async (req, res) => {
  const { athleteId } = req.params;
  const { resultId, testDate } = req.query;
  
  if (!athleteId || (!resultId && !testDate)) {
    return res.status(400).json({ error: 'Athlete ID and either result ID or test date required' });
  }
  
  try {
    console.log(`Getting hybrid performance data for athlete ${athleteId}, resultId: ${resultId}, testDate: ${testDate}`);
    
    // Step 1: Get live test data from VALD API
    const liveTestData = await valdAPI.getTestResults(athleteId, resultId || testDate);
    
    if (!liveTestData || !liveTestData.cmj) {
      return res.status(404).json({ error: 'No test data found' });
    }
    
    // Step 2: Get athlete profile
    const athleteProfile = await valdAPI.getAthleteProfile(athleteId);
    
    // Step 3: Calculate analytics from BigQuery for CMJ data
    const cmjMetrics = liveTestData.cmj.metrics;
    const [
      metricsWithPercentiles,
      compositeScore,
      databaseBenchmarks,
      athleteHistory
    ] = await Promise.all([
      analytics.calculateMultiplePercentiles(cmjMetrics, 'cmj'),
      analytics.calculateCompositeScore(cmjMetrics, 'cmj'),
      analytics.getDatabaseBenchmarks('cmj'),
      analytics.getAthleteHistory(athleteId, 'cmj', 5)
    ]);
    
    // Step 4: Combine live data with analytics
    const hybridResponse = {
      athlete: athleteProfile,
      testData: {
        ...liveTestData,
        cmj: {
          ...liveTestData.cmj,
          metrics: metricsWithPercentiles,
          compositeScore: compositeScore
        }
      },
      benchmarks: databaseBenchmarks,
      history: athleteHistory,
      source: 'hybrid',
      timestamp: new Date().toISOString()
    };
    
    console.log(`Successfully retrieved hybrid data for ${athleteProfile.athlete_name}`);
    res.json(hybridResponse);
    
  } catch (err) {
    console.error('Hybrid performance data error:', err);
    res.status(500).json({ 
      error: 'Failed to fetch performance data',
      details: err.message 
    });
  }
});

// Get database-wide averages for all CMJ metrics
app.get('/database-averages', async (req, res) => {
  try {
    const query = `
      SELECT
        AVG(cmj_composite_score) AS cmj_composite_score,
        AVG(CONCENTRIC_IMPULSE_Trial_Ns) AS CONCENTRIC_IMPULSE_Trial_Ns,
        AVG(ECCENTRIC_BRAKING_RFD_Trial_N_s) AS ECCENTRIC_BRAKING_RFD_Trial_N_s,
        AVG(PEAK_CONCENTRIC_FORCE_Trial_N) AS PEAK_CONCENTRIC_FORCE_Trial_N,
        AVG(BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg) AS BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg,
        AVG(RSI_MODIFIED_Trial_RSI_mod) AS RSI_MODIFIED_Trial_RSI_mod,
        AVG(ECCENTRIC_BRAKING_IMPULSE_Trial_Ns) AS ECCENTRIC_BRAKING_IMPULSE_Trial_Ns
      FROM \`${DATASET}.cmj_results\`
      WHERE cmj_composite_score IS NOT NULL
        AND CONCENTRIC_IMPULSE_Trial_Ns IS NOT NULL
        AND ECCENTRIC_BRAKING_RFD_Trial_N_s IS NOT NULL
        AND PEAK_CONCENTRIC_FORCE_Trial_N IS NOT NULL
        AND BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg IS NOT NULL
        AND RSI_MODIFIED_Trial_RSI_mod IS NOT NULL
        AND ECCENTRIC_BRAKING_IMPULSE_Trial_Ns IS NOT NULL
    `;
    const options = {
      query,
      location: 'US',
    };
    const [rows] = await bqClient.query(options);
    res.json(rows[0] || {});
  } catch (err) {
    console.error('Failed to fetch database averages:', err);
    res.status(500).json({ error: 'Failed to fetch database averages' });
  }
});

// Generate PDF report (now expects resultId instead of testDate)
app.post('/generate-report', async (req, res) => {
  const { athleteName, resultId } = req.body;
  
  if (!athleteName || !resultId) {
    return res.status(400).json({ error: 'Athlete name and result ID are required' });
  }

  try {
    console.log(`Generating report for athleteName ${athleteName} with resultId ${resultId}`);
    
    // Fetch athlete data from BigQuery using result_id and athlete_name
    const query = `
      SELECT 
        result_id,
        assessment_id,
        athlete_id,
        athlete_name,
        test_date,
        cmj_composite_score,
        CONCENTRIC_IMPULSE_Trial_Ns,
        ECCENTRIC_BRAKING_RFD_Trial_N_s,
        PEAK_CONCENTRIC_FORCE_Trial_N,
        BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg,
        RSI_MODIFIED_Trial_RSI_mod,
        ECCENTRIC_BRAKING_IMPULSE_Trial_Ns
      FROM \`${DATASET}.cmj_results\`
      WHERE athlete_name = @athleteName AND result_id = @resultId
      LIMIT 1
    `;
    
    const options = {
      query,
      params: { athleteName, resultId },
      location: 'US',
    };
    
    const [rows] = await bqClient.query(options);
    
    if (rows.length === 0) {
      return res.status(404).json({ error: 'No data found for this athlete and result ID' });
    }
    
    const athleteData = rows[0];
    console.log('Athlete data fetched:', athleteData);

    // Fetch true database-wide averages for comparison (with debugging)
    const avgQuery = `
      SELECT
        COUNT(*) as total_records,
        AVG(cmj_composite_score) AS cmj_composite_score,
        AVG(CONCENTRIC_IMPULSE_Trial_Ns) AS CONCENTRIC_IMPULSE_Trial_Ns,
        AVG(ECCENTRIC_BRAKING_RFD_Trial_N_s) AS ECCENTRIC_BRAKING_RFD_Trial_N_s,
        AVG(PEAK_CONCENTRIC_FORCE_Trial_N) AS PEAK_CONCENTRIC_FORCE_Trial_N,
        AVG(BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg) AS BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg,
        AVG(RSI_MODIFIED_Trial_RSI_mod) AS RSI_MODIFIED_Trial_RSI_mod,
        AVG(ECCENTRIC_BRAKING_IMPULSE_Trial_Ns) AS ECCENTRIC_BRAKING_IMPULSE_Trial_Ns,
        COUNT(DISTINCT athlete_name) as unique_athletes
      FROM \`${DATASET}.cmj_results\`
      WHERE cmj_composite_score IS NOT NULL
        AND CONCENTRIC_IMPULSE_Trial_Ns IS NOT NULL
        AND ECCENTRIC_BRAKING_RFD_Trial_N_s IS NOT NULL
        AND PEAK_CONCENTRIC_FORCE_Trial_N IS NOT NULL
        AND BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg IS NOT NULL
        AND RSI_MODIFIED_Trial_RSI_mod IS NOT NULL
        AND ECCENTRIC_BRAKING_IMPULSE_Trial_Ns IS NOT NULL
    `;
    const avgOptions = {
      query: avgQuery,
      location: 'US',
    };
    const [avgRows] = await bqClient.query(avgOptions);
    const averages = avgRows[0] || {};
    
    // Get max values for spider chart scaling
    const maxQuery = `
      SELECT
        MAX(cmj_composite_score) AS max_cmj_composite_score,
        MAX(CONCENTRIC_IMPULSE_Trial_Ns) AS max_CONCENTRIC_IMPULSE_Trial_Ns,
        MAX(ECCENTRIC_BRAKING_RFD_Trial_N_s) AS max_ECCENTRIC_BRAKING_RFD_Trial_N_s,
        MAX(PEAK_CONCENTRIC_FORCE_Trial_N) AS max_PEAK_CONCENTRIC_FORCE_Trial_N,
        MAX(BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg) AS max_BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg,
        MAX(RSI_MODIFIED_Trial_RSI_mod) AS max_RSI_MODIFIED_Trial_RSI_mod,
        MAX(ECCENTRIC_BRAKING_IMPULSE_Trial_Ns) AS max_ECCENTRIC_BRAKING_IMPULSE_Trial_Ns
      FROM \`${DATASET}.cmj_results\`
      WHERE cmj_composite_score IS NOT NULL
        AND CONCENTRIC_IMPULSE_Trial_Ns IS NOT NULL
        AND ECCENTRIC_BRAKING_RFD_Trial_N_s IS NOT NULL
        AND PEAK_CONCENTRIC_FORCE_Trial_N IS NOT NULL
        AND BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg IS NOT NULL
        AND RSI_MODIFIED_Trial_RSI_mod IS NOT NULL
        AND ECCENTRIC_BRAKING_IMPULSE_Trial_Ns IS NOT NULL
    `;
    const maxOptions = {
      query: maxQuery,
      location: 'US',
    };
    const [maxRows] = await bqClient.query(maxOptions);
    const maxValues = maxRows[0] || {};

    // Debug logging for averages and max values
    console.log('Database-wide averages query result:', averages);
    console.log('Database-wide max values:', maxValues);
    console.log('Total records in database:', averages.total_records);
    console.log('Unique athletes:', averages.unique_athletes);
    console.log('Composite score average:', averages.cmj_composite_score);
    console.log('Current athlete composite score:', athleteData.cmj_composite_score);
    
    // Debug logging for test date
    console.log('Raw test_date from BigQuery:', athleteData.test_date);
    console.log('Test date type:', typeof athleteData.test_date);
    
    // Properly format the test date
    let formattedTestDate = 'Unknown Date';
    if (athleteData.test_date) {
      if (athleteData.test_date.value) {
        // BigQuery datetime object
        formattedTestDate = new Date(athleteData.test_date.value).toLocaleDateString();
      } else if (typeof athleteData.test_date === 'string') {
        // String date
        formattedTestDate = new Date(athleteData.test_date).toLocaleDateString();
      } else {
        // Try converting to string first
        formattedTestDate = new Date(athleteData.test_date.toString()).toLocaleDateString();
      }
    }
    console.log('Formatted test date:', formattedTestDate);

    // Calculate percentiles for each metric
    const percentiles = {};
    const metricColumns = [
      'cmj_composite_score',
      'CONCENTRIC_IMPULSE_Trial_Ns',
      'ECCENTRIC_BRAKING_RFD_Trial_N_s',
      'PEAK_CONCENTRIC_FORCE_Trial_N',
      'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg',
      'RSI_MODIFIED_Trial_RSI_mod',
      'ECCENTRIC_BRAKING_IMPULSE_Trial_Ns'
    ];
    
    for (const metric of metricColumns) {
      if (athleteData[metric] !== null && athleteData[metric] !== undefined) {
        // Calculate percentile for this metric
        const percentileQuery = `
          SELECT 
            COUNT(*) as total_count,
            SUM(CASE WHEN ${metric} <= @metricValue THEN 1 ELSE 0 END) as rank
          FROM \`${DATASET}.cmj_results\`
          WHERE ${metric} IS NOT NULL
        `;
        
        const percentileOptions = {
          query: percentileQuery,
          params: { metricValue: athleteData[metric] },
          location: 'US',
        };
        
        const [percentileRows] = await bqClient.query(percentileOptions);
        const percentileData = percentileRows[0];
        
        const percentile = percentileData.total_count > 0 
          ? (percentileData.rank / percentileData.total_count * 100) 
          : 0;
        
        percentiles[metric] = Math.round(percentile);
      } else {
        percentiles[metric] = 0;
      }
    }
    
    console.log('Calculated percentiles:', percentiles);

    // Call Python script to generate PDF
    const pythonScript = path.join(__dirname, '../../Scripts/generate_report.py');
    const pythonProcess = spawn('python', [
      pythonScript,
      '--athlete-name', athleteData.athlete_name,
      '--test-date', formattedTestDate,
      '--composite-score', athleteData.cmj_composite_score?.toString() || '0',
      '--concentric-impulse', athleteData.CONCENTRIC_IMPULSE_Trial_Ns?.toString() || '0',
      '--eccentric-rfd', athleteData.ECCENTRIC_BRAKING_RFD_Trial_N_s?.toString() || '0',
      '--peak-force', athleteData.PEAK_CONCENTRIC_FORCE_Trial_N?.toString() || '0',
      '--takeoff-power', athleteData.BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg?.toString() || '0',
      '--rsi-modified', athleteData.RSI_MODIFIED_Trial_RSI_mod?.toString() || '0',
      '--eccentric-impulse', athleteData.ECCENTRIC_BRAKING_IMPULSE_Trial_Ns?.toString() || '0',
      '--avg-composite-score', averages.cmj_composite_score?.toString() || '0',
      '--avg-concentric-impulse', averages.CONCENTRIC_IMPULSE_Trial_Ns?.toString() || '0',
      '--avg-eccentric-rfd', averages.ECCENTRIC_BRAKING_RFD_Trial_N_s?.toString() || '0',
      '--avg-peak-force', averages.PEAK_CONCENTRIC_FORCE_Trial_N?.toString() || '0',
      '--avg-takeoff-power', averages.BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg?.toString() || '0',
      '--avg-rsi-modified', averages.RSI_MODIFIED_Trial_RSI_mod?.toString() || '0',
      '--avg-eccentric-impulse', averages.ECCENTRIC_BRAKING_IMPULSE_Trial_Ns?.toString() || '0',
      '--max-composite-score', maxValues.max_cmj_composite_score?.toString() || '100',
      '--max-concentric-impulse', maxValues.max_CONCENTRIC_IMPULSE_Trial_Ns?.toString() || '500',
      '--max-eccentric-rfd', maxValues.max_ECCENTRIC_BRAKING_RFD_Trial_N_s?.toString() || '10000',
      '--max-peak-force', maxValues.max_PEAK_CONCENTRIC_FORCE_Trial_N?.toString() || '5000',
      '--max-takeoff-power', maxValues.max_BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg?.toString() || '50',
      '--max-rsi-modified', maxValues.max_RSI_MODIFIED_Trial_RSI_mod?.toString() || '2.5',
      '--max-eccentric-impulse', maxValues.max_ECCENTRIC_BRAKING_IMPULSE_Trial_Ns?.toString() || '200',
      '--percentile-composite-score', percentiles.cmj_composite_score?.toString() || '0',
      '--percentile-concentric-impulse', percentiles.CONCENTRIC_IMPULSE_Trial_Ns?.toString() || '0',
      '--percentile-eccentric-rfd', percentiles.ECCENTRIC_BRAKING_RFD_Trial_N_s?.toString() || '0',
      '--percentile-peak-force', percentiles.PEAK_CONCENTRIC_FORCE_Trial_N?.toString() || '0',
      '--percentile-takeoff-power', percentiles.BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg?.toString() || '0',
      '--percentile-rsi-modified', percentiles.RSI_MODIFIED_Trial_RSI_mod?.toString() || '0',
      '--percentile-eccentric-impulse', percentiles.ECCENTRIC_BRAKING_IMPULSE_Trial_Ns?.toString() || '0'
    ]);
    
    let pdfBuffer = Buffer.alloc(0);
    let errorOutput = '';
    
    pythonProcess.stdout.on('data', (data) => {
      pdfBuffer = Buffer.concat([pdfBuffer, data]);
    });
    
    pythonProcess.stderr.on('data', (data) => {
      errorOutput += data.toString();
      console.error('Python script error:', data.toString());
    });
    
    pythonProcess.on('close', (code) => {
      if (code === 0 && pdfBuffer.length > 0) {
        console.log('PDF generated successfully, size:', pdfBuffer.length);
        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', `attachment; filename="${athleteData.athlete_name}_${athleteData.test_date}_report.pdf"`);
        res.send(pdfBuffer);
      } else {
        console.error('Python script failed with code:', code);
        console.error('Error output:', errorOutput);
        res.status(500).json({ 
          error: 'Failed to generate PDF report',
          details: errorOutput
        });
      }
    });
    
    pythonProcess.on('error', (err) => {
      console.error('Failed to start Python script:', err);
      res.status(500).json({ error: 'Failed to start report generation' });
    });
    
  } catch (err) {
    console.error('Error generating report:', err);
    res.status(500).json({ error: 'Failed to generate report' });
  }
});

// NEW: Hybrid PDF report generation - Live VALD data + BigQuery analytics
app.post('/generate-hybrid-report', async (req, res) => {
  const { athleteId, resultId, testDate } = req.body;
  
  if (!athleteId || (!resultId && !testDate)) {
    return res.status(400).json({ error: 'Athlete ID and either result ID or test date are required' });
  }

  try {
    console.log(`Generating hybrid report for athlete ${athleteId}, resultId: ${resultId}, testDate: ${testDate}`);
    
    // Step 1: Get complete hybrid performance data
    const hybridData = await getHybridPerformanceData(athleteId, resultId || testDate);
    
    if (!hybridData || !hybridData.testData.cmj) {
      return res.status(404).json({ error: 'No test data found for report generation' });
    }
    
    const { athlete, testData, benchmarks } = hybridData;
    const cmjData = testData.cmj;
    
    // Step 2: Format test date
    let formattedTestDate = 'Unknown Date';
    if (cmjData.test_date) {
      formattedTestDate = new Date(cmjData.test_date).toLocaleDateString();
    }
    
    // Step 3: Extract metrics and percentiles
    const metrics = cmjData.metrics;
    
    // Step 4: Call Python script with hybrid data
    const pythonScript = path.join(__dirname, '../../Scripts/generate_report.py');
    const pythonArgs = [
      '--athlete-name', athlete.athlete_name,
      '--test-date', formattedTestDate,
      '--composite-score', cmjData.compositeScore?.toString() || '0',
      '--concentric-impulse', metrics.CONCENTRIC_IMPULSE_Trial_Ns?.value?.toString() || '0',
      '--eccentric-rfd', metrics.ECCENTRIC_BRAKING_RFD_Trial_N_s?.value?.toString() || '0',
      '--peak-force', metrics.PEAK_CONCENTRIC_FORCE_Trial_N?.value?.toString() || '0',
      '--takeoff-power', metrics.BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg?.value?.toString() || '0',
      '--rsi-modified', metrics.RSI_MODIFIED_Trial_RSI_mod?.value?.toString() || '0',
      '--eccentric-impulse', metrics.ECCENTRIC_BRAKING_IMPULSE_Trial_Ns?.value?.toString() || '0',
      '--avg-composite-score', benchmarks.cmj_composite_score?.average?.toString() || '0',
      '--avg-concentric-impulse', benchmarks.CONCENTRIC_IMPULSE_Trial_Ns?.average?.toString() || '0',
      '--avg-eccentric-rfd', benchmarks.ECCENTRIC_BRAKING_RFD_Trial_N_s?.average?.toString() || '0',
      '--avg-peak-force', benchmarks.PEAK_CONCENTRIC_FORCE_Trial_N?.average?.toString() || '0',
      '--avg-takeoff-power', benchmarks.BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg?.average?.toString() || '0',
      '--avg-rsi-modified', benchmarks.RSI_MODIFIED_Trial_RSI_mod?.average?.toString() || '0',
      '--avg-eccentric-impulse', benchmarks.ECCENTRIC_BRAKING_IMPULSE_Trial_Ns?.average?.toString() || '0',
      '--max-composite-score', '100', // Will be calculated from benchmarks
      '--max-concentric-impulse', benchmarks.CONCENTRIC_IMPULSE_Trial_Ns?.maximum?.toString() || '500',
      '--max-eccentric-rfd', benchmarks.ECCENTRIC_BRAKING_RFD_Trial_N_s?.maximum?.toString() || '10000',
      '--max-peak-force', benchmarks.PEAK_CONCENTRIC_FORCE_Trial_N?.maximum?.toString() || '5000',
      '--max-takeoff-power', benchmarks.BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg?.maximum?.toString() || '50',
      '--max-rsi-modified', benchmarks.RSI_MODIFIED_Trial_RSI_mod?.maximum?.toString() || '2.5',
      '--max-eccentric-impulse', benchmarks.ECCENTRIC_BRAKING_IMPULSE_Trial_Ns?.maximum?.toString() || '200',
      '--percentile-composite-score', cmjData.compositeScore?.toString() || '0',
      '--percentile-concentric-impulse', metrics.CONCENTRIC_IMPULSE_Trial_Ns?.percentile?.toString() || '0',
      '--percentile-eccentric-rfd', metrics.ECCENTRIC_BRAKING_RFD_Trial_N_s?.percentile?.toString() || '0',
      '--percentile-peak-force', metrics.PEAK_CONCENTRIC_FORCE_Trial_N?.percentile?.toString() || '0',
      '--percentile-takeoff-power', metrics.BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg?.percentile?.toString() || '0',
      '--percentile-rsi-modified', metrics.RSI_MODIFIED_Trial_RSI_mod?.percentile?.toString() || '0',
      '--percentile-eccentric-impulse', metrics.ECCENTRIC_BRAKING_IMPULSE_Trial_Ns?.percentile?.toString() || '0'
    ];
    
    const pythonProcess = spawn('python', [pythonScript, ...pythonArgs]);
    
    let pdfBuffer = Buffer.alloc(0);
    let errorOutput = '';
    
    pythonProcess.stdout.on('data', (data) => {
      pdfBuffer = Buffer.concat([pdfBuffer, data]);
    });
    
    pythonProcess.stderr.on('data', (data) => {
      errorOutput += data.toString();
      console.error('Python script error:', data.toString());
    });
    
    pythonProcess.on('close', (code) => {
      if (code === 0 && pdfBuffer.length > 0) {
        console.log('Hybrid PDF generated successfully, size:', pdfBuffer.length);
        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', `attachment; filename="${athlete.athlete_name}_${formattedTestDate}_hybrid_report.pdf"`);
        res.send(pdfBuffer);
      } else {
        console.error('Python script failed with code:', code);
        console.error('Error output:', errorOutput);
        res.status(500).json({ 
          error: 'Failed to generate hybrid PDF report',
          details: errorOutput
        });
      }
    });
    
    pythonProcess.on('error', (err) => {
      console.error('Failed to start Python script:', err);
      res.status(500).json({ error: 'Failed to start hybrid report generation' });
    });
    
  } catch (err) {
    console.error('Error generating hybrid report:', err);
    res.status(500).json({ error: 'Failed to generate hybrid report' });
  }
});

// Helper function for hybrid performance data
async function getHybridPerformanceData(athleteId, identifier) {
  // Get live test data from VALD API
  const liveTestData = await valdAPI.getTestResults(athleteId, identifier);
  
  if (!liveTestData || !liveTestData.cmj) {
    throw new Error('No test data found');
  }
  
  // Get athlete profile
  const athleteProfile = await valdAPI.getAthleteProfile(athleteId);
  
  // Calculate analytics from BigQuery for CMJ data
  const cmjMetrics = liveTestData.cmj.metrics;
  const [
    metricsWithPercentiles,
    compositeScore,
    databaseBenchmarks
  ] = await Promise.all([
    analytics.calculateMultiplePercentiles(cmjMetrics, 'cmj'),
    analytics.calculateCompositeScore(cmjMetrics, 'cmj'),
    analytics.getDatabaseBenchmarks('cmj')
  ]);
  
  return {
    athlete: athleteProfile,
    testData: {
      ...liveTestData,
      cmj: {
        ...liveTestData.cmj,
        metrics: metricsWithPercentiles,
        compositeScore: compositeScore
      }
    },
    benchmarks: databaseBenchmarks
  };
}

app.listen(PORT, () => {
  console.log(`Backend API listening on port ${PORT}`);
}); 