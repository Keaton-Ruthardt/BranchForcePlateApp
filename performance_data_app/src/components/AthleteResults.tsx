import React, { useEffect, useState } from 'react';
import { 
  Paper, 
  Typography, 
  Box, 
  CircularProgress, 
  Select, 
  MenuItem, 
  FormControl, 
  InputLabel,
  Grid,
  Card,
  CardContent,
  Divider
} from '@mui/material';
import { Radar, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis, ResponsiveContainer, Legend } from 'recharts';
import axios from 'axios';

interface AthleteResultsProps {
  athleteName: string;
}

interface MetricInfo {
  value: number;
  percentile: number | null;
}

interface RadarData {
  metric: string;
  testValue: number;
  databaseAverage: number;
}

const metricLabels: Record<string, string> = {
  cmj_composite_score: 'Composite Score',
  CONCENTRIC_IMPULSE_Trial_Ns: 'Concentric Impulse',
  ECCENTRIC_BRAKING_RFD_Trial_N_s: 'Eccentric Braking RFD',
  PEAK_CONCENTRIC_FORCE_Trial_N: 'Peak Concentric Force', 
  BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg: 'BM Rel. Takeoff Power',
  RSI_MODIFIED_Trial_RSI_mod: 'RSI Modified',
  ECCENTRIC_BRAKING_IMPULSE_Trial_Ns: 'Eccentric Braking Impulse',
};

const AthleteResults: React.FC<AthleteResultsProps> = ({ athleteName }) => {
  const [metrics, setMetrics] = useState<Record<string, MetricInfo> | null>(null);
  const [databaseAverages, setDatabaseAverages] = useState<Record<string, number> | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [testDates, setTestDates] = useState<{ result_id: string; test_date: string }[]>([]);
  const [selectedResultId, setSelectedResultId] = useState<string | null>(null);
  const [selectedTestDate, setSelectedTestDate] = useState<string | null>(null);
  const [datesLoading, setDatesLoading] = useState(false);

  // Fetch all test dates for the athlete
  useEffect(() => {
    if (!athleteName) return;
    setDatesLoading(true);
    axios
      .get(`/athletes/${encodeURIComponent(athleteName)}/cmj-test-dates`)
      .then((res) => {
        setTestDates(res.data);
        if (res.data.length > 0) {
          setSelectedResultId(res.data[0].result_id);
          setSelectedTestDate(res.data[0].test_date);
        }
      })
      .catch(() => setError('Failed to fetch test dates'))
      .finally(() => setDatesLoading(false));
  }, [athleteName]);

  // Fetch database averages once
  useEffect(() => {
    const fetchDatabaseAverages = async () => {
      try {
        const response = await axios.get('/database-averages');
        setDatabaseAverages(response.data);
      } catch (err) {
        console.error('Failed to fetch database averages:', err);
      }
    };
    fetchDatabaseAverages();
  }, []);

  // Fetch metrics for the selected test
  useEffect(() => {
    if (!athleteName || !selectedResultId) return;
    setLoading(true);
    setError('');
    axios
      .get(`/athletes/${encodeURIComponent(athleteName)}/cmj-metric-percentiles?resultId=${encodeURIComponent(selectedResultId)}`)
      .then((res) => {
        setMetrics(res.data.metrics);
        setSelectedTestDate(res.data.test_date);
      })
      .catch(() => setError('Failed to fetch results'))
      .finally(() => setLoading(false));
  }, [athleteName, selectedResultId]);

  // Create radar chart data
  const createRadarData = (): RadarData[] => {
    if (!metrics || !databaseAverages) return [];
    
    const radarMetrics = [
      'CONCENTRIC_IMPULSE_Trial_Ns',
      'ECCENTRIC_BRAKING_RFD_Trial_N_s', 
      'PEAK_CONCENTRIC_FORCE_Trial_N',
      'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg',
      'RSI_MODIFIED_Trial_RSI_mod',
      'ECCENTRIC_BRAKING_IMPULSE_Trial_Ns'
    ];

    return radarMetrics.map(metric => {
      const testValue = metrics[metric]?.value || 0;
      const dbAverage = databaseAverages[metric] || 0;
      
      // Normalize to 0-100 percentile scale
      const testPercentile = (metrics[metric]?.percentile || 0) * 100;
      const avgPercentile = 50; // Database average should be around 50th percentile
      
      return {
        metric: metricLabels[metric] || metric,
        testValue: testPercentile,
        databaseAverage: avgPercentile
      };
    });
  };

  if (!athleteName) return null;

  if (datesLoading || loading) {
    return (
      <Box 
        sx={{ 
          display: 'flex', 
          justifyContent: 'center', 
          alignItems: 'center', 
          minHeight: '60vh',
          backgroundColor: '#fafafa'
        }}
      >
        <CircularProgress size={60} sx={{ color: '#1976d2' }} />
      </Box>
    );
  }

  if (error) {
    return (
      <Paper sx={{ p: 4, mt: 4, backgroundColor: 'white', textAlign: 'center' }}>
        <Typography color="error" variant="h6">{error}</Typography>
      </Paper>
    );
  }

  if (!metrics) {
    return (
      <Paper sx={{ p: 4, mt: 4, backgroundColor: 'white', textAlign: 'center' }}>
        <Typography variant="h6" color="text.secondary">No results found.</Typography>
      </Paper>
    );
  }

  const radarData = createRadarData();
  const compositeScore = metrics.cmj_composite_score?.value || 0;

  return (
    <Box sx={{ backgroundColor: '#fafafa', minHeight: '100vh', pt: 2 }}>
      {/* Test Date Selector */}
      {testDates.length > 0 && (
        <Box mb={3} display="flex" justifyContent="center">
          <FormControl variant="outlined" size="small" sx={{ minWidth: 250, backgroundColor: 'white' }}>
            <InputLabel>Test Date</InputLabel>
            <Select
              value={selectedResultId || ''}
              label="Test Date"
              onChange={(e) => setSelectedResultId(e.target.value as string)}
            >
              {testDates.map((d) => {
                let dateLabel = 'Unknown Date';
                if (d.test_date && !isNaN(Date.parse(d.test_date))) {
                  dateLabel = new Date(d.test_date).toLocaleDateString();
                }
                return (
                  <MenuItem key={d.result_id} value={d.result_id}>
                    {dateLabel} ({d.result_id.slice(-6)})
                  </MenuItem>
                );
              })}
            </Select>
          </FormControl>
        </Box>
      )}

      {/* Main 3-Section Layout */}
      <Box sx={{ maxWidth: 1200, mx: 'auto', px: 3 }}>
        
        {/* TOP SECTION - 20% Height: Composite Score */}
        <Paper 
          elevation={2}
          sx={{ 
            height: '20vh', 
            mb: 3, 
            backgroundColor: 'white',
            display: 'flex',
            flexDirection: 'column',
            justifyContent: 'center',
            alignItems: 'center',
            border: '1px solid #e0e0e0'
          }}
        >
          <Typography 
            variant="h3" 
            sx={{ 
              fontSize: '4rem', 
              fontWeight: 700, 
              color: '#424242', 
              mb: 1,
              fontFamily: '"Helvetica Neue", Helvetica, Arial, sans-serif'
            }}
          >
            {compositeScore.toFixed(1)}
          </Typography>
          <Typography 
            variant="h5" 
            sx={{ 
              color: '#1976d2', 
              fontWeight: 600,
              textTransform: 'uppercase',
              letterSpacing: '0.1em'
            }}
          >
            CMJ Composite Score
          </Typography>
        </Paper>

        {/* MIDDLE SECTION - 60% Height: Metrics Grid */}
        <Paper 
          elevation={2}
          sx={{ 
            height: '60vh', 
            mb: 3, 
            backgroundColor: 'white',
            p: 3,
            border: '1px solid #e0e0e0',
            overflow: 'auto'
          }}
        >
          <Grid container spacing={3} sx={{ height: '100%' }}>
            {Object.entries(metrics)
              .filter(([key]) => key !== 'cmj_composite_score')
              .map(([key, info]) => (
                <Grid item xs={12} sm={6} md={4} key={key}>
                  <Card 
                    elevation={0}
                    sx={{ 
                      height: '100%',
                      border: '1px solid #e0e0e0',
                      backgroundColor: '#fafafa',
                      '&:hover': {
                        backgroundColor: '#f5f5f5',
                        borderColor: '#1976d2'
                      }
                    }}
                  >
                    <CardContent sx={{ textAlign: 'center', py: 3 }}>
                      <Typography 
                        variant="subtitle1" 
                        sx={{ 
                          color: '#424242',
                          fontWeight: 600,
                          mb: 2,
                          textTransform: 'uppercase',
                          fontSize: '0.875rem',
                          letterSpacing: '0.05em'
                        }}
                      >
                        {metricLabels[key] || key}
                      </Typography>
                      
                      <Typography 
                        variant="h4" 
                        sx={{ 
                          color: '#1976d2',
                          fontWeight: 700,
                          mb: 1,
                          fontFamily: '"Helvetica Neue", Helvetica, Arial, sans-serif'
                        }}
                      >
                        {typeof info.value === 'number' && !isNaN(info.value) 
                          ? info.value.toFixed(2) 
                          : 'N/A'
                        }
                      </Typography>
                      
                      <Divider sx={{ my: 1, backgroundColor: '#e0e0e0' }} />
                      
                      <Typography 
                        variant="body2" 
                        sx={{ 
                          color: '#757575',
                          fontWeight: 500
                        }}
                      >
                        {typeof info.percentile === 'number' && !isNaN(info.percentile) 
                          ? `${Math.round(info.percentile * 100)}th Percentile` 
                          : 'Percentile N/A'
                        }
                      </Typography>
                    </CardContent>
                  </Card>
                </Grid>
              ))}
          </Grid>
        </Paper>

        {/* BOTTOM SECTION - 20% Height: Spider Chart */}
        <Paper 
          elevation={2}
          sx={{ 
            height: '20vh', 
            backgroundColor: 'white',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            border: '1px solid #e0e0e0',
            p: 2
          }}
        >
          <ResponsiveContainer width="100%" height="100%">
            <RadarChart data={radarData} margin={{ top: 10, right: 10, bottom: 10, left: 10 }}>
              <PolarGrid stroke="#e0e0e0" />
              <PolarAngleAxis 
                dataKey="metric" 
                tick={{ 
                  fontSize: 10, 
                  fill: '#424242',
                  fontWeight: 600
                }} 
              />
              <PolarRadiusAxis 
                domain={[0, 100]} 
                tick={{ fontSize: 8, fill: '#757575' }}
                tickCount={3}
                angle={-90}
              />
              <Radar
                name="Test Result"
                dataKey="testValue"
                stroke="#1976d2"
                fill="#1976d2"
                fillOpacity={0.3}
                strokeWidth={3}
              />
              <Radar
                name="Database Average"
                dataKey="databaseAverage"
                stroke="#ff6f00"
                fill="#ff6f00"
                fillOpacity={0.1}
                strokeWidth={2}
                strokeDasharray="5 5"
              />
              <Legend 
                wrapperStyle={{ 
                  fontSize: '12px',
                  fontWeight: 600,
                  color: '#424242'
                }}
              />
            </RadarChart>
          </ResponsiveContainer>
        </Paper>
      </Box>
    </Box>
  );
};

export default AthleteResults;