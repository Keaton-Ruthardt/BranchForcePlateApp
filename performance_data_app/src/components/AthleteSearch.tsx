import React, { useState } from 'react';
import { TextField, List, ListItem, ListItemButton, ListItemText, CircularProgress, Paper, Button, Box, Typography, MenuItem } from '@mui/material';
import { DatePicker } from '@mui/x-date-pickers/DatePicker';
import { LocalizationProvider } from '@mui/x-date-pickers/LocalizationProvider';
import { AdapterDateFns } from '@mui/x-date-pickers/AdapterDateFns';
import axios from 'axios';

interface Athlete {
  athlete_id: string;
  athlete_name: string;
  source: 'live' | 'cached';
  sport?: string;
  position?: string;
  error?: string;
}

interface AthleteSearchProps {
  onSelect: (athleteName: string, athleteId: string) => void;
}

const AthleteSearch: React.FC<AthleteSearchProps> = ({ onSelect }) => {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<Athlete[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [selectedAthlete, setSelectedAthlete] = useState<Athlete | null>(null);
  const [selectedTest, setSelectedTest] = useState<{ result_id: string; test_date: string; source?: string } | null>(null);
  const [generatingReport, setGeneratingReport] = useState(false);

  const handleSearch = async (e: React.ChangeEvent<HTMLInputElement>) => {
    setQuery(e.target.value);
    if (e.target.value.length < 2) {
      setResults([]);
      return;
    }
    setLoading(true);
    setError('');
    try {
      console.log('Searching for:', e.target.value);
      const res = await axios.get(`/athletes?search=${encodeURIComponent(e.target.value)}`);
      console.log('Received hybrid athlete data:', res.data);
      setResults(res.data);
    } catch (err) {
      console.error('Failed to fetch athletes:', err);
      setError('Failed to fetch athletes');
    } finally {
      setLoading(false);
    }
  };

  const handleAthleteSelect = (athlete: Athlete) => {
    setSelectedAthlete(athlete);
    onSelect(athlete.athlete_name, athlete.athlete_id);
    setSelectedTest(null); // Reset selected test when athlete changes
  };

  // Fetch all tests for the selected athlete using hybrid endpoint
  React.useEffect(() => {
    if (!selectedAthlete) return;
    setLoading(true);
    setError('');
    axios.get(`/athletes/${selectedAthlete.athlete_id}/test-dates`)
      .then((res) => {
        console.log('Received test dates:', res.data);
        setTestList(res.data);
      })
      .catch((err) => {
        console.error('Failed to fetch test dates:', err);
        setError('Failed to fetch test dates');
      })
      .finally(() => setLoading(false));
  }, [selectedAthlete]);

  // New state for test list
  const [testList, setTestList] = useState<{ result_id: string; test_date: string; source?: string }[]>([]);

  const handleGenerateReport = async () => {
    if (!selectedAthlete || !selectedTest) {
      setError('Please select both an athlete and a test');
      return;
    }
    setGeneratingReport(true);
    setError('');
    try {
      console.log('Generating hybrid report for:', selectedAthlete.athlete_name, selectedTest.result_id);
      const response = await axios.post('/generate-hybrid-report', {
        athleteId: selectedAthlete.athlete_id,
        resultId: selectedTest.result_id,
        testDate: selectedTest.test_date
      }, {
        responseType: 'blob'
      });
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', `${selectedAthlete.athlete_name}_${selectedTest.test_date}_hybrid_report.pdf`);
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
    } catch (err) {
      console.error('Failed to generate hybrid report:', err);
      setError('Failed to generate hybrid report');
    } finally {
      setGeneratingReport(false);
    }
  };

  return (
    <Paper 
      elevation={2}
      sx={{ 
        p: 3, 
        mb: 3, 
        backgroundColor: 'white',
        border: '1px solid #e0e0e0'
      }}
    >
      <Typography 
        variant="h5" 
        gutterBottom
        sx={{ 
          color: '#1976d2',
          fontWeight: 600,
          mb: 3
        }}
      >
        Athlete Search & Report Generation
      </Typography>
      <TextField
        label="Search Athlete"
        variant="outlined"
        fullWidth
        value={query}
        onChange={handleSearch}
        autoComplete="off"
        sx={{ mb: 2 }}
      />
      {loading && <CircularProgress size={24} sx={{ mb: 2 }} />}
      {error && <Typography color="error" sx={{ mb: 2 }}>{error}</Typography>}
      <List sx={{ mb: 2, maxHeight: 200, overflow: 'auto' }}>
        {results.map((athlete) => (
          <ListItem key={athlete.athlete_id} disablePadding>
            <ListItemButton onClick={() => handleAthleteSelect(athlete)}>
              <ListItemText 
                primary={athlete.athlete_name}
                secondary={
                  <span>
                    {athlete.sport && athlete.position ? `${athlete.sport} - ${athlete.position}` : athlete.athlete_id}
                    {athlete.source === 'live' ? ' 🔴 Live' : ' 💾 Cached'}
                    {athlete.error && ` ⚠️ ${athlete.error}`}
                  </span>
                }
              />
            </ListItemButton>
          </ListItem>
        ))}
      </List>
      {selectedAthlete && (
        <Box sx={{ mb: 2 }}>
          <Typography variant="subtitle1" gutterBottom>
            Selected Athlete: {selectedAthlete.athlete_name}
            {selectedAthlete.source === 'live' ? ' 🔴 Live Data' : ' 💾 Cached Data'}
          </Typography>
          {selectedAthlete.sport && selectedAthlete.position && (
            <Typography variant="body2" color="text.secondary">
              {selectedAthlete.sport} - {selectedAthlete.position}
            </Typography>
          )}
        </Box>
      )}
      {/* Dropdown for all available tests for the selected athlete */}
      {selectedAthlete && testList.length > 0 && (
        <TextField
          select
          label="Select Test"
          value={selectedTest ? selectedTest.result_id : ''}
          onChange={(e) => {
            const test = testList.find(t => t.result_id === e.target.value);
            setSelectedTest(test || null);
          }}
          fullWidth
          sx={{ mb: 2 }}
        >
          {testList.map((test) => {
            let dateLabel = 'Unknown Date';
            if (test.test_date && !isNaN(Date.parse(test.test_date))) {
              dateLabel = new Date(test.test_date).toLocaleDateString();
            }
            const dataSource = test.source === 'live' ? ' 🔴' : ' 💾';
            return (
              <MenuItem key={test.result_id} value={test.result_id}>
                {dateLabel} ({test.result_id.slice(-6)}){dataSource}
              </MenuItem>
            );
          })}
        </TextField>
      )}
      <Button
        variant="contained"
        onClick={handleGenerateReport}
        disabled={!selectedAthlete || !selectedTest || generatingReport}
        fullWidth
        sx={{ 
          backgroundColor: selectedAthlete?.source === 'live' ? '#1976d2' : '#666',
          '&:hover': {
            backgroundColor: selectedAthlete?.source === 'live' ? '#1565c0' : '#555'
          }
        }}
      >
        {generatingReport ? (
          <CircularProgress size={24} />
        ) : (
          `Generate ${selectedAthlete?.source === 'live' ? 'Live' : 'Cached'} Report`
        )}
      </Button>
    </Paper>
  );
};

export default AthleteSearch; 