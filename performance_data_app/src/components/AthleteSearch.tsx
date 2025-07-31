import React, { useState } from 'react';
import { TextField, List, ListItem, ListItemButton, ListItemText, CircularProgress, Paper, Button, Box, Typography, MenuItem } from '@mui/material';
import { DatePicker } from '@mui/x-date-pickers/DatePicker';
import { LocalizationProvider } from '@mui/x-date-pickers/LocalizationProvider';
import { AdapterDateFns } from '@mui/x-date-pickers/AdapterDateFns';
import axios from 'axios';

interface AthleteSearchProps {
  onSelect: (athleteName: string) => void;
}

const AthleteSearch: React.FC<AthleteSearchProps> = ({ onSelect }) => {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [selectedAthlete, setSelectedAthlete] = useState<string>('');
  const [selectedTest, setSelectedTest] = useState<{ result_id: string; test_date: string } | null>(null);
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
      console.log('Searching for:', e.target.value); // Debug log
      const res = await axios.get(`/athletes?search=${encodeURIComponent(e.target.value)}`);
      console.log('Received response:', res.data); // Debug log
      setResults(res.data.map((row: any) => row.athlete_name));
    } catch (err) {
      console.error('Failed to fetch athletes:', err); // Debug log
      setError('Failed to fetch athletes');
    } finally {
      setLoading(false);
    }
  };

  const handleAthleteSelect = (athleteName: string) => {
    setSelectedAthlete(athleteName);
    onSelect(athleteName);
    setSelectedTest(null); // Reset selected test when athlete changes
  };

  // Fetch all tests for the selected athlete
  React.useEffect(() => {
    if (!selectedAthlete) return;
    setLoading(true);
    setError('');
    axios.get(`/athletes/${encodeURIComponent(selectedAthlete)}/results`)
      .then((res) => {
        setResults(res.data.map((row: any) => row.athlete_name)); // Keep for search
        // Extract result_id and test_date for dropdown
        setTestList(res.data.map((row: any) => ({
          result_id: row.result_id,
          test_date: row.test_date
        })));
      })
      .catch(() => setError('Failed to fetch tests'))
      .finally(() => setLoading(false));
  }, [selectedAthlete]);

  // New state for test list
  const [testList, setTestList] = useState<{ result_id: string; test_date: string }[]>([]);

  const handleGenerateReport = async () => {
    if (!selectedAthlete || !selectedTest) {
      setError('Please select both an athlete and a test');
      return;
    }
    setGeneratingReport(true);
    setError('');
    try {
      const response = await axios.post('/generate-report', {
        athleteName: selectedAthlete,
        resultId: selectedTest.result_id // Use result_id instead of test_date
      }, {
        responseType: 'blob'
      });
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', `${selectedAthlete}_${selectedTest.test_date}_report.pdf`);
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
    } catch (err) {
      console.error('Failed to generate report:', err);
      setError('Failed to generate report');
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
        {results.map((name) => (
          <ListItem key={name} disablePadding>
            <ListItemButton onClick={() => handleAthleteSelect(name)}>
              <ListItemText primary={name} />
            </ListItemButton>
          </ListItem>
        ))}
      </List>
      {selectedAthlete && (
        <Box sx={{ mb: 2 }}>
          <Typography variant="subtitle1" gutterBottom>
            Selected Athlete: {selectedAthlete}
          </Typography>
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
            return (
              <MenuItem key={test.result_id} value={test.result_id}>
                {dateLabel} ({test.result_id.slice(-6)})
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
      >
        {generatingReport ? <CircularProgress size={24} /> : 'Generate Report'}
      </Button>
    </Paper>
  );
};

export default AthleteSearch; 