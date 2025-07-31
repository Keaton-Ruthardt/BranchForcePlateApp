import React, { useState } from 'react';
import { AppBar, Toolbar, Typography, Container, Box, CssBaseline, ThemeProvider, createTheme } from '@mui/material';
import AthleteSearch from './components/AthleteSearch';
import AthleteResults from './components/AthleteResults';

// Light corporate theme
const theme = createTheme({
  palette: {
    mode: 'light',
    primary: {
      main: '#1976d2',
    },
    secondary: {
      main: '#ff6f00',
    },
    background: {
      default: '#fafafa',
      paper: '#ffffff',
    },
    text: {
      primary: '#424242',
      secondary: '#757575',
    },
  },
  typography: {
    fontFamily: '"Helvetica Neue", Helvetica, Arial, sans-serif',
    h6: {
      fontWeight: 600,
    },
  },
});

const App: React.FC = () => {
  const [selectedAthlete, setSelectedAthlete] = useState<string | null>(null);

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box sx={{ backgroundColor: '#fafafa', minHeight: '100vh' }}>
        <AppBar position="static" elevation={1} sx={{ backgroundColor: 'white', color: '#424242' }}>
          <Toolbar>
            <Typography 
              variant="h6" 
              component="div" 
              sx={{ 
                flexGrow: 1,
                color: '#1976d2',
                fontWeight: 700,
                letterSpacing: '0.05em'
              }}
            >
              VALD Performance Analytics
            </Typography>
          </Toolbar>
        </AppBar>
        
        <Container maxWidth="xl" sx={{ pt: 3, pb: 4 }}>
          <AthleteSearch onSelect={setSelectedAthlete} />
          {selectedAthlete && <AthleteResults athleteName={selectedAthlete} />}
        </Container>
      </Box>
    </ThemeProvider>
  );
};

export default App; 