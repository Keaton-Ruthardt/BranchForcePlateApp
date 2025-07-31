const axios = require('axios');
const path = require('path');
require('dotenv').config();

// Import existing VALD API helpers
const { spawn } = require('child_process');

class VALDAPIService {
  constructor() {
    this.baseURL = process.env.FORCEDECKS_URL;
    this.clientId = process.env.CLIENT_ID;
    this.clientSecret = process.env.CLIENT_SECRET;
    this.authURL = process.env.AUTH_URL;
    this.tenantId = process.env.TENANT_ID;
    this.tokenCache = null;
    this.tokenExpiry = null;
  }

  async getAccessToken() {
    // Check if token is still valid
    if (this.tokenCache && this.tokenExpiry && Date.now() < this.tokenExpiry) {
      return this.tokenCache;
    }

    try {
      const response = await axios.post(this.authURL, {
        grant_type: 'client_credentials',
        client_id: this.clientId,
        client_secret: this.clientSecret
      }, {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded'
        }
      });

      this.tokenCache = response.data.access_token;
      const expiresIn = response.data.expires_in || 7200; // Default 2 hours
      this.tokenExpiry = Date.now() + (expiresIn - 60) * 1000; // Refresh 1 minute early

      console.log('VALD API token refreshed successfully');
      return this.tokenCache;
    } catch (error) {
      console.error('Failed to get VALD API token:', error.response?.data || error.message);
      throw new Error('Authentication failed');
    }
  }

  async makeAPIRequest(endpoint, params = {}) {
    const token = await this.getAccessToken();
    
    try {
      const response = await axios.get(`${this.baseURL}${endpoint}`, {
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        params: {
          tenantId: this.tenantId,
          ...params
        },
        timeout: 30000 // 30 second timeout
      });

      return response.data;
    } catch (error) {
      console.error(`VALD API request failed for ${endpoint}:`, error.response?.data || error.message);
      throw error;
    }
  }

  async searchAthletes(searchTerm) {
    try {
      const data = await this.makeAPIRequest('/profiles', {
        search: searchTerm,
        limit: 20
      });

      return data.map(profile => ({
        athlete_id: profile.id,
        athlete_name: profile.name,
        source: 'live',
        sport: profile.sport,
        position: profile.position,
        last_updated: new Date().toISOString()
      }));
    } catch (error) {
      console.error('Search athletes failed:', error);
      return [];
    }
  }

  async getAthleteProfile(athleteId) {
    try {
      const data = await this.makeAPIRequest(`/profiles/${athleteId}`);
      
      return {
        athlete_id: data.id,
        athlete_name: data.name,
        sport: data.sport,
        position: data.position,
        date_of_birth: data.dateOfBirth,
        height: data.height,
        weight: data.weight,
        source: 'live'
      };
    } catch (error) {
      console.error('Get athlete profile failed:', error);
      throw error;
    }
  }

  async getAthleteTestDates(athleteId) {
    try {
      const data = await this.makeAPIRequest(`/profiles/${athleteId}/tests`, {
        includeResults: false
      });

      // Group tests by date and test type
      const testsByDate = {};
      data.forEach(test => {
        const testDate = test.testDate.split('T')[0]; // Get date part only
        if (!testsByDate[testDate]) {
          testsByDate[testDate] = {
            test_date: testDate,
            result_id: test.id,
            assessment_id: test.assessmentId,
            test_types: []
          };
        }
        testsByDate[testDate].test_types.push(test.testType);
      });

      return Object.values(testsByDate)
        .sort((a, b) => new Date(b.test_date) - new Date(a.test_date));
    } catch (error) {
      console.error('Get athlete test dates failed:', error);
      throw error;
    }
  }

  async getTestResults(athleteId, resultId) {
    try {
      // Get all test types for this result
      const [cmjData, imtpData, ppuData, hjData] = await Promise.all([
        this.getTestTypeResults(athleteId, resultId, 'CMJ').catch(() => null),
        this.getTestTypeResults(athleteId, resultId, 'IMTP').catch(() => null),
        this.getTestTypeResults(athleteId, resultId, 'PPU').catch(() => null),
        this.getTestTypeResults(athleteId, resultId, 'HJ').catch(() => null)
      ]);

      return {
        result_id: resultId,
        athlete_id: athleteId,
        cmj: cmjData,
        imtp: imtpData,
        ppu: ppuData,
        hj: hjData,
        source: 'live',
        retrieved_at: new Date().toISOString()
      };
    } catch (error) {
      console.error('Get test results failed:', error);
      throw error;
    }
  }

  async getTestTypeResults(athleteId, resultId, testType) {
    try {
      const data = await this.makeAPIRequest(`/profiles/${athleteId}/tests/${resultId}`, {
        testType: testType
      });

      // Extract metrics based on test type
      return this.extractMetrics(data, testType);
    } catch (error) {
      console.error(`Get ${testType} results failed:`, error);
      return null;
    }
  }

  extractMetrics(rawData, testType) {
    const metrics = {};
    
    switch (testType) {
      case 'CMJ':
        metrics.CONCENTRIC_IMPULSE_Trial_Ns = rawData.concentricImpulse;
        metrics.ECCENTRIC_BRAKING_RFD_Trial_N_s = rawData.eccentricBrakingRFD;
        metrics.PEAK_CONCENTRIC_FORCE_Trial_N = rawData.peakConcentricForce;
        metrics.BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg = rawData.bodyMassRelativeTakeoffPower;
        metrics.RSI_MODIFIED_Trial_RSI_mod = rawData.rsiModified;
        metrics.ECCENTRIC_BRAKING_IMPULSE_Trial_Ns = rawData.eccentricBrakingImpulse;
        metrics.JUMP_HEIGHT_Trial_m = rawData.jumpHeight;
        break;
      
      case 'IMTP':
        metrics.PEAK_FORCE_Trial_N = rawData.peakForce;
        metrics.BODYMASS_RELATIVE_PEAK_FORCE_Trial_N_kg = rawData.bodyMassRelativePeakForce;
        metrics.RFD_0_100_Trial_N_s = rawData.rfd0_100;
        metrics.RFD_0_200_Trial_N_s = rawData.rfd0_200;
        break;
      
      case 'PPU':
        metrics.PEAK_FORCE_Trial_N = rawData.peakForce;
        metrics.AVERAGE_FORCE_Trial_N = rawData.averageForce;
        metrics.RFD_Trial_N_s = rawData.rfd;
        metrics.FORCE_ASYMMETRY_Trial_percent = rawData.forceAsymmetry;
        break;
      
      case 'HJ':
        metrics.RSI_Trial_RSI = rawData.rsi;
        metrics.CONTACT_TIME_Trial_s = rawData.contactTime;
        metrics.JUMP_HEIGHT_Trial_m = rawData.jumpHeight;
        break;
    }

    return {
      test_type: testType,
      test_date: rawData.testDate,
      metrics: metrics,
      raw_data: rawData
    };
  }

  // Fallback method using Python script for complex operations
  async callPythonHelper(operation, params = []) {
    return new Promise((resolve, reject) => {
      const pythonScript = path.join(__dirname, '../../Scripts/VALDapiHelpers.py');
      const pythonProcess = spawn('python', [pythonScript, operation, ...params]);
      
      let result = '';
      let error = '';
      
      pythonProcess.stdout.on('data', (data) => {
        result += data.toString();
      });
      
      pythonProcess.stderr.on('data', (data) => {
        error += data.toString();
      });
      
      pythonProcess.on('close', (code) => {
        if (code === 0) {
          try {
            resolve(JSON.parse(result));
          } catch (e) {
            resolve(result);
          }
        } else {
          reject(new Error(`Python helper failed: ${error}`));
        }
      });
    });
  }
}

module.exports = VALDAPIService;