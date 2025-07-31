const { BigQuery } = require('@google-cloud/bigquery');
const path = require('path');
// Load .env file from Scripts directory
require('dotenv').config({ path: path.join(__dirname, '../../Scripts/.env') });

class AnalyticsService {
  constructor() {
    this.bqClient = new BigQuery({
      keyFilename: path.join(__dirname, '../../Scripts/gcp_credentials.json'),
      projectId: 'vald-ref-data',
    });
    this.dataset = 'athlete_performance_db';
  }

  async calculatePercentile(metricName, metricValue, testType = 'cmj') {
    try {
      const tableName = `${testType}_results`;
      const query = `
        SELECT 
          COUNT(*) as total_count,
          SUM(CASE WHEN ${metricName} <= @metricValue THEN 1 ELSE 0 END) as rank
        FROM \`${this.dataset}.${tableName}\`
        WHERE ${metricName} IS NOT NULL
      `;
      
      const options = {
        query,
        params: { metricValue: parseFloat(metricValue) },
        location: 'US',
      };
      
      const [rows] = await this.bqClient.query(options);
      const { total_count, rank } = rows[0];
      
      return total_count > 0 ? Math.round((rank / total_count) * 100) : 0;
    } catch (error) {
      console.error(`Calculate percentile failed for ${metricName}:`, error);
      return 0;
    }
  }

  async calculateMultiplePercentiles(metrics, testType = 'cmj') {
    const percentilePromises = Object.entries(metrics).map(async ([metricName, metricValue]) => {
      if (metricValue !== null && metricValue !== undefined) {
        const percentile = await this.calculatePercentile(metricName, metricValue, testType);
        return [metricName, { value: metricValue, percentile }];
      }
      return [metricName, { value: null, percentile: null }];
    });

    const percentileResults = await Promise.all(percentilePromises);
    return Object.fromEntries(percentileResults);
  }

  async calculateCompositeScore(metrics, testType = 'cmj') {
    try {
      // Get composite score weights from database or use defaults
      const weights = await this.getCompositeWeights(testType);
      
      let weightedSum = 0;
      let totalWeight = 0;
      
      for (const [metricName, weight] of Object.entries(weights)) {
        if (metrics[metricName] !== null && metrics[metricName] !== undefined) {
          const percentile = await this.calculatePercentile(metricName, metrics[metricName], testType);
          weightedSum += percentile * weight;
          totalWeight += weight;
        }
      }
      
      return totalWeight > 0 ? Math.round(weightedSum / totalWeight) : 0;
    } catch (error) {
      console.error('Calculate composite score failed:', error);
      return 0;
    }
  }

  async getCompositeWeights(testType = 'cmj') {
    // Default weights - could be stored in BigQuery for dynamic updates
    const defaultWeights = {
      cmj: {
        'CONCENTRIC_IMPULSE_Trial_Ns': 0.15,
        'ECCENTRIC_BRAKING_RFD_Trial_N_s': 0.20,
        'PEAK_CONCENTRIC_FORCE_Trial_N': 0.15,
        'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg': 0.25,
        'RSI_MODIFIED_Trial_RSI_mod': 0.15,
        'ECCENTRIC_BRAKING_IMPULSE_Trial_Ns': 0.10
      },
      imtp: {
        'PEAK_FORCE_Trial_N': 0.4,
        'BODYMASS_RELATIVE_PEAK_FORCE_Trial_N_kg': 0.3,
        'RFD_0_100_Trial_N_s': 0.15,
        'RFD_0_200_Trial_N_s': 0.15
      },
      ppu: {
        'PEAK_FORCE_Trial_N': 0.3,
        'AVERAGE_FORCE_Trial_N': 0.25,
        'RFD_Trial_N_s': 0.25,
        'FORCE_ASYMMETRY_Trial_percent': 0.2
      },
      hj: {
        'RSI_Trial_RSI': 0.5,
        'CONTACT_TIME_Trial_s': 0.25,
        'JUMP_HEIGHT_Trial_m': 0.25
      }
    };

    return defaultWeights[testType] || defaultWeights.cmj;
  }

  async getDatabaseBenchmarks(testType = 'cmj') {
    try {
      const tableName = `${testType}_results`;
      const query = `
        SELECT
          'CONCENTRIC_IMPULSE_Trial_Ns' as metric_name,
          AVG(CONCENTRIC_IMPULSE_Trial_Ns) as average,
          MAX(CONCENTRIC_IMPULSE_Trial_Ns) as maximum,
          MIN(CONCENTRIC_IMPULSE_Trial_Ns) as minimum,
          STDDEV(CONCENTRIC_IMPULSE_Trial_Ns) as std_dev
        FROM \`${this.dataset}.${tableName}\`
        WHERE CONCENTRIC_IMPULSE_Trial_Ns IS NOT NULL
        
        UNION ALL
        
        SELECT
          'ECCENTRIC_BRAKING_RFD_Trial_N_s' as metric_name,
          AVG(ECCENTRIC_BRAKING_RFD_Trial_N_s) as average,
          MAX(ECCENTRIC_BRAKING_RFD_Trial_N_s) as maximum,
          MIN(ECCENTRIC_BRAKING_RFD_Trial_N_s) as minimum,
          STDDEV(ECCENTRIC_BRAKING_RFD_Trial_N_s) as std_dev
        FROM \`${this.dataset}.${tableName}\`
        WHERE ECCENTRIC_BRAKING_RFD_Trial_N_s IS NOT NULL
        
        UNION ALL
        
        SELECT
          'PEAK_CONCENTRIC_FORCE_Trial_N' as metric_name,
          AVG(PEAK_CONCENTRIC_FORCE_Trial_N) as average,
          MAX(PEAK_CONCENTRIC_FORCE_Trial_N) as maximum,
          MIN(PEAK_CONCENTRIC_FORCE_Trial_N) as minimum,
          STDDEV(PEAK_CONCENTRIC_FORCE_Trial_N) as std_dev
        FROM \`${this.dataset}.${tableName}\`
        WHERE PEAK_CONCENTRIC_FORCE_Trial_N IS NOT NULL
        
        UNION ALL
        
        SELECT
          'BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg' as metric_name,
          AVG(BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg) as average,
          MAX(BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg) as maximum,
          MIN(BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg) as minimum,
          STDDEV(BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg) as std_dev
        FROM \`${this.dataset}.${tableName}\`
        WHERE BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg IS NOT NULL
        
        UNION ALL
        
        SELECT
          'RSI_MODIFIED_Trial_RSI_mod' as metric_name,
          AVG(RSI_MODIFIED_Trial_RSI_mod) as average,
          MAX(RSI_MODIFIED_Trial_RSI_mod) as maximum,
          MIN(RSI_MODIFIED_Trial_RSI_mod) as minimum,
          STDDEV(RSI_MODIFIED_Trial_RSI_mod) as std_dev
        FROM \`${this.dataset}.${tableName}\`
        WHERE RSI_MODIFIED_Trial_RSI_mod IS NOT NULL
        
        UNION ALL
        
        SELECT
          'ECCENTRIC_BRAKING_IMPULSE_Trial_Ns' as metric_name,
          AVG(ECCENTRIC_BRAKING_IMPULSE_Trial_Ns) as average,
          MAX(ECCENTRIC_BRAKING_IMPULSE_Trial_Ns) as maximum,
          MIN(ECCENTRIC_BRAKING_IMPULSE_Trial_Ns) as minimum,
          STDDEV(ECCENTRIC_BRAKING_IMPULSE_Trial_Ns) as std_dev
        FROM \`${this.dataset}.${tableName}\`
        WHERE ECCENTRIC_BRAKING_IMPULSE_Trial_Ns IS NOT NULL
      `;
      
      const options = {
        query,
        location: 'US',
      };
      
      const [rows] = await this.bqClient.query(options);
      
      // Convert to object format
      const benchmarks = {};
      rows.forEach(row => {
        benchmarks[row.metric_name] = {
          average: row.average,
          maximum: row.maximum,
          minimum: row.minimum,
          std_dev: row.std_dev
        };
      });
      
      return benchmarks;
    } catch (error) {
      console.error('Get database benchmarks failed:', error);
      return {};
    }
  }

  async getAthleteHistory(athleteId, testType = 'cmj', limit = 10) {
    try {
      const tableName = `${testType}_results`;
      const query = `
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
        FROM \`${this.dataset}.${tableName}\`
        WHERE athlete_id = @athleteId
        ORDER BY test_date DESC, result_id DESC
        LIMIT @limit
      `;
      
      const options = {
        query,
        params: { athleteId, limit },
        location: 'US',
      };
      
      const [rows] = await this.bqClient.query(options);
      return rows;
    } catch (error) {
      console.error('Get athlete history failed:', error);
      return [];
    }
  }

  async getAthleteAverages(athleteId, testType = 'cmj') {
    try {
      const tableName = `${testType}_results`;
      const query = `
        SELECT
          AVG(cmj_composite_score) AS avg_composite_score,
          AVG(CONCENTRIC_IMPULSE_Trial_Ns) AS avg_concentric_impulse,
          AVG(ECCENTRIC_BRAKING_RFD_Trial_N_s) AS avg_eccentric_rfd,
          AVG(PEAK_CONCENTRIC_FORCE_Trial_N) AS avg_peak_force,
          AVG(BODYMASS_RELATIVE_TAKEOFF_POWER_Trial_W_kg) AS avg_takeoff_power,
          AVG(RSI_MODIFIED_Trial_RSI_mod) AS avg_rsi_modified,
          AVG(ECCENTRIC_BRAKING_IMPULSE_Trial_Ns) AS avg_eccentric_impulse,
          COUNT(*) as total_tests
        FROM \`${this.dataset}.${tableName}\`
        WHERE athlete_id = @athleteId
      `;
      
      const options = {
        query,
        params: { athleteId },
        location: 'US',
      };
      
      const [rows] = await this.bqClient.query(options);
      return rows[0] || {};
    } catch (error) {
      console.error('Get athlete averages failed:', error);
      return {};
    }
  }

  // Fallback search for when VALD API is unavailable
  async searchAthletesFallback(searchTerm) {
    try {
      const query = `
        SELECT DISTINCT athlete_id, athlete_name
        FROM \`${this.dataset}.cmj_results\`
        WHERE LOWER(athlete_name) LIKE @search
        ORDER BY athlete_name
        LIMIT 20
      `;
      
      const options = {
        query,
        params: { search: `%${searchTerm.toLowerCase()}%` },
        location: 'US',
      };
      
      const [rows] = await this.bqClient.query(options);
      return rows.map(row => ({ ...row, source: 'cached' }));
    } catch (error) {
      console.error('Fallback athlete search failed:', error);
      return [];
    }
  }

  // Fallback test dates for when VALD API is unavailable
  async getTestDatesFallback(athleteId) {
    try {
      const query = `
        SELECT result_id, assessment_id, FORMAT_DATE('%Y-%m-%d', DATE(test_date)) AS test_date
        FROM \`${this.dataset}.cmj_results\`
        WHERE athlete_id = @athleteId
        ORDER BY test_date DESC, result_id DESC
        LIMIT 100
      `;
      
      const options = {
        query,
        params: { athleteId },
        location: 'US',
      };
      
      const [rows] = await this.bqClient.query(options);
      return rows.map(row => ({ ...row, source: 'cached' }));
    } catch (error) {
      console.error('Fallback test dates failed:', error);
      return [];
    }
  }
}

module.exports = AnalyticsService;