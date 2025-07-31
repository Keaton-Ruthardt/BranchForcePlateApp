# buildAthleteRefData.py --> process_test_data()
# VALDapiHelpers.py --> 

# API Steps: Token, Profile, Tests, Results

# Imports
import pandas as pd # for data manipulation
import os, json, logging, requests # ???
from datetime import datetime, timedelta
from dotenv import load_dotenv # for reading .env file (key/value pairs)
from filelock import FileLock # for locking the cache file

# Load secrets and constants from enviornment
load_dotenv()
CLIENT_ID = os.getenv('CLIENT_ID') # Issued by VALD
CLIENT_SECRET = os.getenv('CLIENT_SECRET') # Issued by VALD
AUTH_URL = os.getenv("AUTH_URL") # Asking VALD for a token
CACHE_FILE = os.getenv("TOKEN_CACHE_PATH", ".token_cache.json")
LOCK_FILE = f"{CACHE_FILE}.lock"
     # TODO: Add links for forcedecks, dynamo, and sprint, and profile
PROFILE_URL = os.getenv("PROFILE_URL")
TENANT_ID = os.getenv("TENANT_ID")

# Housekeeping
log = logging.getLogger(__name__) # Log records of events
session = requests.Session() # Persistent session for API calls
session.headers.update({"Content-Type": "application/x-www-form-urlencoded"})

# Token Helper: read the token from cache (if it exsists and is valid)
def _read_cache() -> dict:
    try:
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    
# Token Helper: write a fresh token to cache
def _write_cache(token: str, expires_in: int) -> None:
    data = {"access_token": token,
            "expires_at": (datetime.now() + timedelta(seconds=expires_in - 60)).isoformat()} # 60 seconds?
    with open(CACHE_FILE, "w") as f:
        json.dump(data, f)

# Returns a valid API token
def get_access_token() -> str:
    with FileLock(LOCK_FILE, timeout=10):
        # Check cache first
        data = _read_cache()
        if "expires_at" in data and datetime.now() < datetime.fromisoformat(data["expires_at"]):
            return data["access_token"] # Cache still valid -> we're done
        # Cache is missing or expired -> OAuth server
        payload = {"grant_type": "client_credentials",
                   "client_id": CLIENT_ID,
                   "client_secret": CLIENT_SECRET}
        resp = session.post(AUTH_URL, data=payload, timeout=5)
        resp.raise_for_status()
        body = resp.json()
        token = body["access_token"]
        expires_in = body.get("expires_in", 7200)
        _write_cache(token, expires_in)
        log.info("Access token refreshed.")
        return token  
    
# Returns JSON of all profiles
def get_profiles(token):
    # Setting up the current date
    today = datetime.today()
    # Making the GET request
    url = f"{PROFILE_URL}/profiles?tenantId={TENANT_ID}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    # Checking the response
    if response.status_code == 200:
        df = pd.DataFrame(response.json()['profiles'])
        df['givenName'] = df['givenName'].str.strip()
        df['familyName'] = df['familyName'].str.strip()
        df['fullName'] = df['givenName'] + ' ' + df['familyName']
        df['dateOfBirth'] = pd.to_datetime(df['dateOfBirth'])
        df['age'] = (today.year - df['dateOfBirth'].dt.year -
                   ((today.month < df['dateOfBirth'].dt.month) |
                   ((today.month == df['dateOfBirth'].dt.month) & 
                    (today.day < df['dateOfBirth'].dt.day)))).astype(int)
        df = df[['profileId', 'givenName', 'familyName', 'fullName', 'dateOfBirth','age']]
        return df
    else:
        print(f"Failed to get profiles: {response.status_code}")
        return pd.DataFrame()




# Building the Ref Database
# Step 1: Get token
token = get_access_token()
# Step 2: Get profiles from API
profiles = get_profiles(token)
profiles.to_csv("Outputs/ProfilesOM.csv")
# Step 3: 





