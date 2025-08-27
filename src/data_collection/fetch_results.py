import requests
import pandas as pd
from datetime import datetime
import os
import pytz

def get_api_key():
    """Get API key from environment variable or return default"""
    return os.environ.get('FOOTBALL_DATA_API_KEY', 'fb4ec8e596414131adeeb76803633492')

def get_competition_id():
    """Return the competition ID for Campeonato Brasileiro Série A"""
    return 2013

def get_api_headers(api_key=None):
    """Get headers for API request"""
    if api_key is None:
        api_key = get_api_key()
    return {'X-Auth-Token': api_key}

def fetch_matches_from_api(competition_id=None, headers=None):
    """Fetch matches from the football-data.org API"""
    if competition_id is None:
        competition_id = get_competition_id()
    if headers is None:
        headers = get_api_headers()
    
    url = f'https://api.football-data.org/v4/competitions/{competition_id}/matches'
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"Error: API returned status code {response.status_code}")
        print(response.text)
        return None
    
    data = response.json()
    return data.get('matches', [])

def process_match_data(match, min_matchday=18, only_finished=True):
    """Process a single match data and return a dictionary or None if skipped"""
    # Skip matches before min_matchday
    if match['matchday'] < min_matchday:
        return None
        
    # Skip unfinished matches if only_finished is True
    if only_finished and match['status'] != 'FINISHED':
        return None
    
    # Convert UTC time to local time with proper timezone handling
    
    utc_time = datetime.fromisoformat(match['utcDate'].replace('Z', '+00:00'))
    brasilia_timezone = pytz.timezone('America/Sao_Paulo')
    brasilia_time = utc_time.astimezone(brasilia_timezone)
    formatted_date = brasilia_time.strftime("%Y-%m-%d")
    
    # Handle scores for matches that haven't finished yet
    home_score = None
    away_score = None
    result = None
    
    if match['status'] == 'FINISHED':
        # Convert scores to int
        home_score = match['score']['fullTime']['home']
        away_score = match['score']['fullTime']['away']
        
        # Calculate result
        if home_score > away_score:
            result = 'Home'  # Home win
        elif home_score < away_score:
            result = 'Away'  # Away win
        else:
            result = 'Draw'  # Draw
    
    return {
        'match_id': match['id'],
        'date': formatted_date,
        'matchday': match['matchday'],
        'home_team': match['homeTeam']['name'],
        'away_team': match['awayTeam']['name'],
        'home_score': home_score,
        'away_score': away_score,
        'status': match['status'],
        'result': result
    }

def process_matches(matches, min_matchday=14, only_finished=True):
    """Process all matches and return a list of processed matches"""
    results_data = []
    for match in matches:
        processed_match = process_match_data(match, min_matchday, only_finished)
        if processed_match:
            results_data.append(processed_match)
    return results_data

def create_dataframe_from_matches(results_data):
    """Create a DataFrame from processed match data"""
    return pd.DataFrame(results_data)

def save_dataframe_to_csv(df, filename):
    """Save DataFrame to CSV file"""
    # Ensure directory exists
    directory = os.path.dirname(filename)
    if directory:
        os.makedirs(directory, exist_ok=True)
    
    df.to_csv(filename, index=False)
    print(f"Saved results to {filename}")
    return df

def fetch_brasileirao_results(only_finished=True, min_matchday=14, output_file=None):
    """Main function to fetch Brasileirão results from the API"""
    print(f"Fetching Brasileirão match results from matchday {min_matchday} onwards...")
    
    # Get matches from API
    matches = fetch_matches_from_api()
    if not matches:
        return None
    
    print(f"Fetched {len(matches)} matches from the API")
    
    # Process matches
    results_data = process_matches(matches, min_matchday, only_finished)
    
    # Create DataFrame
    results_df = create_dataframe_from_matches(results_data)
    
    # Count number of finished matches
    finished_matches = results_df[results_df['status'] == 'FINISHED'].shape[0]
    total_matches = results_df.shape[0]
    print(f"Processed {finished_matches} completed matches out of {total_matches} total matches")
    
    # Save to CSV if output_file is provided
    if output_file:
        save_dataframe_to_csv(results_df, output_file)
    
    return results_df

def fetch_matchday_results(matchday, base_output_file=None, matchday_output_file=None):
    """Fetch results for a specific matchday"""
    # Get all results first
    all_results = fetch_brasileirao_results(only_finished=False, output_file=base_output_file)
    
    if all_results is None:
        return None
    
    # Filter for the specific matchday
    matchday_results = all_results[all_results['matchday'] == matchday]
    
    # Count finished matches in this matchday
    finished_matches = matchday_results[matchday_results['status'] == 'FINISHED'].shape[0]
    total_matches = matchday_results.shape[0]
    
    print(f"Matchday {matchday}: {finished_matches} of {total_matches} matches completed")
    
    # Save to matchday-specific CSV
    if matchday_output_file:
        save_dataframe_to_csv(matchday_results, matchday_output_file)
    
    return matchday_results

def get_latest_completed_matchday(results_df):
    """Get the latest completed matchday from a DataFrame of results"""
    completed_matchdays = results_df[results_df['status'] == 'FINISHED']['matchday'].unique()
    if len(completed_matchdays) > 0:
        return max(completed_matchdays)
    return None

if __name__ == '__main__':
    # Define output file path here in main
    output_file = 'data/raw/brasileirao_results.csv'
    
    # Fetch all results from matchday 14 onwards
    results = fetch_brasileirao_results(only_finished=False, output_file=output_file)
    
    if results is not None:
        print("\nResults DataFrame Preview:")
        print(results.head())
        
        # Find the latest completed matchday
        latest_matchday = get_latest_completed_matchday(results)
        if latest_matchday:
            print(f"\nLatest completed matchday: {latest_matchday}")