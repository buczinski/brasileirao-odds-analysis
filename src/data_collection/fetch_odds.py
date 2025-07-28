# Fetch odds from William Hill bookmaker for each round of Brasileirão matches

import requests
import pandas as pd
import json
import os
from datetime import datetime
import pytz

def fetch_odds(api_key, output_path='../../data/raw/brasileirao_odds.csv'):
    """Fetch odds data from the API and append new matches to existing data"""
    
    odds = requests.get(
        'https://api.the-odds-api.com/v4/sports/soccer_brazil_campeonato/odds/',
        params={
            'apiKey': api_key,
            'regions': 'eu',
            'markets': 'h2h',
            'oddsFormat': 'decimal'
        }
    ).json()
    
    print(f"Fetched {len(odds)} matches from the API")
    
    # Process matches
    matches_dict = process_odds_data(odds)
    
    # Create DataFrame from newly fetched matches
    new_odds_df = pd.DataFrame(list(matches_dict.values()))
    
    # Filter out rows where odds values are NaN
    odds_columns = ['home_win_odds', 'draw_odds', 'away_win_odds']
    new_odds_df = new_odds_df.dropna(subset=odds_columns)
    
    print(f"Processed {len(new_odds_df)} matches with valid odds")
    
    # Load existing odds data if file exists
    if os.path.exists(output_path):
        existing_odds_df = pd.read_csv(output_path)
        print(f"Loaded {len(existing_odds_df)} existing matches from {output_path}")
        
        # Identify which match IDs already exist in our data
        existing_match_ids = set(existing_odds_df['match_id'])
        new_match_ids = set(new_odds_df['match_id'])
        
        # Matches to add (not in existing data)
        matches_to_add = new_match_ids - existing_match_ids
        
        # Filter the new DataFrame to only include new matches
        matches_to_add_df = new_odds_df[new_odds_df['match_id'].isin(matches_to_add)]
        
        print(f"Found {len(matches_to_add)} new matches to add")
        
        # Combine existing and new data
        combined_df = pd.concat([existing_odds_df, matches_to_add_df], ignore_index=True)
        
        # Save the combined data
        combined_df.to_csv(output_path, index=False)
        print(f"Updated {output_path} with new matches. Total records: {len(combined_df)}")
        return combined_df
    else:
        # If file doesn't exist, save new data as initial file
        new_odds_df.to_csv(output_path, index=False)
        print(f"Created new file {output_path} with {len(new_odds_df)} matches")
        return new_odds_df

def process_odds_data(odds):
    """Process raw odds data into a structured dictionary"""
    matches_dict = {}
    
    for match in odds:
        match_id = match['id']
        home_team = match['home_team']
        away_team = match['away_team']
        
        # Convert commence_time to Brasilia time (UTC-3)
        utc_time = datetime.fromisoformat(match['commence_time'].replace('Z', '+00:00'))
        brasilia_timezone = pytz.timezone('America/Sao_Paulo')
        brasilia_time = utc_time.astimezone(brasilia_timezone)
        formatted_time = brasilia_time.strftime("%Y-%m-%d")
        
        # Initialize match entry with basic info
        if match_id not in matches_dict:
            matches_dict[match_id] = {
                'match_id': match_id,
                'home_team': home_team,
                'away_team': away_team,
                'commence_time': formatted_time
            }
        
        # Process bookmakers
        for bookmaker in match['bookmakers']:
            bookmaker_name = bookmaker['title']
            
            # Only process William Hill
            if bookmaker_name not in ["William Hill"]:
                continue
            
            # Extract market odds
            odds_data = extract_market_odds(bookmaker, home_team, away_team)
            
            # Add columns for odds (using generic names)
            matches_dict[match_id]['home_win_odds'] = odds_data['home']
            matches_dict[match_id]['draw_odds'] = odds_data['draw']
            matches_dict[match_id]['away_win_odds'] = odds_data['away']
    
    return matches_dict

def extract_market_odds(bookmaker, home_team, away_team):
    """Extract market odds for a specific bookmaker"""
    home_odds = None
    draw_odds = None
    away_odds = None
    
    for market in bookmaker['markets']:
        if market['key'] == 'h2h':
            for outcome in market['outcomes']:
                if outcome['name'] == home_team:
                    home_odds = outcome['price']
                elif outcome['name'] == away_team:
                    away_odds = outcome['price']
                elif outcome['name'] == 'Draw':
                    draw_odds = outcome['price']
    
    return {
        'home': home_odds,
        'draw': draw_odds,
        'away': away_odds
    }

if __name__ == "__main__":
    # For standalone testing
    API_KEY = 'e791c01b3f81d55c9bf9c4134503fe56'
    # Fix the path by adding a leading slash
    fetch_odds(API_KEY, 'data/raw/brasileirao_odds.csv')

