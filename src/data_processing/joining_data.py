import pandas as pd
import sqlite3
from fuzzywuzzy import process
import unicodedata
import os

def normalize_text(text):
    """Remove accents and convert to lowercase for better matching"""
    if not isinstance(text, str):
        return ""
    return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII').lower()

def load_datasets(odds_file, results_file):
    """Load odds and results datasets from CSV files"""
    odds_df = pd.read_csv(odds_file)
    results_df = pd.read_csv(results_file)
    
    print(f"Odds data shape: {odds_df.shape}")
    print(f"Results data shape: {results_df.shape}")
    
    return odds_df, results_df

def get_unique_teams(odds_df, results_df):
    """Get unique team names from both datasets"""
    odds_teams = set(odds_df['home_team'].unique()) | set(odds_df['away_team'].unique())
    results_teams = set(results_df['home_team'].unique()) | set(results_df['away_team'].unique())
    
    print(f"Unique teams in odds data: {len(odds_teams)}")
    print(f"Unique teams in results data: {len(results_teams)}")
    
    return odds_teams, results_teams

def print_team_names(odds_teams, results_teams):
    """Print team names for debugging purposes"""
    print("\nTeams in odds data:")
    for team in sorted(odds_teams):
        print(f"  - '{team}'")
        
    print("\nTeams in results data:")
    for team in sorted(results_teams):
        print(f"  - '{team}'")

def find_best_match(team_name, choices, normalized_choices, threshold=50):
    """Find the best matching team name from the choices list."""
    # Normalize the team name for matching
    normalized_team = normalize_text(team_name)
    
    # Try exact match after normalization first
    if normalized_team in normalized_choices:
        return normalized_choices[normalized_team]
    
    # No exact match found, use fuzzy matching
    match, score = process.extractOne(normalized_team, list(normalized_choices.keys()))
    
    if score >= threshold:
        return normalized_choices[match]
    else:
        print(f"Warning: Low match confidence for '{team_name}' -> '{normalized_choices[match]}' (score: {score})")
        return normalized_choices[match]  # Still return best match even if below threshold

def create_team_mapping(odds_teams, results_teams):
    """Create a mapping dictionary from odds team names to results team names"""
    normalized_results_teams = {normalize_text(team): team for team in results_teams}
    
    team_mapping = {}
    for team in odds_teams:
        team_mapping[team] = find_best_match(team, results_teams, normalized_results_teams)
    
    print("\nFuzzy matching results:")
    for odds_team, results_team in team_mapping.items():
        if odds_team != results_team:
            print(f"  '{odds_team}' -> '{results_team}'")
    
    return team_mapping

def standardize_team_names(odds_df, results_df, team_mapping):
    """Apply team name mapping to standardize team names and prepare date columns"""
    # Map team names
    odds_df['home_team_std'] = odds_df['home_team'].map(lambda x: team_mapping.get(x, x))
    odds_df['away_team_std'] = odds_df['away_team'].map(lambda x: team_mapping.get(x, x))
    results_df['home_team_std'] = results_df['home_team']
    results_df['away_team_std'] = results_df['away_team']
    
    # Standardize dates
    if 'commence_time' in odds_df.columns and 'date' in results_df.columns:
        print("Standardizing date formats for matching...")
        
        # Convert to datetime and extract date only (no time)
        odds_df['match_date'] = pd.to_datetime(odds_df['commence_time']).dt.date
        results_df['match_date'] = pd.to_datetime(results_df['date']).dt.date
        
        # Print date ranges for debugging
        print(f"Odds date range: {odds_df['match_date'].min()} to {odds_df['match_date'].max()}")
        print(f"Results date range: {results_df['match_date'].min()} to {results_df['match_date'].max()}")
    else:
        print("Warning: Date columns not found for matching")
    
    return odds_df, results_df

def merge_datasets(odds_df, results_df):
    """Join datasets on standardized team names and match date"""
    # Check if date columns were prepared
    if 'match_date' in odds_df.columns and 'match_date' in results_df.columns:
        print("Merging on team names and match date...")
        
        # Debug: Show a few sample teams and dates before merging
        print("\nSample data before merge:")
        sample = odds_df[['home_team_std', 'away_team_std', 'match_date']].head(3)
        print("Odds sample:")
        print(sample)
        
        # Try to find these exact matches in results
        for _, row in sample.iterrows():
            home = row['home_team_std']
            away = row['away_team_std']
            date = row['match_date']
            matches = results_df[(results_df['home_team_std'] == home) & 
                               (results_df['away_team_std'] == away) &
                               (results_df['match_date'] == date)]
            print(f"Found {len(matches)} exact matches for {home} vs {away} on {date}")
        
        # Perform merge with date
        merged_df = pd.merge(
            odds_df,
            results_df,
            how='left',
            left_on=['home_team_std', 'away_team_std', 'match_date'],
            right_on=['home_team_std', 'away_team_std', 'match_date']
        )
    else:
        # Fall back to team-only merge
        print("Date columns not available, merging on team names only...")
        merged_df = pd.merge(
            odds_df,
            results_df,
            how='left',
            left_on=['home_team_std', 'away_team_std', 'commence_time'],
            right_on=['home_team_std', 'away_team_std', 'date']
        )
    
    # Check merge results
    match_count = merged_df['result'].notna().sum() if 'result' in merged_df.columns else 0
    print(f"Merge result: {match_count} of {len(merged_df)} records matched with results")
    
    return merged_df

def clean_merged_dataframe(merged_df):
    """Clean up and format the merged dataframe"""
    # Check how many odds entries were matched with results
    matched_count = merged_df['result'].notna().sum() if 'result' in merged_df.columns else 0
    total_count = len(merged_df)
    
    if total_count > 0:
        match_percentage = matched_count/total_count*100
        print(f"Matched {matched_count} out of {total_count} odds entries with results ({match_percentage:.1f}%)")
    
    # Columns to drop
    cols_to_drop = ['home_team_y', 'away_team_y', 'home_team_std', 'away_team_std', 
                   'status', 'match_id_y', 'match_id_x', 'commence_time', 'match_date']
    
    # Drop columns that exist
    merged_df = merged_df.drop(columns=[col for col in cols_to_drop if col in merged_df.columns])
    
    # Rename columns
    merged_df = merged_df.rename(columns={
        'home_team_x': 'home_team', 
        'away_team_x': 'away_team',
        'date': 'match_date'  # Keep the date with a clearer name
    })
    
    # Reorder columns to have matchday first
    if 'matchday' in merged_df.columns:
        # Get all columns except matchday
        other_columns = [col for col in merged_df.columns if col != 'matchday']
        # Create new column order with matchday first
        new_column_order = ['matchday'] + other_columns
        # Reorder the DataFrame
        merged_df = merged_df[new_column_order]
    else:
        print("Warning: 'matchday' column not found in the merged DataFrame")
    
    return merged_df

def save_to_csv(df, output_file):
    """Save DataFrame to CSV file"""
    # Ensure directory exists
    os.makedirs(os.path.dirname(os.path.abspath(output_file)) or '.', exist_ok=True)
    df.to_csv(output_file, index=False)
    print(f"Merged data saved to {output_file}")
    
    return df

def save_to_sqlite(df, db_file, table_name="odds_results"):
    """Save DataFrame to SQLite database"""
    # Ensure directory exists
    os.makedirs(os.path.dirname(os.path.abspath(db_file)) or '.', exist_ok=True)
    conn = sqlite3.connect(db_file)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    print(f"Merged data saved to SQLite database '{db_file}'")
    conn.close()
    
    return df

def print_summary(merged_df):
    """Print summary statistics of the merged data"""
    print("\nPreview of merged data:")
    print(merged_df.head())
    
    print("\nSummary of merged data:")
    print(f"Total matches: {len(merged_df)}")
    print(f"Matches with results: {merged_df['result'].notna().sum() if 'result' in merged_df.columns else 0}")
    
    # Show distribution of results where available
    if 'result' in merged_df.columns and merged_df['result'].notna().sum() > 0:
        result_counts = merged_df['result'].value_counts()
        print("\nResult distribution:")
        for result, count in result_counts.items():
            print(f"  {result}: {count} ({count/result_counts.sum()*100:.1f}%)")

def join_data(odds_file, results_file, output_file, db_file="brasileirao.db"):
    """Main function to join odds and results data"""
    # Load datasets
    odds_df, results_df = load_datasets(odds_file, results_file)
    
    # Get unique teams
    odds_teams, results_teams = get_unique_teams(odds_df, results_df)
    
    # Print team names for debugging
    print_team_names(odds_teams, results_teams)
    
    # Create team mapping using only fuzzy matching
    team_mapping = create_team_mapping(odds_teams, results_teams)
    
    # Standardize team names
    odds_df, results_df = standardize_team_names(odds_df, results_df, team_mapping)
    
    # Merge datasets
    merged_df = merge_datasets(odds_df, results_df)

    
    # Clean merged dataframe
    merged_df = clean_merged_dataframe(merged_df)
    
    # Save to CSV and SQLite
    save_to_csv(merged_df, output_file)
    save_to_sqlite(merged_df, db_file)
    
    # Print summary
    print_summary(merged_df)
    
    return merged_df

if __name__ == '__main__':
    # Define input and output file paths
    odds_file = "/Users/buc/Documents/brasileirao-odds-analysis/data/raw/brasileirao_odds.csv"
    results_file = "/Users/buc/Documents/brasileirao-odds-analysis/data/raw/brasileirao_results.csv"
    output_file = "/Users/buc/Documents/brasileirao-odds-analysis/data/processed/brasileirao_odds_results.csv"
    db_file = "/Users/buc/Documents/brasileirao-odds-analysis/data/processed/brasileirao.db"
    
    # Run the main function
    join_data(odds_file, results_file, output_file, db_file)