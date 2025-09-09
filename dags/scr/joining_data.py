import logging
import os
import unicodedata
import pandas as pd
from fuzzywuzzy import process
import fsspec  # S3 filesystem via s3fs

logger = logging.getLogger(__name__)


def _is_s3(path: str) -> bool:
    return str(path).startswith("s3://")

def normalize_text(text):
    """Remove accents and convert to lowercase for better matching"""
    if not isinstance(text, str):
        return ""
    return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII').lower()

def load_datasets(odds_file: str, results_file: str):
    """Load odds and results datasets from parquet files"""
    odds_df = pd.read_parquet(odds_file, engine='pyarrow')
    results_df = pd.read_parquet(results_file, engine='pyarrow')
    logger.info("Loaded datasets | odds: %s rows, %s cols | results: %s rows, %s cols",
                odds_df.shape[0], odds_df.shape[1], results_df.shape[0], results_df.shape[1])
    return odds_df, results_df

def get_unique_teams(odds_df, results_df):
    """Get unique team names from both datasets"""
    odds_teams = set(odds_df['home_team']).union(set(odds_df['away_team']))
    results_teams = set(results_df['home_team']).union(set(results_df['away_team']))
    logger.info("Unique teams | odds: %d | results: %d", len(odds_teams), len(results_teams))
    return odds_teams, results_teams

def print_team_names(odds_teams, results_teams):
    """Print team names for debugging purposes"""
    print("\nTeams in odds data:")
    for team in sorted(odds_teams):
        print(f"  - '{team}'")
        
    print("\nTeams in results data:")
    for team in sorted(results_teams):
        print(f"  - '{team}'")

def find_best_match(team_name, normalized_results, threshold=50):
    """Find the best matching team name from the choices list."""
    norm_team = normalize_text(team_name)
    if norm_team in normalized_results:
        return normalized_results[norm_team], 100
    match, score = process.extractOne(norm_team, list(normalized_results.keys()))
    return (normalized_results[match], score)

def create_team_mapping(odds_teams, results_teams, threshold=50):
    """Create a mapping dictionary from odds team names to results team names"""
    normalized_results = {normalize_text(t): t for t in results_teams}
    mapping = {}
    for t in odds_teams:
        mapped, score = find_best_match(t, normalized_results, threshold)
        mapping[t] = mapped
        if t != mapped:
            level = logging.WARNING if score < threshold else logging.INFO
            logger.log(level, "Fuzzy map: '%s' -> '%s' (score=%d)", t, mapped, score)
    return mapping

def standardize_team_names(odds_df, results_df, team_mapping):
    """Apply team name mapping to standardize team names and prepare date columns"""
    odds_df['home_team_std'] = odds_df['home_team'].map(lambda x: team_mapping.get(x, x))
    odds_df['away_team_std'] = odds_df['away_team'].map(lambda x: team_mapping.get(x, x))
    results_df['home_team_std'] = results_df['home_team']
    results_df['away_team_std'] = results_df['away_team']
    
    if 'commence_time' in odds_df.columns and 'date' in results_df.columns:
        print("Standardizing date formats for matching...")
        odds_df['match_date'] = pd.to_datetime(odds_df['commence_time'], errors='coerce').dt.date
        results_df['match_date'] = pd.to_datetime(results_df['date'], errors='coerce').dt.date
        logger.info("Date ranges | odds: %s -> %s | results: %s -> %s",
                    odds_df['match_date'].min(), odds_df['match_date'].max(),
                    results_df['match_date'].min(), results_df['match_date'].max())
    else:
        logger.warning("Missing date columns for matching; fallback merge may be less accurate.")
    return odds_df, results_df

def merge_datasets(odds_df, results_df):
    """Join datasets on standardized team names and match date"""
    if 'match_date' in odds_df.columns and 'match_date' in results_df.columns:
        logger.info("Merging on (home_team_std, away_team_std, match_date)")
        merged = pd.merge(
            odds_df, results_df,
            how='left',
            left_on=['home_team_std', 'away_team_std', 'match_date'],
            right_on=['home_team_std', 'away_team_std', 'match_date']
        )
    else:
        logger.info("Merging on (home_team_std, away_team_std, commence_time==date)")
        merged = pd.merge(
            odds_df, results_df,
            how='left',
            left_on=['home_team_std', 'away_team_std', 'commence_time'],
            right_on=['home_team_std', 'away_team_std', 'date']
        )

    matched = merged['result'].notna().sum() if 'result' in merged.columns else 0
    logger.info("Merge result: matched=%d / total=%d (%.1f%%)",
                matched, len(merged), (matched / len(merged) * 100 if len(merged) else 0))
    return merged

def clean_merged_dataframe(merged_df):
    """Clean up and format the merged dataframe"""
    drop_cols = [
        'home_team_y', 'away_team_y', 'home_team_std', 'away_team_std',
        'status', 'match_id_y', 'match_id_x', 'commence_time'
    ]
    if 'match_date' in merged_df.columns:
        # Keep match_date as canonical date
        pass
    merged_df = merged_df.drop(columns=[c for c in drop_cols if c in merged_df.columns], errors='ignore')
    rename_map = {
        'home_team_x': 'home_team',
        'away_team_x': 'away_team',
        'date': 'match_date'
    }
    merged_df = merged_df.rename(columns={k: v for k, v in rename_map.items() if k in merged_df.columns})
    if 'matchday' in merged_df.columns:
        cols = ['matchday'] + [c for c in merged_df.columns if c != 'matchday']
        merged_df = merged_df[cols]
    else:
        logger.warning("'matchday' column missing after merge.")
    return merged_df

def save_to_parquet(df, output_file: str):
    """Save DataFrame to parquet file (local or S3)"""
    # Assume S3 path; let exceptions surface if misconfigured
    df.to_parquet(output_file, index=False, engine='pyarrow')
    logger.info("Saved merged dataset: %d rows -> %s", len(df), output_file)
    return df

def print_summary(merged_df):
    """Print summary statistics of the merged data"""
    if merged_df is None or merged_df.empty:
        logger.warning("No merged data to summarize.")
        return
    total = len(merged_df)
    matched = merged_df['result'].notna().sum() if 'result' in merged_df.columns else 0
    logger.info("Summary | rows=%d | matched=%d (%.1f%%)",
                total, matched, (matched / total * 100 if total else 0))
    if 'result' in merged_df.columns and matched:
        dist = merged_df['result'].value_counts()
        for k, v in dist.items():
            logger.info("Result '%s': %d (%.1f%%)", k, v, v / matched * 100)

def join_data(odds_file: str, results_file: str, output_file: str):
    """Main function to join odds and results data"""
    logger.info("Joining odds and results | odds=%s | results=%s", odds_file, results_file)
    odds_df, results_df = load_datasets(odds_file, results_file)
    odds_teams, results_teams = get_unique_teams(odds_df, results_df)
    team_mapping = create_team_mapping(odds_teams, results_teams)
    odds_df, results_df = standardize_team_names(odds_df, results_df, team_mapping)
    merged_df = merge_datasets(odds_df, results_df)
    merged_df = clean_merged_dataframe(merged_df)
    save_to_parquet(merged_df, output_file)
    print_summary(merged_df)
    return merged_df
