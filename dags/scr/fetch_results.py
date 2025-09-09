import os
import logging
from datetime import datetime
import requests
import pytz
import pandas as pd
import fsspec


logger = logging.getLogger(__name__)


def _is_s3(path: str) -> bool:
    return str(path).startswith("s3://")


def _exists(path: str) -> bool:
    return fsspec.filesystem("s3").exists(path) if _is_s3(path) else os.path.exists(path)


def get_api_key() -> str:
    """Get API key from env; raise if missing (avoid hardcoded secrets)."""
    key = os.environ.get('FOOTBALL_DATA_API_KEY')
    if not key:
        raise RuntimeError("FOOTBALL_DATA_API_KEY environment variable not set.")
    return key


def get_competition_id() -> int:
    return 2013  # Brasileirão Série A


def get_api_headers(api_key: str | None = None) -> dict:
    if api_key is None:
        api_key = get_api_key()
    return {'X-Auth-Token': api_key}


def fetch_matches_from_api(competition_id: int | None = None, headers: dict | None = None):
    """Fetch raw matches JSON list from football-data.org."""
    if competition_id is None:
        competition_id = get_competition_id()
    if headers is None:
        headers = get_api_headers()
    url = f'https://api.football-data.org/v4/competitions/{competition_id}/matches'
    try:
        resp = requests.get(url, headers=headers, timeout=30)
    except requests.RequestException as e:
        logger.error("Request to results API failed: %s", e, exc_info=True)
        return []
    if resp.status_code != 200:
        logger.error("Results API status %s: %s", resp.status_code, resp.text[:300])
        return []
    try:
        data = resp.json()
    except ValueError:
        logger.error("Failed decoding JSON from results API.")
        return []
    matches = data.get('matches', [])
    logger.info("Fetched %d raw matches from API.", len(matches))
    return matches


def process_match_data(match: dict, min_matchday: int = 18, only_finished: bool = True):
    """Return processed row dict or None if filtered out."""
    if match.get('matchday') is None:
        return None
    if match['matchday'] < min_matchday:
        return None
    if only_finished and match.get('status') != 'FINISHED':
        return None

    try:
        utc_time = datetime.fromisoformat(match['utcDate'].replace('Z', '+00:00'))
    except Exception:
        return None
    br_tz = pytz.timezone('America/Sao_Paulo')
    br_time = utc_time.astimezone(br_tz)
    date_str = br_time.strftime("%Y-%m-%d")

    home_score = away_score = result = None
    if match.get('status') == 'FINISHED':
        full = match.get('score', {}).get('fullTime', {})
        home_score = full.get('home')
        away_score = full.get('away')
        if home_score is not None and away_score is not None:
            if home_score > away_score:
                result = 'Home'
            elif home_score < away_score:
                result = 'Away'
            else:
                result = 'Draw'

    return {
        'match_id': match.get('id'),
        'date': date_str,
        'matchday': match.get('matchday'),
        'home_team': match.get('homeTeam', {}).get('name'),
        'away_team': match.get('awayTeam', {}).get('name'),
        'home_score': home_score,
        'away_score': away_score,
        'status': match.get('status'),
        'result': result
    }


def process_matches(matches: list, min_matchday: int = 14, only_finished: bool = True) -> list:
    rows = []
    for m in matches:
        row = process_match_data(m, min_matchday=min_matchday, only_finished=only_finished)
        if row:
            rows.append(row)
    logger.info("Processed %d matches after filtering.", len(rows))
    return rows


def save_dataframe_to_parquet(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """Append new rows by match_id (first write wins)."""
    if df is None or df.empty:
        logger.warning("No data to save for %s.", filename)
        return df

    # Create local directory only for local paths
    if not _is_s3(filename):
        directory = os.path.dirname(filename)
        if directory:
            os.makedirs(directory, exist_ok=True)

    # Load existing (assume S3 path; rely on exception if missing)
    try:
        existing = pd.read_parquet(filename, engine='pyarrow')
        logger.debug("Loaded existing Parquet %s with %d rows.", filename, len(existing))
    except FileNotFoundError:
        logger.info("No existing Parquet at %s (will create).", filename)
        existing = pd.DataFrame()
    except Exception as e:
        logger.warning("Failed reading existing Parquet (%s). Recreating file.", e)
        existing = pd.DataFrame()

    new_df = df.copy()
    if 'match_id' in new_df.columns:
        new_df['match_id'] = new_df['match_id'].astype('string')

    if not existing.empty and 'match_id' in existing.columns:
        existing['match_id'] = existing['match_id'].astype('string')
        existing_ids = set(existing['match_id'])
        add_df = new_df[~new_df['match_id'].isin(existing_ids)]
        combined = pd.concat([existing, add_df], ignore_index=True)
        logger.info(
            "Existing rows: %d | New unique rows: %d | Total: %d",
            len(existing), len(add_df), len(combined)
        )
    else:
        combined = new_df
        logger.info("Initialized Parquet with %d rows.", len(combined))

    try:
        combined.to_parquet(filename, index=False, engine='pyarrow')
        logger.info("Wrote %d rows to %s.", len(combined), filename)
    except Exception as e:
        logger.error("Failed writing Parquet to %s: %s", filename, e, exc_info=True)
        return existing if not existing.empty else None

    return combined


def fetch_brasileirao_results(only_finished: bool = True,
                              min_matchday: int = 14,
                              output_file: str | None = None) -> pd.DataFrame | None:
    """Fetch, process, and optionally persist Brasileirão results."""
    logger.info("Starting results fetch (min_matchday=%d, only_finished=%s).",
                min_matchday, only_finished)
    matches = fetch_matches_from_api()
    if not matches:
        logger.warning("No matches returned by API.")
        return None

    processed_rows = process_matches(matches, min_matchday=min_matchday, only_finished=only_finished)
    results_df = pd.DataFrame(processed_rows)

    if results_df.empty:
        logger.warning("No rows after processing.")
        return None

    finished = results_df[results_df['status'] == 'FINISHED'].shape[0]
    logger.info("Processed total rows: %d | Finished: %d", len(results_df), finished)

    if output_file:
        results_df = save_dataframe_to_parquet(results_df, output_file)

    return results_df


def fetch_matchday_results(matchday: int,
                           base_output_file: str | None = None,
                           matchday_output_file: str | None = None) -> pd.DataFrame | None:
    """Fetch all results (not only finished) then filter a single matchday."""
    logger.info("Fetching matchday %d results.", matchday)
    all_results = fetch_brasileirao_results(only_finished=False, output_file=base_output_file)
    if all_results is None or all_results.empty:
        logger.warning("No base results available for matchday %d.", matchday)
        return None

    md_df = all_results[all_results['matchday'] == matchday].copy()
    finished = md_df[md_df['status'] == 'FINISHED'].shape[0]
    logger.info("Matchday %d rows: %d (finished: %d)", matchday, len(md_df), finished)

    if matchday_output_file:
        save_dataframe_to_parquet(md_df, matchday_output_file)

    return md_df


def get_latest_completed_matchday(results_df: pd.DataFrame) -> int | None:
    if results_df is None or results_df.empty:
        return None
    completed = results_df[results_df['status'] == 'FINISHED']['matchday'].dropna().astype('Int64').unique()
    return int(max(completed)) if len(completed) > 0 else None

