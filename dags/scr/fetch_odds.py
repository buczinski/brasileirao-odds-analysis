import os
import logging
from datetime import datetime

import pandas as pd
import pytz
import requests
import fsspec  


logger = logging.getLogger(__name__)


def _is_s3(path: str) -> bool:
    return str(path).startswith("s3://")


def _exists(path: str) -> bool:
    return fsspec.filesystem("s3").exists(path) if _is_s3(path) else os.path.exists(path)


def fetch_odds(api_key: str, output_path: str):
    """
    Fetch odds, merge with existing Parquet at output_path (S3), and save back.
    Assumes output_path is an S3 URI. Returns the combined DataFrame or None.
    """
    logger.info("Starting odds fetch from API.")
    try:
        resp = requests.get(
            'https://api.the-odds-api.com/v4/sports/soccer_brazil_campeonato/odds/',
            params={
                'apiKey': api_key,
                'regions': 'eu',
                'markets': 'h2h',
                'oddsFormat': 'decimal',
            },
            timeout=25,
        )
    except requests.RequestException as e:
        logger.error("Request to odds API failed: %s", e, exc_info=True)
        return None

    if resp.status_code != 200:
        logger.error("API returned status %s: %s", resp.status_code, resp.text[:250])
        return None

    try:
        odds_payload = resp.json()
    except ValueError:
        logger.error("Failed to decode JSON from API response.")
        return None

    if not isinstance(odds_payload, list):
        logger.error("Unexpected API payload type: %s", type(odds_payload))
        return None

    logger.info("Fetched %d raw matches from API.", len(odds_payload))

    matches_dict = process_odds_data(odds_payload)
    new_df = pd.DataFrame(list(matches_dict.values()))

    if new_df.empty:
        logger.warning("No rows after processing.")
        return None

    required_cols = ['home_win_odds', 'draw_odds', 'away_win_odds']
    new_df = new_df.dropna(subset=required_cols)

    if new_df.empty:
        logger.warning("All processed rows missing required odds fields; nothing to save.")
        return None

    logger.info("Processed %d matches with complete odds.", len(new_df))

    # Normalize dtypes
    if 'match_id' in new_df.columns:
        new_df['match_id'] = new_df['match_id'].astype('string')
    if 'commence_time' in new_df.columns:
        ct = pd.to_datetime(new_df['commence_time'], errors='coerce')
        # Remove timezone to keep Parquet schema stable
        try:
            ct = ct.dt.tz_localize(None)
        except Exception:
            pass
        new_df['commence_time'] = ct

    # Load existing Parquet (append-only)
    try:
        existing_df = pd.read_parquet(output_path, engine='pyarrow')
        logger.debug("Existing Parquet loaded: %d rows.", len(existing_df))
    except FileNotFoundError:
        logger.info("No existing Parquet at %s; creating new file.", output_path)
        existing_df = pd.DataFrame()
    except Exception as e:
        logger.warning("Failed reading existing Parquet (%s). Recreating file.", e)
        existing_df = pd.DataFrame()

    if not existing_df.empty and 'match_id' in existing_df.columns:
        existing_df['match_id'] = existing_df['match_id'].astype('string')
        existing_ids = set(existing_df['match_id'])
        add_df = new_df[~new_df['match_id'].isin(existing_ids)]
        logger.info("New matches to append: %d (existing kept: %d).", len(add_df), len(existing_df))
        combined_df = pd.concat([existing_df, add_df], ignore_index=True)
    else:
        combined_df = new_df
        logger.info("Initialized Parquet with %d rows.", len(combined_df))

    # Persist
    try:
        combined_df.to_parquet(output_path, index=False, engine='pyarrow')
        logger.info("Wrote %d total rows to %s.", len(combined_df), output_path)
    except Exception as e:
        logger.error("Failed writing Parquet to %s: %s", output_path, e, exc_info=True)
        return None

    return combined_df


def process_odds_data(odds_payload):
    """
    Transform the odds API payload into a dict of match_id -> row with William Hill odds.
    """
    tz_br = pytz.timezone('America/Sao_Paulo')
    rows = {}

    for match in odds_payload:
        match_id = match.get('id')
        home_team = match.get('home_team')
        away_team = match.get('away_team')
        commence = match.get('commence_time')

        if not (match_id and home_team and away_team and commence):
            logger.debug("Skipping match with missing core fields: %s", match)
            continue

        try:
            utc_dt = datetime.fromisoformat(commence.replace('Z', '+00:00'))
            br_dt = utc_dt.astimezone(tz_br)
            kickoff_str = br_dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            kickoff_str = commence

        entry = rows.setdefault(match_id, {
            'match_id': str(match_id),
            'home_team': home_team,
            'away_team': away_team,
            'commence_time': kickoff_str,
        })

        for bookmaker in match.get('bookmakers', []):
            if bookmaker.get('title') != "William Hill":
                continue
            odds_triplet = extract_market_odds(bookmaker, home_team, away_team)
            entry['home_win_odds'] = odds_triplet['home']
            entry['draw_odds'] = odds_triplet['draw']
            entry['away_win_odds'] = odds_triplet['away']

    return rows


def extract_market_odds(bookmaker, home_team, away_team):
    """
    Extract h2h odds (home/draw/away) from a single bookmaker dict.
    """
    home_odds = draw_odds = away_odds = None
    for market in bookmaker.get('markets', []):
        if market.get('key') != 'h2h':
            continue
        for outcome in market.get('outcomes', []):
            name = outcome.get('name')
            price = outcome.get('price')
            if name == home_team:
                home_odds = price
            elif name == away_team:
                away_odds = price
            elif name == 'Draw':
                draw_odds = price
    return {'home': home_odds, 'draw': draw_odds, 'away': away_odds}
