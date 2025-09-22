"""Technical Analysis utilities for both streaming and batch processors."""
import math
import pandas as pd
import numpy as np
from datetime import datetime, date, timezone
from typing import Dict, Any, Optional, List, Union, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def calculate_ta_metrics(df: pd.DataFrame, symbol: str = None) -> pd.DataFrame:
    """
    Calculate technical analysis metrics for a DataFrame with OHLCV data.
    
    Args:
        df: DataFrame with columns: timestamp, open, high, low, close, volume
             (must be sorted by timestamp ascending)
        symbol: Optional symbol name to add to the dataframe
        
    Returns:
        DataFrame with original data plus calculated TA metrics
    """
    if df.empty:
        logger.warning(f"Empty dataframe provided for {symbol}")
        return df
    
    # Ensure lowercase column names for consistency
    df.columns = [col.lower() for col in df.columns]
    
    # Convert timestamp to datetime if it's not already
    if 'timestamp' not in df.columns and 'date' in df.columns:
        df = df.rename(columns={'date': 'timestamp'})
        
    if not pd.api.types.is_datetime64_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Ensure required columns
    required_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    # Ensure data is sorted by timestamp
    df = df.sort_values('timestamp')
    
    # Store original index if needed
    original_index = df.index
    
    # Reset index for calculations
    df = df.reset_index(drop=True)
    
    # Add symbol if provided
    if symbol is not None and 'symbol' not in df.columns:
        df['symbol'] = symbol
    
    # --- Moving Averages ---
    df['sma_20'] = df['close'].rolling(window=20).mean()
    df['ema_12'] = df['close'].ewm(span=12, adjust=False).mean()
    df['ema_26'] = df['close'].ewm(span=26, adjust=False).mean()
    
    # --- MACD ---
    df['macd'] = df['ema_12'] - df['ema_26']
    df['macd_signal_9'] = df['macd'].ewm(span=9, adjust=False).mean()
    df['macd_hist'] = df['macd'] - df['macd_signal_9']
    
    # --- Bollinger Bands ---
    roll_20 = df['close'].rolling(window=20)
    df['boll_mid_20'] = roll_20.mean()
    df['boll_std_20'] = roll_20.std()
    df['boll_upper_20'] = df['boll_mid_20'] + 2 * df['boll_std_20']
    df['boll_lower_20'] = df['boll_mid_20'] - 2 * df['boll_std_20']
    
    # --- RSI (14-period Wilder's) ---
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    
    # After initial 14-period average
    for i in range(14, len(df)):
        avg_gain.iloc[i] = (avg_gain.iloc[i-1] * 13 + gain.iloc[i]) / 14
        avg_loss.iloc[i] = (avg_loss.iloc[i-1] * 13 + loss.iloc[i]) / 14
    
    rs = avg_gain / avg_loss
    df['rsi_14'] = 100 - (100 / (1 + rs))
    
    # --- ATR (14-period) ---
    tr1 = df['high'] - df['low']
    tr2 = abs(df['high'] - df['close'].shift())
    tr3 = abs(df['low'] - df['close'].shift())
    tr = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1)
    df['atr_14'] = tr.rolling(window=14).mean()
    
    # --- OBV (On-Balance Volume) ---
    df['obv'] = (np.sign(df['close'].diff()) * df['volume']).fillna(0).cumsum()
    
    # --- VWAP (Session) ---
    # Group by date for session VWAP
    df['date'] = df['timestamp'].dt.date
    
    # Calculate typical price and VWAP components
    df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
    df['vwap_num'] = df['typical_price'] * df['volume']
    
    # Group by date to calculate VWAP within each session
    vwap_data = []
    for date_val, group in df.groupby('date'):
        group = group.copy()
        group['vwap_num_cumsum'] = group['vwap_num'].cumsum()
        group['volume_cumsum'] = group['volume'].cumsum()
        group['vwap_session'] = group['vwap_num_cumsum'] / group['volume_cumsum'].replace(0, float('nan'))
        vwap_data.append(group)
    
    df = pd.concat(vwap_data)
    
    # --- Returns ---
    df['log_return'] = np.log(df['close'] / df['close'].shift(1))
    
    # Cumulative returns (log method)
    df['cum_return'] = np.exp(df['log_return'].cumsum()) - 1
    
    # Clean up columns we don't need anymore
    df.drop(['vwap_num', 'vwap_num_cumsum', 'volume_cumsum', 'typical_price'], 
             axis=1, errors='ignore', inplace=True)
    
    # Add ingested_at timestamp if it doesn't exist
    if 'ingested_at' not in df.columns:
        df['ingested_at'] = datetime.now(timezone.utc).isoformat()
    
    # Restore original index if needed
    if hasattr(original_index, 'name') and original_index.name is not None:
        df.set_index(original_index.name, inplace=True)
    
    return df

def market_tick_to_dict(df_row: pd.Series) -> Dict[str, Any]:
    """Convert a pandas Series (DataFrame row) to a dictionary for MongoDB."""
    row_dict = df_row.to_dict()
    
    # Handle timestamp conversion - ensure it's a datetime object
    if 'timestamp' in row_dict and not isinstance(row_dict['timestamp'], (datetime, pd.Timestamp)):
        row_dict['timestamp'] = pd.to_datetime(row_dict['timestamp'])
    
    # Convert pandas timestamp to datetime
    if 'timestamp' in row_dict and isinstance(row_dict['timestamp'], pd.Timestamp):
        row_dict['timestamp'] = row_dict['timestamp'].to_pydatetime()
    
    # Handle date field conversion
    if 'date' in row_dict and isinstance(row_dict['date'], date):
        row_dict['date'] = row_dict['date'].isoformat()
    
    # Convert NaN/None values to None for MongoDB compatibility
    for key, val in row_dict.items():
        if pd.isna(val):
            row_dict[key] = None
    
    return row_dict

def process_market_tick_json(tick_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a JSON tick from Kafka and prepare it for DataFrame conversion.
    
    Args:
        tick_json: A dictionary with market tick data
        
    Returns:
        Processed dictionary with standard format
    """
    # Standardize field names
    tick = {k.lower(): v for k, v in tick_json.items()}
    
    # Ensure timestamp field is parsed from ISO format if needed
    if 'timestamp' in tick and isinstance(tick['timestamp'], str):
        try:
            tick['timestamp'] = pd.to_datetime(tick['timestamp'])
        except ValueError as e:
            logger.error(f"Error parsing timestamp: {e}")
    
    # Handle other necessary conversions
    for field in ['open', 'high', 'low', 'close', 'volume']:
        if field in tick and isinstance(tick[field], str):
            try:
                tick[field] = float(tick[field])
            except ValueError:
                logger.error(f"Could not convert {field} to float: {tick[field]}")
    
    return tick