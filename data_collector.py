"""
Data Collector Module
Handles connection to Interactive Brokers and retrieves market data
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd
from ib_insync import IB, Stock, util
import yaml


class DataCollector:
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize the data collector with configuration"""
        self.config = self._load_config(config_path)
        self.ib = IB()
        self._setup_logging()
        self.connected = False
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            logging.error(f"Config file {config_path} not found")
            raise
        except yaml.YAMLError as e:
            logging.error(f"Error parsing config file: {e}")
            raise
    
    def _setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=getattr(logging, self.config['logging']['level']),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.config['logging']['file']),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> bool:
        """Connect to Interactive Brokers"""
        try:
            ib_config = self.config['ib']
            self.ib.connect(
                host=ib_config['host'],
                port=ib_config['port'],
                clientId=ib_config['client_id'],
                timeout=ib_config['timeout']
            )
            self.connected = True
            self.logger.info("Successfully connected to Interactive Brokers")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to IB: {e}")
            self.connected = False
            return False
    
    def disconnect(self):
        """Disconnect from Interactive Brokers"""
        if self.connected:
            self.ib.disconnect()
            self.connected = False
            self.logger.info("Disconnected from Interactive Brokers")
    
    def get_current_prices(self) -> Dict[str, float]:
        """Get current market prices for configured symbols"""
        if not self.connected:
            self.logger.error("Not connected to IB. Call connect() first.")
            return {}
        
        prices = {}
        symbols = self.config['trading']['symbols']
        
        for symbol in symbols:
            try:
                # Create stock contract
                stock = Stock(symbol, 'SMART', 'USD')
                
                # Request market data
                self.ib.reqMktData(stock, '', False, False)
                
                # Wait for data
                self.ib.sleep(2)
                
                # Get ticker
                ticker = self.ib.ticker(stock)
                
                if ticker and ticker.marketPrice():
                    prices[symbol] = ticker.marketPrice()
                    self.logger.info(f"{symbol}: ${ticker.marketPrice():.2f}")
                else:
                    self.logger.warning(f"No price data available for {symbol}")
                
                # Cancel market data to avoid unnecessary data fees
                self.ib.cancelMktData(stock)
                
            except Exception as e:
                self.logger.error(f"Error getting price for {symbol}: {e}")
        
        return prices
    
    def get_historical_data(self, symbol: str, duration: str, 
                            bar_size: str) -> Optional[pd.DataFrame]:
        """
        Get historical data for a symbol
        
        Args:
            symbol: Stock symbol (e.g., 'SPY')
            duration: Duration of data (e.g., '1 D', '5 D', '1 W')
            bar_size: Bar size (e.g., '1 min', '5 mins', '1 hour')
        
        Returns:
            DataFrame with OHLCV data or None if error
        """
        if not self.connected:
            self.logger.error("Not connected to IB. Call connect() first.")
            return None
        
        try:
            # Create stock contract
            stock = Stock(symbol, 'SMART', 'USD')
            
            # Request historical data
            bars = self.ib.reqHistoricalData(
                stock,
                endDateTime='',
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow='TRADES',
                useRTH=True,
                formatDate=1
            )
            
            if bars:
                # Convert to DataFrame
                df = util.df(bars)
                df['symbol'] = symbol
                self.logger.info(f"Retrieved {len(df)} bars for {symbol}")
                return df
            else:
                self.logger.warning(f"No historical data available for {symbol}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error getting historical data for {symbol}: {e}")
            return None
    
    def test_connection(self) -> bool:
        """Test the IB connection by getting account info"""
        if not self.connected:
            return False
        
        try:
            account_values = self.ib.accountValues()
            if account_values:
                self.logger.info("Connection test successful")
                return True
            else:
                self.logger.warning("Connection test failed - no account data")
                return False
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False


def main():
    """Test the data collector"""
    collector = DataCollector()
    
    # Test connection
    if collector.connect():
        print("✅ Connected to Interactive Brokers")
        
        # Test connection
        if collector.test_connection():
            print("✅ Connection test passed")
        
        # Get current prices
        print("\n📊 Current Prices:")
        prices = collector.get_current_prices()
        for symbol, price in prices.items():
            print(f"{symbol}: ${price:.2f}")
        
        # Get historical data for first symbol
        if prices:
            symbol = list(prices.keys())[0]
            print(f"\n📈 Historical data for {symbol}:")
            df = collector.get_historical_data(symbol, "20 D", "1 day")
            if df is not None:
                print(f"Retrieved {len(df)} bars")
                print(df.tail())
        
        collector.disconnect()
        print("✅ Disconnected successfully")
    else:
        print("❌ Failed to connect to Interactive Brokers")
        print("Make sure TWS or IB Gateway is running on port 7497")


if __name__ == "__main__":
    main()