"""
Rule Engine Module
Evaluates trading conditions and generates signals
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from dataclasses import dataclass
import yaml


@dataclass
class Signal:
    """Trading signal data structure"""
    symbol: str
    signal_type: str  # 'BUY', 'SELL', 'ALERT'
    strategy: str
    price: float
    timestamp: datetime
    message: str
    confidence: float = 0.0
    metadata: Dict = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class RuleEngine:
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize the rule engine with configuration"""
        self.config = self._load_config(config_path)
        self._setup_logging()
        self.price_history = {}  # Store recent price history
        self.signals_history = []  # Store generated signals
        
        # Strategy parameters
        self.strategy_config = self.config['trading']['strategy']
        self.ma_period = self.strategy_config.get('period', 20)
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    
    def _setup_logging(self):
        """Setup logging configuration"""
        self.logger = logging.getLogger(__name__)
    
    def update_price_history(self, symbol: str, historical_data: pd.DataFrame):
        """Update internal price history for a symbol"""
        if historical_data is not None and not historical_data.empty:
            self.price_history[symbol] = historical_data.copy()
            self.logger.info(f"Updated price history for {symbol}: {len(historical_data)} bars")
    
    def calculate_moving_average(self, symbol: str, period: int = None) -> Optional[float]:
        """Calculate moving average for a symbol"""
        if period is None:
            period = self.ma_period
            
        if symbol not in self.price_history:
            self.logger.warning(f"No price history available for {symbol}")
            return None
        
        df = self.price_history[symbol]
        if len(df) < period:
            self.logger.warning(f"Insufficient data for MA{period} calculation for {symbol}")
            return None
        
        # Calculate moving average using close prices
        ma = df['close'].tail(period).mean()
        self.logger.debug(f"{symbol} MA{period}: {ma:.2f}")
        return ma
    
    def calculate_rsi(self, symbol: str, period: int = 14) -> Optional[float]:
        """Calculate RSI (Relative Strength Index)"""
        if symbol not in self.price_history:
            return None
        
        df = self.price_history[symbol]
        if len(df) < period + 1:
            return None
        
        # Calculate price changes
        delta = df['close'].diff()
        
        # Separate gains and losses
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        # Calculate RSI
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else None
    
    def calculate_price_change(self, symbol: str, periods: int = 1) -> Optional[Tuple[float, float]]:
        """Calculate price change over specified periods"""
        if symbol not in self.price_history:
            return None
        
        df = self.price_history[symbol]
        if len(df) < periods + 1:
            return None
        
        current_price = df['close'].iloc[-1]
        previous_price = df['close'].iloc[-1 - periods]
        
        change = current_price - previous_price
        change_pct = (change / previous_price) * 100
        
        return change, change_pct
    
    def check_moving_average_crossover(self, symbol: str, current_price: float) -> Optional[Signal]:
        """Check if price crosses above moving average"""
        ma = self.calculate_moving_average(symbol)
        if ma is None:
            return None
        
        # Check if we have previous price to determine crossover
        if symbol not in self.price_history or len(self.price_history[symbol]) < 2:
            return None
        
        df = self.price_history[symbol]
        previous_price = df['close'].iloc[-2]
        
        # Bullish crossover: price was below MA, now above
        if previous_price <= ma and current_price > ma:
            confidence = min(((current_price - ma) / ma) * 100, 10.0)  # Max 10% confidence
            
            signal = Signal(
                symbol=symbol,
                signal_type='BUY',
                strategy='MA_Crossover',
                price=current_price,
                timestamp=datetime.now(),
                message=f"{symbol} crossed above MA{self.ma_period}: ${current_price:.2f} > ${ma:.2f}",
                confidence=confidence,
                metadata={
                    'ma_value': ma,
                    'ma_period': self.ma_period,
                    'crossover_type': 'bullish'
                }
            )
            
            self.logger.info(f"🟢 BULLISH SIGNAL: {signal.message}")
            return signal
        
        # Bearish crossover: price was above MA, now below
        elif previous_price >= ma and current_price < ma:
            confidence = min(((ma - current_price) / ma) * 100, 10.0)
            
            signal = Signal(
                symbol=symbol,
                signal_type='SELL',
                strategy='MA_Crossover',
                price=current_price,
                timestamp=datetime.now(),
                message=f"{symbol} crossed below MA{self.ma_period}: ${current_price:.2f} < ${ma:.2f}",
                confidence=confidence,
                metadata={
                    'ma_value': ma,
                    'ma_period': self.ma_period,
                    'crossover_type': 'bearish'
                }
            )
            
            self.logger.info(f"🔴 BEARISH SIGNAL: {signal.message}")
            return signal
        
        return None
    
    def check_volume_spike(self, symbol: str) -> Optional[Signal]:
        """Check for unusual volume spikes"""
        if symbol not in self.price_history:
            return None
        
        df = self.price_history[symbol]
        if len(df) < 20:  # Need at least 20 bars for volume average
            return None
        
        current_volume = df['volume'].iloc[-1]
        avg_volume = df['volume'].tail(20).mean()
        
        # Volume spike if current volume is 2x average
        if current_volume > avg_volume * 2:
            current_price = df['close'].iloc[-1]
            volume_ratio = current_volume / avg_volume
            
            signal = Signal(
                symbol=symbol,
                signal_type='ALERT',
                strategy='Volume_Spike',
                price=current_price,
                timestamp=datetime.now(),
                message=f"{symbol} volume spike: {volume_ratio:.1f}x average volume",
                confidence=min(volume_ratio * 2, 10.0),
                metadata={
                    'current_volume': current_volume,
                    'average_volume': avg_volume,
                    'volume_ratio': volume_ratio
                }
            )
            
            self.logger.info(f"📊 VOLUME SPIKE: {signal.message}")
            return signal
        
        return None
    
    def check_price_breakout(self, symbol: str, current_price: float) -> Optional[Signal]:
        """Check for price breakouts (simple high/low break)"""
        if symbol not in self.price_history:
            return None
        
        df = self.price_history[symbol]
        if len(df) < 20:
            return None
        
        # Calculate 20-period high and low
        period_high = df['high'].tail(20).max()
        period_low = df['low'].tail(20).min()
        
        # Breakout above resistance
        if current_price > period_high:
            signal = Signal(
                symbol=symbol,
                signal_type='BUY',
                strategy='Breakout',
                price=current_price,
                timestamp=datetime.now(),
                message=f"{symbol} breakout above resistance: ${current_price:.2f} > ${period_high:.2f}",
                confidence=5.0,
                metadata={
                    'resistance_level': period_high,
                    'breakout_type': 'upward'
                }
            )
            
            self.logger.info(f"⬆️ BREAKOUT: {signal.message}")
            return signal
        
        # Breakdown below support
        elif current_price < period_low:
            signal = Signal(
                symbol=symbol,
                signal_type='SELL',
                strategy='Breakout',
                price=current_price,
                timestamp=datetime.now(),
                message=f"{symbol} breakdown below support: ${current_price:.2f} < ${period_low:.2f}",
                confidence=5.0,
                metadata={
                    'support_level': period_low,
                    'breakout_type': 'downward'
                }
            )
            
            self.logger.info(f"⬇️ BREAKDOWN: {signal.message}")
            return signal
        
        return None
    
    def evaluate_all_rules(self, current_prices: Dict[str, float]) -> List[Signal]:
        """Evaluate all trading rules for all symbols"""
        signals = []
        
        for symbol, price in current_prices.items():
            self.logger.debug(f"Evaluating rules for {symbol} at ${price:.2f}")
            
            # Check moving average crossover
            ma_signal = self.check_moving_average_crossover(symbol, price)
            if ma_signal:
                signals.append(ma_signal)
            
            # Check volume spike
            volume_signal = self.check_volume_spike(symbol)
            if volume_signal:
                signals.append(volume_signal)
            
            # Check price breakout
            breakout_signal = self.check_price_breakout(symbol, price)
            if breakout_signal:
                signals.append(breakout_signal)
        
        # Store signals in history
        self.signals_history.extend(signals)
        
        # Keep only last 100 signals
        if len(self.signals_history) > 100:
            self.signals_history = self.signals_history[-100:]
        
        return signals
    
    def get_market_summary(self, current_prices: Dict[str, float]) -> Dict[str, any]:
        """Generate market summary for all tracked symbols"""
        summary = {
            'timestamp': datetime.now(),
            'symbols': {}
        }
        
        for symbol, price in current_prices.items():
            ma = self.calculate_moving_average(symbol)
            rsi = self.calculate_rsi(symbol)
            price_change = self.calculate_price_change(symbol, 5)  # 5-period change
            
            symbol_data = {
                'current_price': price,
                'moving_average': ma,
                'rsi': rsi,
                'price_change_5period': price_change,
                'trend': 'ABOVE_MA' if ma and price > ma else 'BELOW_MA' if ma else 'NO_DATA'
            }
            
            summary['symbols'][symbol] = symbol_data
        
        return summary


def main():
    """Test the rule engine"""
    from data_collector import DataCollector
    
    # Initialize components
    collector = DataCollector()
    engine = RuleEngine()
    
    if collector.connect():
        print("✅ Connected to Interactive Brokers")
        
        # Get historical data for analysis
        symbols = collector.config['trading']['symbols']
        for symbol in symbols:
            print(f"📊 Getting historical data for {symbol}...")
            hist_data = collector.get_historical_data(symbol, "2 D", "5 mins")
            if hist_data is not None:
                engine.update_price_history(symbol, hist_data)
                print(f"✅ Updated {symbol} history: {len(hist_data)} bars")
        
        # Get current prices and evaluate rules
        print("\n🔍 Evaluating trading rules...")
        current_prices = collector.get_current_prices()
        signals = engine.evaluate_all_rules(current_prices)
        
        if signals:
            print(f"\n🚨 Generated {len(signals)} signals:")
            for signal in signals:
                print(f"  {signal.signal_type} {signal.symbol}: {signal.message}")
        else:
            print("\n✅ No signals generated")
        
        # Show market summary
        print("\n📈 Market Summary:")
        summary = engine.get_market_summary(current_prices)
        for symbol, data in summary['symbols'].items():
            trend = data['trend']
            price = data['current_price']
            ma = data['moving_average']
            print(f"  {symbol}: ${price:.2f} | MA: ${ma:.2f if ma else 0:.2f} | {trend}")
        
        collector.disconnect()
        print("\n✅ Test completed")
    else:
        print("❌ Failed to connect to Interactive Brokers")


if __name__ == "__main__":
    main()