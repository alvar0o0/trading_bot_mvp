"""
Notifier Module
Handles sending trading alerts via multiple channels (Telegram, Email, etc.)
"""

import logging
import json
from datetime import datetime
from typing import List, Dict, Optional
import requests
import yaml
from rule_engine import Signal


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str):
        """Initialize Telegram notifier"""
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self.logger = logging.getLogger(__name__)
        
    def send_message(self, message: str, parse_mode: str = "HTML") -> bool:
        """Send a message via Telegram"""
        url = f"{self.base_url}/sendMessage"
        
        payload = {
            'chat_id': self.chat_id,
            'text': message,
            'parse_mode': parse_mode,
            'disable_web_page_preview': True
        }
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            
            self.logger.info("✅ Telegram message sent successfully")
            return True
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ Failed to send Telegram message: {e}")
            return False
    
    def test_connection(self) -> bool:
        """Test Telegram bot connection"""
        url = f"{self.base_url}/getMe"
        
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            bot_info = response.json()
            if bot_info.get('ok'):
                bot_name = bot_info['result']['first_name']
                self.logger.info(f"✅ Telegram bot connected: {bot_name}")
                return True
            else:
                self.logger.error("❌ Telegram bot test failed")
                return False
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ Telegram connection test failed: {e}")
            return False


class NotificationFormatter:
    """Formats trading signals into readable messages"""
    
    @staticmethod
    def format_signal_telegram(signal: Signal) -> str:
        """Format a signal for Telegram with HTML formatting"""
        
        # Emoji mapping for signal types
        emoji_map = {
            'BUY': '🟢',
            'SELL': '🔴',
            'ALERT': '🟡'
        }
        
        emoji = emoji_map.get(signal.signal_type, '⚪')
        
        # Format timestamp
        time_str = signal.timestamp.strftime("%H:%M:%S")
        
        # Basic message
        message = f"{emoji} <b>{signal.signal_type}</b> - {signal.symbol}\n"
        message += f"💰 Price: <b>${signal.price:.2f}</b>\n"
        message += f"📊 Strategy: {signal.strategy}\n"
        message += f"⏰ Time: {time_str}\n"
        
        # Add confidence if available
        if signal.confidence > 0:
            confidence_stars = "⭐" * min(int(signal.confidence), 5)
            message += f"🎯 Confidence: {signal.confidence:.1f} {confidence_stars}\n"
        
        # Add strategy-specific details
        if signal.metadata:
            message += "\n📋 <b>Details:</b>\n"
            
            if signal.strategy == 'MA_Crossover':
                ma_value = signal.metadata.get('ma_value')
                ma_period = signal.metadata.get('ma_period')
                crossover_type = signal.metadata.get('crossover_type', '').title()
                
                if ma_value and ma_period:
                    message += f"📈 MA{ma_period}: ${ma_value:.2f}\n"
                    message += f"🔄 Type: {crossover_type} Crossover\n"
            
            elif signal.strategy == 'Volume_Spike':
                volume_ratio = signal.metadata.get('volume_ratio')
                if volume_ratio:
                    message += f"📊 Volume: {volume_ratio:.1f}x average\n"
            
            elif signal.strategy == 'Breakout':
                breakout_type = signal.metadata.get('breakout_type', '').title()
                level_key = 'resistance_level' if 'resistance' in signal.metadata else 'support_level'
                level = signal.metadata.get(level_key)
                
                if level:
                    level_name = 'Resistance' if 'resistance' in level_key else 'Support'
                    message += f"🎯 {level_name}: ${level:.2f}\n"
                    message += f"📊 Direction: {breakout_type}\n"
        
        # Add the main message
        message += f"\n💬 <i>{signal.message}</i>"
        
        return message
    
    @staticmethod
    def format_market_summary_telegram(summary: Dict) -> str:
        """Format market summary for Telegram"""
        timestamp = summary['timestamp'].strftime("%H:%M:%S")
        
        message = f"📊 <b>Market Summary</b> - {timestamp}\n\n"
        
        for symbol, data in summary['symbols'].items():
            price = data['current_price']
            ma = data.get('moving_average')
            trend = data.get('trend', 'NO_DATA')
            rsi = data.get('rsi')
            
            # Trend emoji
            trend_emoji = {
                'ABOVE_MA': '📈',
                'BELOW_MA': '📉',
                'NO_DATA': '⚪'
            }.get(trend, '⚪')
            
            message += f"{trend_emoji} <b>{symbol}</b>: ${price:.2f}\n"
            
            if ma:
                message += f"   MA20: ${ma:.2f} ({trend.replace('_', ' ').title()})\n"
            
            if rsi:
                rsi_status = "Overbought" if rsi > 70 else "Oversold" if rsi < 30 else "Neutral"
                message += f"   RSI: {rsi:.1f} ({rsi_status})\n"
            
            message += "\n"
        
        return message.strip()
    
    @staticmethod
    def format_system_status(status: Dict) -> str:
        """Format system status message"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        message = f"⚙️ <b>System Status</b> - {timestamp}\n\n"
        
        # Connection status
        ib_status = "🟢 Connected" if status.get('ib_connected') else "🔴 Disconnected"
        message += f"📊 Interactive Brokers: {ib_status}\n"
        
        # Monitoring status
        symbols = status.get('monitored_symbols', [])
        message += f"👁️ Monitoring: {', '.join(symbols)}\n"
        
        # Recent activity
        signals_count = status.get('recent_signals', 0)
        message += f"🚨 Recent Signals: {signals_count}\n"
        
        # Uptime
        uptime = status.get('uptime', 'Unknown')
        message += f"⏱️ Uptime: {uptime}\n"
        
        return message


class Notifier:
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize the notification system"""
        self.config = self._load_config(config_path)
        self._setup_logging()
        
        # Initialize Telegram notifier
        telegram_config = self.config['telegram']
        self.telegram = TelegramNotifier(
            bot_token=telegram_config['bot_token'],
            chat_id=telegram_config['chat_id']
        )
        
        self.formatter = NotificationFormatter()
        self.sent_signals = []  # Track sent signals to avoid duplicates
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    
    def _setup_logging(self):
        """Setup logging configuration"""
        self.logger = logging.getLogger(__name__)
    
    def test_connections(self) -> Dict[str, bool]:
        """Test all notification channels"""
        results = {}
        
        # Test Telegram
        results['telegram'] = self.telegram.test_connection()
        
        return results
    
    def send_signal(self, signal: Signal) -> bool:
        """Send a trading signal notification"""
        # Check for duplicate signals (same symbol, strategy, type within 5 minutes)
        signal_key = f"{signal.symbol}_{signal.strategy}_{signal.signal_type}"
        current_time = signal.timestamp
        
        # Remove old entries (older than 5 minutes)
        self.sent_signals = [
            (key, timestamp) for key, timestamp in self.sent_signals
            if (current_time - timestamp).total_seconds() < 300
        ]
        
        # Check if we already sent this type of signal recently
        if any(key == signal_key for key, _ in self.sent_signals):
            self.logger.info(f"Skipping duplicate signal: {signal_key}")
            return False
        
        # Format and send message
        message = self.formatter.format_signal_telegram(signal)
        success = self.telegram.send_message(message)
        
        if success:
            # Track sent signal
            self.sent_signals.append((signal_key, current_time))
            self.logger.info(f"📤 Signal sent: {signal.symbol} {signal.signal_type}")
        
        return success
    
    def send_signals_batch(self, signals: List[Signal]) -> int:
        """Send multiple signals"""
        sent_count = 0
        
        for signal in signals:
            if self.send_signal(signal):
                sent_count += 1
        
        if sent_count > 0:
            self.logger.info(f"📤 Sent {sent_count}/{len(signals)} signals")
        
        return sent_count
    
    def send_market_summary(self, summary: Dict) -> bool:
        """Send market summary"""
        message = self.formatter.format_market_summary_telegram(summary)
        success = self.telegram.send_message(message)
        
        if success:
            self.logger.info("📤 Market summary sent")
        
        return success
    
    def send_system_status(self, status: Dict) -> bool:
        """Send system status notification"""
        message = self.formatter.format_system_status(status)
        success = self.telegram.send_message(message)
        
        if success:
            self.logger.info("📤 System status sent")
        
        return success
    
    def send_startup_message(self) -> bool:
        """Send system startup notification"""
        message = (
            "🚀 <b>Trading Bot Started</b>\n\n"
            f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"📊 Monitoring: {', '.join(self.config['trading']['symbols'])}\n"
            f"📈 Strategy: {self.config['trading']['strategy']['type']}\n"
            f"🔄 MA Period: {self.config['trading']['strategy']['period']}\n\n"
            "✅ Bot is now monitoring the markets!"
        )
        
        success = self.telegram.send_message(message)
        
        if success:
            self.logger.info("📤 Startup message sent")
        
        return success
    
    def send_shutdown_message(self) -> bool:
        """Send system shutdown notification"""
        message = (
            "🛑 <b>Trading Bot Stopped</b>\n\n"
            f"⏰ Stopped at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            "💤 Bot monitoring has been paused."
        )
        
        success = self.telegram.send_message(message)
        
        if success:
            self.logger.info("📤 Shutdown message sent")
        
        return success


def main():
    """Test the notifier"""
    from rule_engine import Signal
    
    # Initialize notifier
    notifier = Notifier()
    
    # Test connections
    print("🔍 Testing notification channels...")
    results = notifier.test_connections()
    
    for channel, success in results.items():
        status = "✅ Working" if success else "❌ Failed"
        print(f"  {channel.title()}: {status}")
    
    # Test startup message
    if results.get('telegram'):
        print("\n📤 Sending startup message...")
        notifier.send_startup_message()
        
        # Test sample signal
        print("📤 Sending test signal...")
        test_signal = Signal(
            symbol="SPY",
            signal_type="BUY",
            strategy="MA_Crossover",
            price=445.67,
            timestamp=datetime.now(),
            message="SPY crossed above MA20: $445.67 > $444.15",
            confidence=7.5,
            metadata={
                'ma_value': 444.15,
                'ma_period': 20,
                'crossover_type': 'bullish'
            }
        )
        
        notifier.send_signal(test_signal)
        
        # Test market summary
        print("📤 Sending market summary...")
        test_summary = {
            'timestamp': datetime.now(),
            'symbols': {
                'SPY': {
                    'current_price': 445.67,
                    'moving_average': 444.15,
                    'rsi': 65.4,
                    'trend': 'ABOVE_MA'
                },
                'QQQ': {
                    'current_price': 378.23,
                    'moving_average': 379.45,
                    'rsi': 45.2,
                    'trend': 'BELOW_MA'
                }
            }
        }
        
        notifier.send_market_summary(test_summary)
        
        print("✅ Test completed! Check your Telegram for messages.")
    else:
        print("❌ Telegram not working, skipping message tests")


if __name__ == "__main__":
    main()