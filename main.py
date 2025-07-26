"""
Trading Bot MVP - Main Application
Coordinates data collection, rule evaluation, and notifications
"""

import time
import signal
import sys
import logging
from datetime import datetime, timedelta
from typing import Dict, List
import yaml
import traceback

from data_collector import DataCollector
from rule_engine import RuleEngine
from notifier import Notifier


class TradingBot:
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize the trading bot"""
        self.config_path = config_path
        self.config = self._load_config()
        self._setup_logging()
        
        # Initialize components
        self.data_collector = DataCollector(config_path)
        self.rule_engine = RuleEngine(config_path)
        self.notifier = Notifier(config_path)
        
        # Bot state
        self.running = False
        self.start_time = None
        self.last_data_update = None
        self.cycle_count = 0
        self.error_count = 0
        self.last_summary_time = None
        
        # Performance tracking
        self.stats = {
            'total_signals': 0,
            'signals_sent': 0,
            'data_updates': 0,
            'errors': 0,
            'uptime': timedelta()
        }
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _load_config(self) -> dict:
        """Load configuration from YAML file"""
        try:
            with open(self.config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            print(f"❌ Failed to load config: {e}")
            sys.exit(1)
    
    def _setup_logging(self):
        """Setup logging configuration"""
        log_config = self.config.get('logging', {})
        log_level = log_config.get('level', 'INFO')
        log_file = log_config.get('file', 'logs/trading_bot.log')
        
        # Create logs directory if it doesn't exist
        import os
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("🤖 Trading Bot initialized")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"🛑 Received shutdown signal ({signum})")
        self.stop()
    
    def _get_uptime(self) -> str:
        """Get formatted uptime string"""
        if not self.start_time:
            return "Not running"
        
        uptime = datetime.now() - self.start_time
        hours, remainder = divmod(int(uptime.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"
    
    def _connect_to_ib(self) -> bool:
        """Connect to Interactive Brokers with retry logic"""
        max_retries = 3
        retry_delay = 5  # seconds
        
        for attempt in range(1, max_retries + 1):
            self.logger.info(f"🔌 Connecting to IB (attempt {attempt}/{max_retries})...")
            
            if self.data_collector.connect():
                self.logger.info("✅ Successfully connected to Interactive Brokers")
                return True
            else:
                if attempt < max_retries:
                    self.logger.warning(f"⚠️ Connection failed, retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    self.logger.error("❌ Failed to connect to IB after all retries")
        
        return False
    
    def _initialize_historical_data(self) -> bool:
        """Load historical data for all symbols"""
        symbols = self.config['trading']['symbols']
        strategy_config = self.config['trading']['strategy']
        
        self.logger.info(f"📊 Loading historical data for {len(symbols)} symbols...")
        
        success_count = 0
        for symbol in symbols:
            try:
                # Get enough historical data for technical analysis
                hist_data = self.data_collector.get_historical_data(
                    symbol=symbol,
                    duration="5 D",  # 5 days of data
                    bar_size=strategy_config.get('timeframe', '1min')
                )
                
                if hist_data is not None and not hist_data.empty:
                    self.rule_engine.update_price_history(symbol, hist_data)
                    success_count += 1
                    self.logger.info(f"✅ {symbol}: {len(hist_data)} bars loaded")
                else:
                    self.logger.warning(f"⚠️ No historical data for {symbol}")
                    
            except Exception as e:
                self.logger.error(f"❌ Failed to load data for {symbol}: {e}")
        
        if success_count == 0:
            self.logger.error("❌ No historical data loaded for any symbol")
            return False
        
        self.logger.info(f"✅ Historical data loaded for {success_count}/{len(symbols)} symbols")
        return True
    
    def _run_trading_cycle(self) -> bool:
        """Execute one complete trading cycle"""
        try:
            self.cycle_count += 1
            cycle_start = datetime.now()
            
            self.logger.debug(f"🔄 Starting cycle #{self.cycle_count}")
            
            # 1. Get current market prices
            current_prices = self.data_collector.get_current_prices()
            if not current_prices:
                self.logger.warning("⚠️ No current prices received")
                return False
            
            self.stats['data_updates'] += 1
            self.last_data_update = datetime.now()
            
            # Log current prices
            price_str = ", ".join([f"{sym}: ${price:.2f}" for sym, price in current_prices.items()])
            self.logger.debug(f"💰 Current prices: {price_str}")
            
            # 2. Evaluate trading rules
            signals = self.rule_engine.evaluate_all_rules(current_prices)
            self.stats['total_signals'] += len(signals)
            
            # 3. Send notifications for any signals
            if signals:
                self.logger.info(f"🚨 Generated {len(signals)} signals")
                
                sent_count = self.notifier.send_signals_batch(signals)
                self.stats['signals_sent'] += sent_count
                
                # Log each signal
                for signal in signals:
                    self.logger.info(f"📊 {signal.signal_type} {signal.symbol}: {signal.message}")
            
            # 4. Send periodic market summary (every 30 minutes)
            if self._should_send_summary():
                summary = self.rule_engine.get_market_summary(current_prices)
                if self.notifier.send_market_summary(summary):
                    self.last_summary_time = datetime.now()
                    self.logger.info("📈 Market summary sent")
            
            # 5. Update performance stats
            cycle_duration = (datetime.now() - cycle_start).total_seconds()
            self.logger.debug(f"✅ Cycle #{self.cycle_count} completed in {cycle_duration:.2f}s")
            
            return True
            
        except Exception as e:
            self.error_count += 1
            self.stats['errors'] += 1
            self.logger.error(f"❌ Error in trading cycle: {e}")
            self.logger.debug(traceback.format_exc())
            return False
    
    def _should_send_summary(self) -> bool:
        """Check if we should send a market summary"""
        if not self.last_summary_time:
            return True  # Send on first cycle
        
        # Send summary every 30 minutes
        time_since_summary = datetime.now() - self.last_summary_time
        return time_since_summary.total_seconds() >= 1800  # 30 minutes
    
    def _get_system_status(self) -> Dict:
        """Get current system status"""
        return {
            'ib_connected': self.data_collector.connected,
            'monitored_symbols': self.config['trading']['symbols'],
            'recent_signals': self.stats['signals_sent'],
            'uptime': self._get_uptime(),
            'cycle_count': self.cycle_count,
            'error_count': self.error_count,
            'last_update': self.last_data_update.strftime('%H:%M:%S') if self.last_data_update else 'Never'
        }
    
    def start(self) -> bool:
        """Start the trading bot"""
        self.logger.info("🚀 Starting Trading Bot...")
        self.start_time = datetime.now()
        
        # Test notification connections
        self.logger.info("🔍 Testing notification channels...")
        notification_tests = self.notifier.test_connections()
        
        for channel, success in notification_tests.items():
            status = "✅" if success else "❌"
            self.logger.info(f"  {channel.title()}: {status}")
        
        if not any(notification_tests.values()):
            self.logger.error("❌ No notification channels working!")
            return False
        
        # Connect to Interactive Brokers
        if not self._connect_to_ib():
            return False
        
        # Load historical data
        if not self._initialize_historical_data():
            self.logger.error("❌ Failed to initialize historical data")
            return False
        
        # Send startup notification
        self.notifier.send_startup_message()
        
        # Start main loop
        self.running = True
        self.logger.info("✅ Trading Bot started successfully!")
        
        return True
    
    def run(self):
        """Main execution loop"""
        if not self.start():
            self.logger.error("❌ Failed to start trading bot")
            return
        
        # Get cycle interval from config (default: 60 seconds)
        cycle_interval = self.config.get('bot', {}).get('cycle_interval', 60)
        
        self.logger.info(f"🔄 Starting main loop (cycle interval: {cycle_interval}s)")
        
        try:
            while self.running:
                cycle_start = time.time()
                
                # Run trading cycle
                success = self._run_trading_cycle()
                
                if not success:
                    self.logger.warning("⚠️ Trading cycle failed")
                    
                    # If too many consecutive errors, pause longer
                    if self.error_count > 5:
                        self.logger.warning("⚠️ Multiple errors detected, extending sleep time")
                        time.sleep(cycle_interval * 2)
                        continue
                
                # Calculate sleep time to maintain consistent intervals
                cycle_duration = time.time() - cycle_start
                sleep_time = max(0, cycle_interval - cycle_duration)
                
                if sleep_time > 0:
                    self.logger.debug(f"😴 Sleeping for {sleep_time:.1f}s")
                    time.sleep(sleep_time)
                else:
                    self.logger.warning(f"⚠️ Cycle took {cycle_duration:.1f}s (longer than {cycle_interval}s interval)")
                
        except KeyboardInterrupt:
            self.logger.info("🛑 Keyboard interrupt received")
        except Exception as e:
            self.logger.error(f"💥 Unexpected error in main loop: {e}")
            self.logger.debug(traceback.format_exc())
        finally:
            self.stop()
    
    def stop(self):
        """Stop the trading bot gracefully"""
        self.logger.info("🛑 Stopping Trading Bot...")
        self.running = False
        
        # Send shutdown notification
        if hasattr(self, 'notifier'):
            self.notifier.send_shutdown_message()
        
        # Disconnect from IB
        if hasattr(self, 'data_collector'):
            self.data_collector.disconnect()
        
        # Log final statistics
        if self.start_time:
            total_uptime = datetime.now() - self.start_time
            self.stats['uptime'] = total_uptime
            
            self.logger.info("📊 Final Statistics:")
            self.logger.info(f"  ⏱️ Total uptime: {self._get_uptime()}")
            self.logger.info(f"  🔄 Cycles completed: {self.cycle_count}")
            self.logger.info(f"  📊 Data updates: {self.stats['data_updates']}")
            self.logger.info(f"  🚨 Total signals: {self.stats['total_signals']}")
            self.logger.info(f"  📤 Signals sent: {self.stats['signals_sent']}")
            self.logger.info(f"  ❌ Errors: {self.stats['errors']}")
        
        self.logger.info("✅ Trading Bot stopped")
    
    def status(self):
        """Print current bot status"""
        if not self.running:
            print("❌ Bot is not running")
            return
        
        status = self._get_system_status()
        
        print(f"\n🤖 Trading Bot Status")
        print(f"📊 IB Connected: {'✅' if status['ib_connected'] else '❌'}")
        print(f"👁️ Monitoring: {', '.join(status['monitored_symbols'])}")
        print(f"⏱️ Uptime: {status['uptime']}")
        print(f"🔄 Cycles: {status['cycle_count']}")
        print(f"🚨 Signals Sent: {status['recent_signals']}")
        print(f"❌ Errors: {status['error_count']}")
        print(f"🕐 Last Update: {status['last_update']}")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Trading Bot MVP')
    parser.add_argument('--config', default='config.yaml', help='Configuration file path')
    parser.add_argument('--test', action='store_true', help='Run in test mode (single cycle)')
    parser.add_argument('--status', action='store_true', help='Show bot status')
    
    args = parser.parse_args()
    
    # Create trading bot instance
    bot = TradingBot(config_path=args.config)
    
    if args.status:
        bot.status()
        return
    
    if args.test:
        print("🧪 Running in test mode...")
        if bot.start():
            print("✅ Starting test cycle...")
            bot._run_trading_cycle()
            bot.stop()
        else:
            print("❌ Test failed to start")
        return
    
    # Normal operation
    print("🚀 Starting Trading Bot MVP...")
    print("Press Ctrl+C to stop gracefully")
    print()
    
    try:
        bot.run()
    except Exception as e:
        print(f"💥 Fatal error: {e}")
        bot.stop()


if __name__ == "__main__":
    main()