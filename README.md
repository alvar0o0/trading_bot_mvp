# Trading Bot MVP

Sistema automatizado de alertas de trading conectado a Interactive Brokers.

## Configuración
1. Instalar dependencias: `pip install -r requirements.txt`
2. Configurar TWS/IB Gateway (puerto 7497 para paper trading)
3. Completar config.yaml con tus tokens
4. Ejecutar: `python main.py`

## Estructura
- main.py: Script principal
- data_collector.py: Conexión con IB
- rule_engine.py: Lógica de trading
- notifier.py: Alertas por Telegram
- config.yaml: Configuración

