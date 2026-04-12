# Databricks notebook source
# MAGIC %md
# MAGIC # 📈 Backtesting Framework for Decision Signals
# MAGIC
# MAGIC Evaluate decision agent performance on historical data:
# MAGIC - Simulate trading based on buy/hold/sell signals
# MAGIC - Calculate returns, Sharpe ratio, drawdown
# MAGIC - Compare against buy-and-hold benchmark
# MAGIC - Analyze signal accuracy and profitability

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql import functions as F
import plotly.graph_objects as go
import plotly.express as px

catalog = "riskbricks"
gold_db = f"{catalog}.gold"

print(f"📊 Backtesting RiskBricks Decision Signals")
print(f"📂 Database: {gold_db}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Load Historical Signals and Prices

# COMMAND ----------

def load_backtest_data(symbol="AAPL", start_date=None, end_date=None):
    """Load decision signals and actual prices for backtesting"""
    
    # Default date range
    if end_date is None:
        end_date = datetime.now().date()
    if start_date is None:
        start_date = end_date - timedelta(days=180)
    
    # Load decision signals
    signals_query = f"""
        SELECT 
            symbol,
            as_of_date,
            target_date,
            signal,
            score,
            expected_return,
            model_count,
            ingestion_timestamp
        FROM {gold_db}.decision_signals
        WHERE symbol = '{symbol}'
          AND as_of_date >= '{start_date}'
          AND as_of_date <= '{end_date}'
        ORDER BY as_of_date
    """
    
    signals_df = spark.sql(signals_query).toPandas()
    
    if len(signals_df) == 0:
        print(f"⚠️  No signals found for {symbol} between {start_date} and {end_date}")
        return None, None
    
    # Load actual prices
    prices_query = f"""
        SELECT 
            symbol,
            date,
            close,
            volume
        FROM {gold_db}.stock_prices_daily
        WHERE symbol = '{symbol}'
          AND date >= '{start_date}'
          AND date <= date_add('{end_date}', 30)
        ORDER BY date
    """
    
    prices_df = spark.sql(prices_query).toPandas()
    
    if len(prices_df) == 0:
        print(f"⚠️  No price data found for {symbol}")
        return None, None
    
    print(f"✅ Loaded {len(signals_df)} signals and {len(prices_df)} price points for {symbol}")
    print(f"   Signal date range: {signals_df['as_of_date'].min()} to {signals_df['as_of_date'].max()}")
    print(f"   Price date range: {prices_df['date'].min()} to {prices_df['date'].max()}")
    
    return signals_df, prices_df

# Test load
signals_df, prices_df = load_backtest_data("AAPL", start_date=datetime(2026, 1, 1).date())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Simulate Trading Strategy

# COMMAND ----------

def backtest_strategy(signals_df, prices_df, initial_capital=100000, transaction_cost=0.001):
    """
    Backtest a trading strategy based on signals
    
    Args:
        signals_df: DataFrame with columns [as_of_date, target_date, signal, expected_return]
        prices_df: DataFrame with columns [date, close]
        initial_capital: Starting capital in USD
        transaction_cost: Transaction cost as fraction (0.001 = 0.1%)
    
    Returns:
        DataFrame with daily portfolio value and trades
    """
    
    if signals_df is None or prices_df is None or len(signals_df) == 0:
        print("❌ No data to backtest")
        return None
    
    # Sort and prepare data
    signals_df = signals_df.sort_values('as_of_date').copy()
    prices_df = prices_df.sort_values('date').copy()
    
    # Create price lookup
    price_dict = dict(zip(prices_df['date'], prices_df['close']))
    
    # Initialize portfolio
    portfolio = {
        'cash': initial_capital,
        'shares': 0,
        'value': initial_capital,
        'position': 'CASH'  # CASH, LONG, SHORT
    }
    
    # Track history
    history = []
    trades = []
    
    # Iterate through each trading day
    all_dates = sorted(set(signals_df['as_of_date'].tolist() + list(price_dict.keys())))
    
    for current_date in all_dates:
        # Get price for current date
        if current_date not in price_dict:
            continue
        
        current_price = price_dict[current_date]
        
        # Check if there's a signal for this date
        day_signals = signals_df[signals_df['as_of_date'] == current_date]
        
        if len(day_signals) > 0:
            # Get most recent signal
            signal = day_signals.iloc[-1]
            signal_action = signal['signal']
            
            # Execute trade based on signal
            if signal_action == 'BUY' and portfolio['position'] != 'LONG':
                # Buy: convert cash to shares
                transaction_value = portfolio['cash'] * (1 - transaction_cost)
                shares_to_buy = transaction_value / current_price
                
                trades.append({
                    'date': current_date,
                    'action': 'BUY',
                    'price': current_price,
                    'shares': shares_to_buy,
                    'value': transaction_value,
                    'expected_return': signal['expected_return']
                })
                
                portfolio['shares'] = shares_to_buy
                portfolio['cash'] = 0
                portfolio['position'] = 'LONG'
            
            elif signal_action == 'SELL' and portfolio['position'] == 'LONG':
                # Sell: convert shares to cash
                transaction_value = portfolio['shares'] * current_price * (1 - transaction_cost)
                
                trades.append({
                    'date': current_date,
                    'action': 'SELL',
                    'price': current_price,
                    'shares': portfolio['shares'],
                    'value': transaction_value,
                    'expected_return': signal['expected_return']
                })
                
                portfolio['cash'] = transaction_value
                portfolio['shares'] = 0
                portfolio['position'] = 'CASH'
        
        # Calculate current portfolio value
        portfolio_value = portfolio['cash'] + (portfolio['shares'] * current_price)
        
        history.append({
            'date': current_date,
            'cash': portfolio['cash'],
            'shares': portfolio['shares'],
            'price': current_price,
            'value': portfolio_value,
            'position': portfolio['position']
        })
    
    # Convert to DataFrames
    history_df = pd.DataFrame(history)
    trades_df = pd.DataFrame(trades) if trades else pd.DataFrame()
    
    # Calculate returns
    history_df['daily_return'] = history_df['value'].pct_change()
    history_df['cumulative_return'] = (history_df['value'] / initial_capital) - 1
    
    # Calculate buy-and-hold benchmark
    if len(history_df) > 0:
        first_price = history_df.iloc[0]['price']
        history_df['buy_hold_value'] = initial_capital * (history_df['price'] / first_price)
        history_df['buy_hold_return'] = (history_df['buy_hold_value'] / initial_capital) - 1
    
    print(f"✅ Backtest complete")
    print(f"   Trades executed: {len(trades_df)}")
    print(f"   Trading days: {len(history_df)}")
    
    return history_df, trades_df

# Run backtest
if signals_df is not None and prices_df is not None:
    history_df, trades_df = backtest_strategy(signals_df, prices_df, initial_capital=100000)
else:
    history_df, trades_df = None, None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Calculate Performance Metrics

# COMMAND ----------

def calculate_performance_metrics(history_df, trades_df, initial_capital=100000):
    """Calculate comprehensive performance metrics"""
    
    if history_df is None or len(history_df) == 0:
        return {}
    
    final_value = history_df.iloc[-1]['value']
    buy_hold_final = history_df.iloc[-1]['buy_hold_value']
    
    # Total return
    total_return = (final_value / initial_capital) - 1
    buy_hold_return = (buy_hold_final / initial_capital) - 1
    
    # Annualized return (assume 252 trading days per year)
    days = len(history_df)
    years = days / 252
    annualized_return = (1 + total_return) ** (1 / max(years, 0.01)) - 1
    
    # Volatility (annualized)
    daily_returns = history_df['daily_return'].dropna()
    volatility = daily_returns.std() * np.sqrt(252)
    
    # Sharpe ratio (assuming 0% risk-free rate)
    sharpe = annualized_return / volatility if volatility > 0 else 0
    
    # Maximum drawdown
    running_max = history_df['value'].expanding().max()
    drawdown = (history_df['value'] - running_max) / running_max
    max_drawdown = drawdown.min()
    
    # Win rate (for trades)
    if trades_df is not None and len(trades_df) > 1:
        # Match buy-sell pairs
        buys = trades_df[trades_df['action'] == 'BUY'].reset_index(drop=True)
        sells = trades_df[trades_df['action'] == 'SELL'].reset_index(drop=True)
        
        if len(sells) > 0 and len(buys) > 0:
            min_len = min(len(buys), len(sells))
            buy_prices = buys['price'].iloc[:min_len].values
            sell_prices = sells['price'].iloc[:min_len].values
            trade_returns = (sell_prices / buy_prices) - 1
            
            wins = (trade_returns > 0).sum()
            win_rate = wins / len(trade_returns) if len(trade_returns) > 0 else 0
            avg_win = trade_returns[trade_returns > 0].mean() if wins > 0 else 0
            avg_loss = trade_returns[trade_returns <= 0].mean() if len(trade_returns) - wins > 0 else 0
        else:
            win_rate = 0
            avg_win = 0
            avg_loss = 0
    else:
        win_rate = 0
        avg_win = 0
        avg_loss = 0
    
    metrics = {
        'initial_capital': initial_capital,
        'final_value': final_value,
        'total_return': total_return,
        'annualized_return': annualized_return,
        'buy_hold_return': buy_hold_return,
        'alpha': total_return - buy_hold_return,
        'volatility': volatility,
        'sharpe_ratio': sharpe,
        'max_drawdown': max_drawdown,
        'num_trades': len(trades_df) if trades_df is not None else 0,
        'win_rate': win_rate,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'total_days': days
    }
    
    return metrics

# Calculate metrics
if history_df is not None:
    metrics = calculate_performance_metrics(history_df, trades_df)
    
    print("\n" + "="*60)
    print("📊 Performance Metrics")
    print("="*60)
    print(f"Initial Capital:        ${metrics['initial_capital']:,.2f}")
    print(f"Final Value:            ${metrics['final_value']:,.2f}")
    print(f"Total Return:           {metrics['total_return']:.2%}")
    print(f"Annualized Return:      {metrics['annualized_return']:.2%}")
    print(f"Buy-Hold Return:        {metrics['buy_hold_return']:.2%}")
    print(f"Alpha (vs Buy-Hold):    {metrics['alpha']:.2%}")
    print(f"Annualized Volatility:  {metrics['volatility']:.2%}")
    print(f"Sharpe Ratio:           {metrics['sharpe_ratio']:.3f}")
    print(f"Max Drawdown:           {metrics['max_drawdown']:.2%}")
    print(f"Number of Trades:       {metrics['num_trades']}")
    print(f"Win Rate:               {metrics['win_rate']:.2%}")
    print(f"Avg Win:                {metrics['avg_win']:.2%}")
    print(f"Avg Loss:               {metrics['avg_loss']:.2%}")
    print("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Visualize Results

# COMMAND ----------

def plot_backtest_results(history_df, trades_df, symbol="AAPL"):
    """Create visualizations of backtest results"""
    
    if history_df is None or len(history_df) == 0:
        print("❌ No data to plot")
        return
    
    # 1. Portfolio Value Over Time
    fig1 = go.Figure()
    
    # Strategy performance
    fig1.add_trace(go.Scatter(
        x=history_df['date'],
        y=history_df['value'],
        mode='lines',
        name='Strategy',
        line=dict(color='blue', width=2)
    ))
    
    # Buy-and-hold benchmark
    fig1.add_trace(go.Scatter(
        x=history_df['date'],
        y=history_df['buy_hold_value'],
        mode='lines',
        name='Buy & Hold',
        line=dict(color='gray', width=2, dash='dash')
    ))
    
    # Mark trades
    if trades_df is not None and len(trades_df) > 0:
        buys = trades_df[trades_df['action'] == 'BUY']
        sells = trades_df[trades_df['action'] == 'SELL']
        
        if len(buys) > 0:
            buy_values = history_df[history_df['date'].isin(buys['date'])]['value']
            fig1.add_trace(go.Scatter(
                x=buys['date'],
                y=buy_values,
                mode='markers',
                name='Buy',
                marker=dict(color='green', size=10, symbol='triangle-up')
            ))
        
        if len(sells) > 0:
            sell_values = history_df[history_df['date'].isin(sells['date'])]['value']
            fig1.add_trace(go.Scatter(
                x=sells['date'],
                y=sell_values,
                mode='markers',
                name='Sell',
                marker=dict(color='red', size=10, symbol='triangle-down')
            ))
    
    fig1.update_layout(
        title=f'{symbol} Strategy Backtest: Portfolio Value',
        xaxis_title='Date',
        yaxis_title='Portfolio Value ($)',
        hovermode='x unified',
        height=500
    )
    
    # 2. Cumulative Returns
    fig2 = go.Figure()
    
    fig2.add_trace(go.Scatter(
        x=history_df['date'],
        y=history_df['cumulative_return'] * 100,
        mode='lines',
        name='Strategy',
        fill='tozeroy',
        line=dict(color='blue', width=2)
    ))
    
    fig2.add_trace(go.Scatter(
        x=history_df['date'],
        y=history_df['buy_hold_return'] * 100,
        mode='lines',
        name='Buy & Hold',
        line=dict(color='gray', width=2, dash='dash')
    ))
    
    fig2.update_layout(
        title=f'{symbol} Cumulative Returns',
        xaxis_title='Date',
        yaxis_title='Return (%)',
        hovermode='x unified',
        height=400
    )
    
    # 3. Drawdown Chart
    running_max = history_df['value'].expanding().max()
    drawdown = (history_df['value'] - running_max) / running_max * 100
    
    fig3 = go.Figure()
    
    fig3.add_trace(go.Scatter(
        x=history_df['date'],
        y=drawdown,
        mode='lines',
        name='Drawdown',
        fill='tozeroy',
        line=dict(color='red', width=2)
    ))
    
    fig3.update_layout(
        title=f'{symbol} Drawdown Analysis',
        xaxis_title='Date',
        yaxis_title='Drawdown (%)',
        hovermode='x unified',
        height=400
    )
    
    # Display plots
    fig1.show()
    fig2.show()
    fig3.show()
    
    print("✅ Plots generated")

# Plot results
if history_df is not None:
    plot_backtest_results(history_df, trades_df, symbol="AAPL")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Backtest Multiple Symbols

# COMMAND ----------

def backtest_multiple_symbols(symbols, start_date=None, end_date=None, initial_capital=100000):
    """Run backtest for multiple symbols and compare"""
    
    results = {}
    
    for symbol in symbols:
        print(f"\n🔄 Backtesting {symbol}...")
        try:
            # Load data
            signals_df, prices_df = load_backtest_data(symbol, start_date, end_date)
            
            if signals_df is None or prices_df is None:
                print(f"   ⚠️  Skipping {symbol} - no data")
                continue
            
            # Run backtest
            history_df, trades_df = backtest_strategy(signals_df, prices_df, initial_capital)
            
            if history_df is None:
                print(f"   ⚠️  Skipping {symbol} - backtest failed")
                continue
            
            # Calculate metrics
            metrics = calculate_performance_metrics(history_df, trades_df, initial_capital)
            
            results[symbol] = {
                'metrics': metrics,
                'history': history_df,
                'trades': trades_df
            }
            
            print(f"   ✅ {symbol}: Return={metrics['total_return']:.2%}, Sharpe={metrics['sharpe_ratio']:.3f}, Trades={metrics['num_trades']}")
        
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    return results

# Backtest multiple symbols
test_symbols = ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"]
multi_results = backtest_multiple_symbols(
    test_symbols,
    start_date=datetime(2026, 1, 1).date(),
    end_date=datetime.now().date(),
    initial_capital=100000
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6️⃣ Compare Symbol Performance

# COMMAND ----------

if multi_results:
    # Create comparison DataFrame
    comparison_data = []
    for symbol, result in multi_results.items():
        m = result['metrics']
        comparison_data.append({
            'Symbol': symbol,
            'Total Return': m['total_return'],
            'Annualized Return': m['annualized_return'],
            'Buy-Hold Return': m['buy_hold_return'],
            'Alpha': m['alpha'],
            'Sharpe Ratio': m['sharpe_ratio'],
            'Max Drawdown': m['max_drawdown'],
            'Win Rate': m['win_rate'],
            'Num Trades': m['num_trades']
        })
    
    comparison_df = pd.DataFrame(comparison_data)
    comparison_df = comparison_df.sort_values('Total Return', ascending=False)
    
    print("\n" + "="*80)
    print("📊 Multi-Symbol Backtest Comparison")
    print("="*80)
    print(comparison_df.to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    print("="*80)
    
    # Plot comparison
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=comparison_df['Symbol'],
        y=comparison_df['Total Return'] * 100,
        name='Strategy Return',
        marker_color='blue'
    ))
    
    fig.add_trace(go.Bar(
        x=comparison_df['Symbol'],
        y=comparison_df['Buy-Hold Return'] * 100,
        name='Buy-Hold Return',
        marker_color='gray'
    ))
    
    fig.update_layout(
        title='Strategy vs Buy-Hold Returns by Symbol',
        xaxis_title='Symbol',
        yaxis_title='Return (%)',
        barmode='group',
        height=500
    )
    
    fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7️⃣ Save Backtest Results to Gold Table

# COMMAND ----------

def save_backtest_results(results_dict, run_date=None):
    """Save backtest results to gold table for tracking"""
    
    if run_date is None:
        run_date = datetime.now().date()
    
    records = []
    for symbol, result in results_dict.items():
        m = result['metrics']
        records.append({
            'symbol': symbol,
            'run_date': run_date,
            'total_return': m['total_return'],
            'annualized_return': m['annualized_return'],
            'buy_hold_return': m['buy_hold_return'],
            'alpha': m['alpha'],
            'sharpe_ratio': m['sharpe_ratio'],
            'max_drawdown': m['max_drawdown'],
            'volatility': m['volatility'],
            'num_trades': m['num_trades'],
            'win_rate': m['win_rate'],
            'avg_win': m['avg_win'],
            'avg_loss': m['avg_loss'],
            'total_days': m['total_days'],
            'final_value': m['final_value'],
            'initial_capital': m['initial_capital']
        })
    
    if records:
        results_df = spark.createDataFrame(records)
        
        # Save to gold table
        table_name = f"{gold_db}.backtest_results"
        results_df.write.mode("append").saveAsTable(table_name)
        
        print(f"✅ Saved {len(records)} backtest results to {table_name}")
    else:
        print("⚠️  No results to save")

# Save results
if multi_results:
    save_backtest_results(multi_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Summary

# COMMAND ----------

print("=" * 60)
print("📈 Backtesting Framework Complete!")
print("=" * 60)
print()
print("✅ What's Been Done:")
print("  - Loaded historical signals and prices")
print("  - Simulated trading strategy")
print("  - Calculated performance metrics")
print("  - Compared against buy-and-hold")
print("  - Backtested multiple symbols")
print("  - Saved results to gold table")
print()
print("📊 Key Metrics Tracked:")
print("  - Total return & annualized return")
print("  - Sharpe ratio")
print("  - Max drawdown")
print("  - Win rate")
print("  - Alpha vs buy-and-hold")
print()
print("🔧 Next Steps:")
print("  1. Run backtests regularly (weekly/monthly)")
print("  2. Optimize decision agent thresholds")
print("  3. Add risk management rules (stop-loss, position sizing)")
print("  4. Test different signal combinations")
print("  5. Create dashboard for backtest results")
print()
print("💡 Usage:")
print("  results = backtest_multiple_symbols(['AAPL', 'MSFT'], start_date='2026-01-01')")
print("  save_backtest_results(results)")
print("=" * 60)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

