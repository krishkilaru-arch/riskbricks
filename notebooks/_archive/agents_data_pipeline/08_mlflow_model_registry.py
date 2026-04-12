# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 MLflow Model Registry Integration
# MAGIC
# MAGIC Register forecast models in Unity Catalog Model Registry for:
# MAGIC - Version control
# MAGIC - Experiment tracking
# MAGIC - Model lineage
# MAGIC - A/B testing
# MAGIC - Model governance

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error
from pyspark.sql import functions as F

mlflow.set_registry_uri("databricks-uc")
catalog = "riskbricks"
schema = "models"

# Ensure schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

print(f"✅ MLflow Registry: databricks-uc")
print(f"📦 Model Schema: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Fetch Training Data

# COMMAND ----------

def get_training_data(symbol="AAPL", lookback_days=252):
    """Get historical price data and features for model training"""
    
    # Get stock prices
    prices_df = spark.sql(f"""
        SELECT 
            symbol,
            date,
            close,
            volume,
            LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date) as prev_close,
            LAG(close, 5) OVER (PARTITION BY symbol ORDER BY date) as close_5d_ago,
            LAG(close, 20) OVER (PARTITION BY symbol ORDER BY date) as close_20d_ago
        FROM riskbricks.gold.stock_prices_daily
        WHERE symbol = '{symbol}'
        ORDER BY date DESC
        LIMIT {lookback_days}
    """).toPandas()
    
    if len(prices_df) == 0:
        raise ValueError(f"No price data found for {symbol}")
    
    # Calculate features
    prices_df = prices_df.sort_values('date')
    prices_df['return_1d'] = prices_df['close'].pct_change()
    prices_df['return_5d'] = (prices_df['close'] / prices_df['close_5d_ago']) - 1
    prices_df['return_20d'] = (prices_df['close'] / prices_df['close_20d_ago']) - 1
    prices_df['vol_20d'] = prices_df['return_1d'].rolling(20).std()
    prices_df['ma_5'] = prices_df['close'].rolling(5).mean()
    prices_df['ma_20'] = prices_df['close'].rolling(20).mean()
    prices_df['volume_ma_20'] = prices_df['volume'].rolling(20).mean()
    
    # Get macro indicators
    macro_df = spark.sql("""
        SELECT 
            date,
            indicator_name,
            value
        FROM riskbricks.gold.macro_indicators_daily
        WHERE indicator_name IN ('DGS10', 'VIXCLS')
    """).toPandas()
    
    if len(macro_df) > 0:
        macro_pivot = macro_df.pivot(index='date', columns='indicator_name', values='value').reset_index()
        prices_df = prices_df.merge(macro_pivot, on='date', how='left')
        prices_df['DGS10'] = prices_df.get('DGS10', 0).fillna(method='ffill')
        prices_df['VIXCLS'] = prices_df.get('VIXCLS', 0).fillna(method='ffill')
    else:
        prices_df['DGS10'] = 0
        prices_df['VIXCLS'] = 0
    
    # Target: next day's return
    prices_df['target_return'] = prices_df['return_1d'].shift(-1)
    prices_df['target_price'] = prices_df['close'].shift(-1)
    
    # Drop NaN rows
    prices_df = prices_df.dropna()
    
    return prices_df

# Test
test_data = get_training_data("AAPL", lookback_days=100)
print(f"✅ Loaded {len(test_data)} training samples for AAPL")
print(f"   Features: {[c for c in test_data.columns if c not in ['symbol', 'date', 'target_return', 'target_price']]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Train GradientBoosting Model

# COMMAND ----------

def train_gbm_model(symbol="AAPL", experiment_name=None):
    """Train GradientBoosting model and log to MLflow"""
    
    if experiment_name is None:
        experiment_name = f"/Users/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/riskbricks_forecast_{symbol}"
    
    mlflow.set_experiment(experiment_name)
    
    # Get data
    df = get_training_data(symbol, lookback_days=252)
    
    # Train/test split (80/20)
    split_idx = int(len(df) * 0.8)
    train = df.iloc[:split_idx]
    test = df.iloc[split_idx:]
    
    # Features
    feature_cols = ['return_1d', 'return_5d', 'return_20d', 'vol_20d', 
                    'ma_5', 'ma_20', 'volume_ma_20', 'DGS10', 'VIXCLS']
    feature_cols = [c for c in feature_cols if c in df.columns]
    
    X_train = train[feature_cols].fillna(0)
    y_train = train['target_return']
    X_test = test[feature_cols].fillna(0)
    y_test = test['target_return']
    
    # Start MLflow run
    with mlflow.start_run(run_name=f"GBM_{symbol}_{datetime.now().strftime('%Y%m%d_%H%M')}") as run:
        # Log parameters
        params = {
            'model_type': 'GradientBoosting',
            'symbol': symbol,
            'n_estimators': 100,
            'max_depth': 3,
            'learning_rate': 0.1,
            'train_samples': len(X_train),
            'test_samples': len(X_test),
            'features': ','.join(feature_cols)
        }
        mlflow.log_params(params)
        
        # Train model
        model = GradientBoostingRegressor(
            n_estimators=params['n_estimators'],
            max_depth=params['max_depth'],
            learning_rate=params['learning_rate'],
            random_state=42
        )
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        train_mae = mean_absolute_error(y_train, y_train_pred)
        test_mae = mean_absolute_error(y_test, y_test_pred)
        train_rmse = np.sqrt(mean_squared_error(y_train, y_train_pred))
        test_rmse = np.sqrt(mean_squared_error(y_test, y_test_pred))
        
        # Directional accuracy
        train_direction = np.mean((np.sign(y_train_pred) == np.sign(y_train)))
        test_direction = np.mean((np.sign(y_test_pred) == np.sign(y_test)))
        
        metrics = {
            'train_mae': train_mae,
            'test_mae': test_mae,
            'train_rmse': train_rmse,
            'test_rmse': test_rmse,
            'train_direction_accuracy': train_direction,
            'test_direction_accuracy': test_direction
        }
        mlflow.log_metrics(metrics)
        
        # Feature importance
        feature_importance = pd.DataFrame({
            'feature': feature_cols,
            'importance': model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        mlflow.log_table(feature_importance, "feature_importance.json")
        
        # Log model
        mlflow.sklearn.log_model(
            model,
            artifact_path="model",
            registered_model_name=f"{catalog}.{schema}.forecast_gbm_{symbol.lower()}",
            signature=mlflow.models.infer_signature(X_train, y_train_pred)
        )
        
        print(f"✅ Model trained and logged for {symbol}")
        print(f"   Train MAE: {train_mae:.6f}, Test MAE: {test_mae:.6f}")
        print(f"   Train RMSE: {train_rmse:.6f}, Test RMSE: {test_rmse:.6f}")
        print(f"   Test Direction Accuracy: {test_direction:.2%}")
        print(f"   Run ID: {run.info.run_id}")
        
        return run.info.run_id, model, metrics

# Train for AAPL
run_id, model, metrics = train_gbm_model("AAPL")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Train Ridge Regression Model

# COMMAND ----------

def train_ridge_model(symbol="AAPL", experiment_name=None):
    """Train Ridge Regression model and log to MLflow"""
    
    if experiment_name is None:
        experiment_name = f"/Users/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/riskbricks_forecast_{symbol}"
    
    mlflow.set_experiment(experiment_name)
    
    # Get data
    df = get_training_data(symbol, lookback_days=252)
    
    # Train/test split
    split_idx = int(len(df) * 0.8)
    train = df.iloc[:split_idx]
    test = df.iloc[split_idx:]
    
    # Features
    feature_cols = ['return_1d', 'return_5d', 'return_20d', 'vol_20d', 
                    'ma_5', 'ma_20', 'volume_ma_20', 'DGS10', 'VIXCLS']
    feature_cols = [c for c in feature_cols if c in df.columns]
    
    X_train = train[feature_cols].fillna(0)
    y_train = train['target_return']
    X_test = test[feature_cols].fillna(0)
    y_test = test['target_return']
    
    # Start MLflow run
    with mlflow.start_run(run_name=f"Ridge_{symbol}_{datetime.now().strftime('%Y%m%d_%H%M')}") as run:
        # Log parameters
        params = {
            'model_type': 'Ridge',
            'symbol': symbol,
            'alpha': 1.0,
            'train_samples': len(X_train),
            'test_samples': len(X_test),
            'features': ','.join(feature_cols)
        }
        mlflow.log_params(params)
        
        # Train model
        model = Ridge(alpha=params['alpha'])
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        train_mae = mean_absolute_error(y_train, y_train_pred)
        test_mae = mean_absolute_error(y_test, y_test_pred)
        train_rmse = np.sqrt(mean_squared_error(y_train, y_train_pred))
        test_rmse = np.sqrt(mean_squared_error(y_test, y_test_pred))
        train_direction = np.mean((np.sign(y_train_pred) == np.sign(y_train)))
        test_direction = np.mean((np.sign(y_test_pred) == np.sign(y_test)))
        
        metrics = {
            'train_mae': train_mae,
            'test_mae': test_mae,
            'train_rmse': train_rmse,
            'test_rmse': test_rmse,
            'train_direction_accuracy': train_direction,
            'test_direction_accuracy': test_direction
        }
        mlflow.log_metrics(metrics)
        
        # Log model
        mlflow.sklearn.log_model(
            model,
            artifact_path="model",
            registered_model_name=f"{catalog}.{schema}.forecast_ridge_{symbol.lower()}",
            signature=mlflow.models.infer_signature(X_train, y_train_pred)
        )
        
        print(f"✅ Ridge model trained for {symbol}")
        print(f"   Test MAE: {test_mae:.6f}, Test Direction: {test_direction:.2%}")
        
        return run.info.run_id, model, metrics

# Train Ridge for AAPL
ridge_run_id, ridge_model, ridge_metrics = train_ridge_model("AAPL")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Train Models for Multiple Symbols

# COMMAND ----------

# Get top symbols from gold table
symbols_df = spark.sql("""
    SELECT DISTINCT symbol
    FROM riskbricks.gold.stock_prices_daily
    WHERE date >= date_sub(current_date(), 252)
    GROUP BY symbol
    HAVING COUNT(*) >= 200
    ORDER BY symbol
    LIMIT 10
""").toPandas()

top_symbols = symbols_df['symbol'].tolist()
print(f"📊 Training models for {len(top_symbols)} symbols: {', '.join(top_symbols)}")

# COMMAND ----------

# Train GBM for each symbol
gbm_results = {}

for symbol in top_symbols:
    try:
        print(f"\n🔄 Training GBM for {symbol}...")
        run_id, model, metrics = train_gbm_model(symbol)
        gbm_results[symbol] = {
            'run_id': run_id,
            'test_mae': metrics['test_mae'],
            'test_direction': metrics['test_direction_accuracy']
        }
    except Exception as e:
        print(f"❌ Error training {symbol}: {e}")
        gbm_results[symbol] = {'error': str(e)}

# Summary
print("\n" + "="*60)
print("📊 GBM Training Summary")
print("="*60)
for sym, result in gbm_results.items():
    if 'error' in result:
        print(f"{sym}: ❌ {result['error']}")
    else:
        print(f"{sym}: ✅ MAE={result['test_mae']:.6f}, Direction={result['test_direction']:.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Compare Models Across Versions

# COMMAND ----------

def compare_model_versions(model_name_pattern="forecast_gbm_"):
    """Compare different versions of registered models"""
    
    from mlflow.tracking import MlflowClient
    client = MlflowClient()
    
    # Get all models matching pattern
    all_models = []
    
    try:
        for rm in client.search_registered_models(f"name LIKE '{catalog}.{schema}.{model_name_pattern}%'"):
            model_name = rm.name
            latest_versions = rm.latest_versions
            
            for version in latest_versions:
                run = client.get_run(version.run_id)
                all_models.append({
                    'model_name': model_name,
                    'version': version.version,
                    'stage': version.current_stage,
                    'run_id': version.run_id,
                    'test_mae': run.data.metrics.get('test_mae'),
                    'test_direction': run.data.metrics.get('test_direction_accuracy'),
                    'created_at': datetime.fromtimestamp(version.creation_timestamp / 1000)
                })
    except Exception as e:
        print(f"⚠️  Could not fetch models: {e}")
        return pd.DataFrame()
    
    df = pd.DataFrame(all_models)
    return df.sort_values('test_mae') if not df.empty else df

# Compare models
comparison_df = compare_model_versions("forecast_gbm_")
if not comparison_df.empty:
    print("📊 Model Comparison (sorted by test MAE):")
    print(comparison_df.to_string(index=False))
else:
    print("ℹ️  No models found for comparison")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6️⃣ Promote Best Model to Production

# COMMAND ----------

def promote_to_production(model_name, version=None):
    """Promote a model version to Production stage"""
    from mlflow.tracking import MlflowClient
    client = MlflowClient()
    
    full_model_name = f"{catalog}.{schema}.{model_name}"
    
    if version is None:
        # Get latest version
        versions = client.get_latest_versions(full_model_name)
        if not versions:
            raise ValueError(f"No versions found for {full_model_name}")
        version = versions[0].version
    
    # Transition to Production
    client.transition_model_version_stage(
        name=full_model_name,
        version=version,
        stage="Production",
        archive_existing_versions=True  # Archive previous production versions
    )
    
    print(f"✅ Promoted {full_model_name} version {version} to Production")
    return version

# Example: Promote AAPL GBM model
if 'AAPL' in gbm_results and 'run_id' in gbm_results['AAPL']:
    try:
        version = promote_to_production("forecast_gbm_aapl")
        print(f"   Version: {version}")
    except Exception as e:
        print(f"⚠️  Could not promote model: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7️⃣ Load Production Model for Inference

# COMMAND ----------

def load_production_model(symbol):
    """Load the production version of a model"""
    model_name = f"{catalog}.{schema}.forecast_gbm_{symbol.lower()}"
    
    try:
        # Load production model
        model_uri = f"models:/{model_name}/Production"
        model = mlflow.sklearn.load_model(model_uri)
        print(f"✅ Loaded production model for {symbol}")
        return model
    except Exception as e:
        print(f"❌ Error loading model: {e}")
        print(f"   Trying 'None' stage...")
        try:
            model_uri = f"models:/{model_name}/None"
            model = mlflow.sklearn.load_model(model_uri)
            print(f"✅ Loaded latest model for {symbol}")
            return model
        except Exception as e2:
            print(f"❌ Could not load any version: {e2}")
            return None

# Test loading
prod_model = load_production_model("AAPL")

if prod_model:
    # Make a prediction
    test_features = get_training_data("AAPL", lookback_days=50).iloc[-1]
    feature_cols = ['return_1d', 'return_5d', 'return_20d', 'vol_20d', 
                    'ma_5', 'ma_20', 'volume_ma_20', 'DGS10', 'VIXCLS']
    feature_cols = [c for c in feature_cols if c in test_features.index]
    
    X = test_features[feature_cols].fillna(0).values.reshape(1, -1)
    pred_return = prod_model.predict(X)[0]
    
    current_price = test_features['close']
    predicted_price = current_price * (1 + pred_return)
    
    print(f"\n🔮 Prediction for AAPL:")
    print(f"   Current price: ${current_price:.2f}")
    print(f"   Predicted return: {pred_return:.4f} ({pred_return*100:.2f}%)")
    print(f"   Predicted price: ${predicted_price:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Summary

# COMMAND ----------

print("=" * 60)
print("🎯 MLflow Model Registry Setup Complete!")
print("=" * 60)
print()
print(f"📦 Registry: databricks-uc://{catalog}.{schema}")
print(f"🔬 Models trained: {len([r for r in gbm_results.values() if 'run_id' in r])}/{len(top_symbols)} symbols")
print()
print("✅ What's Registered:")
print(f"   - GradientBoosting models: forecast_gbm_<symbol>")
print(f"   - Ridge models: forecast_ridge_<symbol>")
print()
print("🔧 Next Steps:")
print("  1. Review model metrics in MLflow UI")
print("  2. Promote best-performing models to Production")
print("  3. Integrate production models into forecast agent")
print("  4. Set up automated retraining pipeline")
print("  5. Monitor model drift and performance")
print()
print("💡 Usage:")
print(f"   model = mlflow.sklearn.load_model('models:/{catalog}.{schema}.forecast_gbm_aapl/Production')")
print("   predictions = model.predict(X)")
print("=" * 60)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Exit

# COMMAND ----------

dbutils.notebook.exit("success")

