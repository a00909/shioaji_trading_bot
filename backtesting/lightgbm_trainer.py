"""
LightGBM 訓練腳本（精簡版）
用法: trainer = LightGBMTrainer()
     trainer.train(train_data, valid_data)
     trainer.analyze_features()
     trainer.save()
"""

import lightgbm as lgb
import pandas as pd


class LightGBMTrainer:

    def __init__(self, params=None):
        self.params = params or {
            'objective': 'binary',
            'metric': 'auc',
            'learning_rate': 0.02,
            'num_boost_round': 1000,
            'max_depth': 3,              # 1個因子，深度設 3 就綽綽有餘 (最多8個葉子)
            'num_leaves': 7,
            'min_data_in_leaf': 100,
            # 1個因子不需要 colsample_bytree 抽樣了
        }
        self.model = None

    def train(self, train_data, valid_data=None):
        self.model = lgb.train(
            self.params,
            train_data,
            num_boost_round=500,
            valid_sets=[valid_data] if valid_data else None,
            callbacks=[
                lgb.early_stopping(30),
                lgb.log_evaluation(50),
            ] if valid_data else None,
        )
        return self.model

    def predict(self, X):
        return self.model.predict(X)

    def save(self, path='model.txt'):
        self.model.save_model(path)

    def load(self, path='model.txt'):
        self.model = lgb.Booster(model_file=path)

    def analyze_features(self):
        names = self.model.feature_name()
        gain = self.model.feature_importance(importance_type='gain')
        split = self.model.feature_importance(importance_type='split')

        df = pd.DataFrame({
            'feature': names,
            'gain': gain,
            'split': split,
        }).sort_values('gain', ascending=False)
        df['gain_pct'] = (df['gain'] / df['gain'].sum() * 100).round(2)

        print(df.to_string(index=False))
        return df
