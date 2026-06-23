"""
LightGBM 訓練腳本（精簡版）
用法: trainer = LightGBMTrainer()
     trainer.train(train_data, valid_data)
     trainer.analyze_features()
     trainer.save()
"""

import lightgbm as lgb
import m2cgen
import pandas as pd
from lightgbm import Booster, LGBMClassifier


class LightGBMTrainer:

    def __init__(self, params=None):
        # self.params = params or {
        #     'objective': 'binary',
        #     'metric': 'auc',
        #     'learning_rate': 0.02,
        #     'num_boost_round': 5000,
        #     'max_depth': 3,
        #     'num_leaves': 7,
        #     'min_data_in_leaf': 100,
        #
        #     # 'max_bin': 63,
        #
        # }
        # 三元分類
        self.params = params or {
            'objective': 'multiclass',
            'metric': 'multi_logloss',
            'num_class': 3,
            'learning_rate': 0.02,
            'num_boost_round': 5000,
            'max_depth': 3,
            'num_leaves': 7,
            'min_data_in_leaf': 100,

            # 'max_bin': 63,

        }
        self.model: Booster = None

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

    def save_py(self, file_path="model.py.bk"):
        """
        純粹從原生 Booster 提取特徵數量，並將其轉譯為 Numba 加速代碼
        """
        # 🔥 直接從 Booster 核心撈出該模型訓練時採用的特徵數量
        feature_count = self.model.num_feature()

        print(f"【系統通知】由 Booster 自動偵測特徵數量：{feature_count}，啟動黑魔法偽裝...")

        # 1. 建立一個相同類型的 Scikit-learn 空殼物件
        disguised_model = LGBMClassifier()

        # 2. 強行把你的原生 booster 塞入這個空殼中
        disguised_model._Booster = self.model

        # 3. 欺騙 m2cgen 的型別檢查機制，偽造必要的 Scikit-learn 內部成員變數
        disguised_model._n_features = feature_count  # 自動填入
        disguised_model._n_classes = 2  # 二元分類固定填 2
        disguised_model.fitted_ = True  # 標記為已訓練完畢

        print("【系統通知】偽裝成功，開始使用 m2cgen 轉譯為純 Python 邏輯分支...")

        # 4. 使用 m2cgen 將模型轉譯為純 Python 程式碼字串
        raw_python_code = m2cgen.export_to_python(disguised_model)

        print("【系統通知】程式碼生成完畢，正在注入 Numba 機械碼優化標頭...")

        # 5. 準備注入 Numba 標頭與效能優化參數
        numba_header = (
            "import numpy as np\n"
            "from numba import njit\n\n"
            "@njit(cache=True, fastmath=True)\n"
        )

        # 組合最終代碼
        final_code = numba_header + raw_python_code

        # 6. 寫入本地檔案
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(final_code)

        print(f"【🔥 成功】極速實盤模型已導出至：{file_path} (自動辨識特徵數: {feature_count})")



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
