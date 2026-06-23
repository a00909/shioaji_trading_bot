import lightgbm as lgb
import numpy as np


class DatasetBuilder:
    @staticmethod
    def build(features: dict[str, np.ndarray], label: np.ndarray, valid_mask: np.ndarray, valid_ratio=0.2,
              valid_offset_ratio: float = 0.3, valid_from_start=False):
        # 先過濾valid範圍
        names = []
        filtered_f = []

        for k, v in features.items():
            names.append(k)
            filtered_f.append(v[valid_mask])
        filtered_label = label[valid_mask]

        valid_start, valid_end = DatasetBuilder.calculate_validation_bounds_safe_offset(
            len(filtered_label), valid_ratio, valid_offset_ratio
        )

        X_valid = np.hstack([f[valid_start:valid_end].reshape(-1, 1) for f in filtered_f])
        y_valid = filtered_label[valid_start:valid_end]

        print(f'dataset len: {len(filtered_label)}')
        print(f'valid range: [{valid_start}:{valid_end}]')

        if valid_from_start:
            X_train = np.hstack([f[valid_end:].reshape(-1, 1) for f in filtered_f])
            y_train = filtered_label[valid_end:]
            print(f'train range: [{valid_end}:]')
        else:
            X_train = np.hstack([f[:valid_start].reshape(-1, 1) for f in filtered_f])
            y_train = filtered_label[:valid_start]
            print(f'train range: [:{valid_start}]')

        assert len(X_train) == len(y_train) and len(X_valid) == len(y_valid)

        train_data = lgb.Dataset(
            X_train, y_train, feature_name=names
        )
        valid_data = lgb.Dataset(
            X_valid, y_valid, feature_name=names
        )

        return train_data, valid_data

    @staticmethod
    def calculate_validation_bounds_safe_offset(d_len: int, v_ratio: float, v_offset_ratio: float) -> tuple[int, int]:
        """
        自適應安全區間偏移計算器 (Offset from Safe Available Length)

        Args:
            d_len (int): 資料集總長度 (例如: 3570000)
            v_ratio (float): 驗證集佔總資料的比例 (例如: 0.2)
            v_offset_ratio (float): 在安全可用空間內的偏移比率，範圍嚴格在 [0.0, 1.0] 之間。
                                    0.0 -> 完美靠左 (從 Index 0 開始)
                                    0.5 -> 完美置中
                                    1.0 -> 完美靠右 (收尾在最後一筆資料)

        Returns:
            tuple[int, int]: (start_idx, end_idx) 驗證集的起點與終點索引 (左閉右開區間)
        """
        # 1. 安全防禦：確保偏移比率在標準範圍
        if not (0.0 <= v_offset_ratio <= 1.0):
            raise ValueError("v_offset_ratio 必須嚴格落在 [0.0, 1.0] 之間。")

        # 2. 計算驗證集的絕對長度
        v_size = int(d_len * v_ratio)
        if v_size <= 0 or v_size >= d_len:
            raise ValueError(f"無效的 v_ratio ({v_ratio}), d_len: {d_len}，驗證集長度計算異常。")

        # 3. 【核心修正】計算扣除驗證集後的「安全可用長度」
        safe_available_len = d_len - v_size

        # 4. 根據安全長度與偏移比率，精準算出起點索引
        start_idx = int(safe_available_len * v_offset_ratio)

        # 5. 算出終點索引
        end_idx = start_idx + v_size

        return start_idx, end_idx

    @staticmethod
    def build2(
            features: dict[str, np.ndarray],
            label: np.ndarray,
            valid_masks: list[np.ndarray],
            times: np.ndarray | None = None,
            valid_ratio=0.2,
            valid_offset_ratio: float = 0.3,
            valid_from_start=False,
            sample_interval_seconds: float = 0.0
    ):
        names = []
        filtered_f = []
        valid_mask = np.all(valid_masks, axis=0)

        for k, v in features.items():
            names.append(k)
            filtered_f.append(v[valid_mask])
        filtered_label = label[valid_mask]

        # 降頻取樣
        if sample_interval_seconds > 0:
            if times is None:
                raise ValueError("sample_interval_seconds > 0 需要傳入 times")
            filtered_times = times[valid_mask]
            mask = DatasetBuilder._build_sample_mask(filtered_times, sample_interval_seconds)
            filtered_f = [f[mask] for f in filtered_f]
            filtered_label = filtered_label[mask]
            print(f'降頻取樣: {len(filtered_times)} → {mask.sum()} (間隔 {sample_interval_seconds}s)')

        valid_start, valid_end = DatasetBuilder.calculate_validation_bounds_safe_offset(
            len(filtered_label), valid_ratio, valid_offset_ratio
        )

        X_valid = np.hstack([f[valid_start:valid_end].reshape(-1, 1) for f in filtered_f])
        y_valid = filtered_label[valid_start:valid_end]

        print(f'dataset len: {len(filtered_label)}')
        print(f'valid range: [{valid_start}:{valid_end}]')

        if valid_from_start:
            X_train = np.hstack([f[valid_end:].reshape(-1, 1) for f in filtered_f])
            y_train = filtered_label[valid_end:]
            print(f'train range: [{valid_end}:]')
        else:
            X_train = np.hstack([f[:valid_start].reshape(-1, 1) for f in filtered_f])
            y_train = filtered_label[:valid_start]
            print(f'train range: [:{valid_start}]')

        assert len(X_train) == len(y_train) and len(X_valid) == len(y_valid)

        train_data = lgb.Dataset(
            X_train, y_train, feature_name=names
        )
        valid_data = lgb.Dataset(
            X_valid, y_valid, feature_name=names
        )

        return train_data, valid_data

    @staticmethod
    def _build_sample_mask(ts: np.ndarray, interval: float) -> np.ndarray:
        mask = np.zeros(len(ts), dtype=bool)
        last_t = -np.inf
        for i, t in enumerate(ts):
            if t - last_t >= interval:
                mask[i] = True
                last_t = t
        return mask
