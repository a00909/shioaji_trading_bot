import itertools
from datetime import datetime

import matplotlib.pyplot as plt
from collections import defaultdict


class Plotter:
    def __init__(self):
        """初始化 Plotter 類別，準備存儲點的資料結構"""
        self.data = defaultdict(list)  # 儲存 key-value 對，key 為 str，value 為點列表
        self.colors = {}  # 用於記錄每個 key 的顏色
        self.point_only = {}
        self.key_to_chart_idx = {}
        self.color_cycle = itertools.cycle(plt.cm.tab10.colors)  # 使用固定的顏色循環
        self.is_active = False

    def active(self):
        self.is_active = True

    def add_points(self, key: str, point: tuple[datetime, float], point_only=False, chart_idx=0):
        """接收 key-value pair，key 為 str，value 為點 (list of tuples)

        Args:
            key (str): 資料組的名稱
            point (tuple): 點 (x,y)
        """
        if not self.is_active:
            return

        if not isinstance(key, str):
            raise ValueError("Key must be a string.")
        if not isinstance(point, tuple) and len(point) == 2:
            raise ValueError("Points must be a list of (x, y) tuples.")

        if key not in self.colors:
            self.colors[key] = next(self.color_cycle)  # 為新 key 分配顏色
        if point_only and key not in self.point_only:
            self.point_only[key] = point_only
        if chart_idx and key not in self.key_to_chart_idx:
            self.key_to_chart_idx[key] = chart_idx

        self.data[key].append(point)  # 添加點到對應的 key

    def plot(self):
        """繪圖，將收集的點繪製成圖表並顯示，省略空區間"""
        if not self.data:
            raise ValueError("No data to plot. Add points first.")


        fig, axes = plt.subplots(len(self.key_to_chart_idx.keys()), 1, sharex=True, figsize=(10, 6))

        for key, points in self.data.items():
            chart_idx = self.key_to_chart_idx.get(key)
            if chart_idx:
                ax = axes[chart_idx]
            else:
                ax = axes[0]

            if self.point_only.get(key):
                linewidth = 0
                markersize = 5
                alpha = 1
            else:
                linewidth = 0.7
                markersize = 0
                alpha = 0.5
            x, y = zip(*points)  # 解壓縮點的座標
            ax.plot(
                x, y,
                label=key,
                marker='o',
                color=self.colors[key],
                linewidth=linewidth,
                markersize=markersize,
                alpha=alpha
            )  # 繪製每組數據

        fig.tight_layout()  # 自動調整布局

        for ax in axes:
            ax.legend(loc='upper right')
            ax.grid(True, alpha=0.2)
            ax.tick_params(axis='y', labelcolor='blue')

        plt.show()




plotter = Plotter()
