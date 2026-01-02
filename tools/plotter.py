import itertools
from datetime import datetime
from typing import Iterable

import matplotlib.pyplot as plt

from collections import defaultdict
# import mplcyberpunk
# plt.style.use('cyberpunk')
# from qbstyles import mpl_style
# mpl_style(dark=True)

# plt.style.use('https://github.com/lewlin/material_darker_mpl/raw/master/material_darker.mplstyle')


class Plotter:
    def __init__(self):
        """初始化 Plotter 類別，準備存儲點的資料結構"""
        self.data = defaultdict(list)  # 儲存 key-value 對，key 為 str，value 為點列表
        self.colors = {}  # 用於記錄每個 key 的顏色
        self.point_only = {}
        self.key_to_chart_idx = {}
        self.color_cycle = itertools.cycle(plt.cm.tab10.colors)  # 使用固定的顏色循環
        self.is_active = False
        self.chart_amount = 1
        self.point_texts: dict[str, list] = {}
        self.point_pos: dict[str, list] = {}

    def active(self):
        self.is_active = True

    def add_points(self, key: str, point: tuple[datetime, float], point_only=False, chart_idx=0, point_text=None,
                   right=True, down=True):
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
            self.point_texts[key] = []
            self.point_pos[key] = []
        if chart_idx and key not in self.key_to_chart_idx:
            self.key_to_chart_idx[key] = chart_idx
            self.chart_amount = max(self.chart_amount, chart_idx + 1)
        if point_only:
            self.point_pos[key].append((right, down))
            if point_text:
                self.point_texts[key].append(point_text)

        self.data[key].append(point)  # 添加點到對應的 key

    def plot(self):
        """繪圖，將收集的點繪製成圖表並顯示，省略空區間"""
        if not self.data:
            raise ValueError("No data to plot. Add points first.")

        plt.style.use('https://github.com/a00909/matplotlib-stylesheets/raw/master/pitayasmoothie-dark.mplstyle')

        fig, axes = plt.subplots(self.chart_amount, 1, sharex=True, figsize=(10, 6))

        if not isinstance(axes, Iterable):
            axes = [axes]

        for key, points in self.data.items():
            chart_idx = self.key_to_chart_idx.get(key, 0)
            ax = axes[chart_idx]

            x, y = zip(*points)  # 解壓縮點的座標
            if self.point_only.get(key):
                texts = self.point_texts.get(key, None)
                pos = self.point_pos.get(key, None)
                point_count = 0
                for i in range(len(x)):
                    right, down = pos[i]
                    ax.annotate(
                        texts[i] if texts else 'n/a',
                        xy=(x[i], y[i]),
                        xytext=(
                            2 if right else -8,
                            -11 if down else 3
                        ),
                        textcoords="offset points",
                        fontsize=9,
                        bbox=dict(
                            boxstyle='round,pad=0.5',
                            fc='#3BB82E' if down else '#B32525',
                            alpha=0.9,
                            edgecolor="white"
                        ),
                        color='white'
                    )
                    point_count += 1
                linewidth = 0
                markersize = 4
                alpha = 0.7
            else:
                linewidth = 0.9
                markersize = 0
                alpha = 0.7

            ax.plot(
                x, y,
                label=key,
                marker='o',
                # color=self.colors[key],
                linewidth=linewidth,
                markersize=markersize,
                alpha=alpha
            )  # 繪製每組數據

        fig.tight_layout()  # 自動調整布局
        # fig.set_facecolor('#E8E8E8')

        for ax in axes:
            ax.legend(loc='upper right')
            ax.set_facecolor('#161A2E')
            # ax.grid(True,color='#E0E0E0')
            # ax.grid(True, alpha=0.2)
            # ax.tick_params(axis='y', labelcolor='white')

        plt.show()


plotter = Plotter()
