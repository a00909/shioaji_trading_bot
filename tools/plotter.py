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
        self.in_second_chart = {}
        self.color_cycle = itertools.cycle(plt.cm.tab10.colors)  # 使用固定的顏色循環
        self.is_active = False

    def active(self):
        self.is_active = True

    def add_points(self, key: str, point: tuple[datetime, float], point_only=False, in_second_chart=False):
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
        if in_second_chart and key not in self.in_second_chart:
            self.in_second_chart[key] = in_second_chart

        self.data[key].append(point)  # 添加點到對應的 key

    def plot(self):
        """繪圖，將收集的點繪製成圖表並顯示，省略空區間"""
        if not self.data:
            raise ValueError("No data to plot. Add points first.")

        # plt.figure(figsize=(10, 6))

        fig, ax1 = plt.subplots(figsize=(10, 6))
        ax2 = None
        ax2_max = 0

        for key, points in self.data.items():
            if self.in_second_chart.get(key):
                if not ax2:
                    ax2 = ax1.twinx()
                ax = ax2
                ax2_max = max(max(p[1] for p in points), ax2_max)
            else:
                ax = ax1

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

        ax1.legend(loc='upper left')
        ax1.set_xlabel("X-axis")
        ax1.set_ylabel("Primary Y-axis", color='blue')
        ax1.grid(True, alpha=0.2)
        ax1.tick_params(axis='y', labelcolor='blue')



        if ax2:
            ax2.legend(loc='upper right')
            ax2.set_ylabel("Secondary Y-axis", color='green')
            ax2.tick_params(axis='y', labelcolor='green')
            ax2.set_ylim(0, ax2_max * 2)  # 設置固定的 Y 軸範圍
            # ax2.set_autoscale_on(False)  # 禁用自動縮放
            # def on_xlim_changed(event_ax):
            #     ax2.set_ylim(0, ax2_max * 1.5)  # 重置副 Y 轴范围
            #
            # ax1.callbacks.connect('xlim_changed', on_xlim_changed)



        plt.title("Data Points Visualization")
        plt.show()


plotter = Plotter()
