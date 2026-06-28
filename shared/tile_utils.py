#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import math


def lnglat_to_tile(lon: float, lat: float, zoom: int) -> tuple[int, int]:
    """
    指定した経度・緯度・ズームレベルからタイル座標 (x, y) を計算します。

    Args:
        lon (float): 経度（度、-180.0 ～ 180.0）
        lat (float): 緯度（度、-85.0511 ～ 85.0511）
        zoom (int): ズームレベル（0 以上の整数）

    Returns:
        tuple[int, int]: タイル座標 (x, y)
    """
    # 1. 該当ズームレベルにおける全タイル数（1辺あたり）を計算（2のzoom乗）
    n = 1 << zoom  # 2 ** zoom と同等（ビットシフトで高速化）

    # 2. 経度をタイルX座標にマッピング
    # 経度 -180～180度 を 0～n に比例分配する
    x_tile = int((lon + 180.0) / 360.0 * n)

    # 3. 緯度をタイルY座標にマッピング
    # Webメルカトルの投影公式を使用。
    # 緯度が北緯/南緯 85.0511度 を超えると計算結果が無限大に近づくため、安全のためにクリップ（制限）する
    lat_rad = math.radians(lat)

    # 境界値エラーを防ぐための安全弁
    if lat_rad > 1.4844222297453322:  # math.atan(math.sinh(math.pi)) の値
        lat_rad = 1.4844222297453322
    elif lat_rad < -1.4844222297453322:
        lat_rad = -1.4844222297453322

    y_tile = int(
        (1.0 - math.log(math.tan(lat_rad) + (1.0 / math.cos(lat_rad))) / math.pi)
        / 2.0
        * n
    )

    # 4. 計算誤差や境界値によるインデックスのオーバーフローを防ぐためのガード
    # 座標が最大値（n）に達してしまった場合は、配列のインデックス内に収まるよう調整する
    x_tile = max(0, min(x_tile, n - 1))
    y_tile = max(0, min(y_tile, n - 1))

    return x_tile, y_tile


def lnglat_to_tile_fraction(lon: float, lat: float, zoom: int) -> tuple[float, float]:
    """
    【おまけ】タイル内の詳細な位置（ピクセル位置）を計算したい場合に使用する、
    切り捨て（int型変換）を行う前の「小数点付き」のタイル座標を返します。
    """
    n = 1 << zoom
    x_fraction = (lon + 180.0) / 360.0 * n

    lat_rad = math.radians(lat)
    # クリップ処理
    lat_rad = max(-1.4844222297453322, min(lat_rad, 1.4844222297453322))

    y_fraction = (
        (1.0 - math.log(math.tan(lat_rad) + (1.0 / math.cos(lat_rad))) / math.pi)
        / 2.0
        * n
    )

    return x_fraction, y_fraction


if __name__ == "__main__":
    # テストデータ: 富士山頂（剣ヶ峰）の座標
    fuji_lon = 138.727778
    fuji_lat = 35.360556

    print("--- 富士山頂のタイル座標計算 ---")
    for z in [0, 5, 10, 15, 18]:
        x, y = lnglat_to_tile(fuji_lon, fuji_lat, zoom=z)
        print(f"ズームレベル z={z:2d}  ->  タイル座標 (x: {x}, y: {y})")
        # 地図URLの例
        print(
            f"  地理院地図URL: https://cyberjapandata.gsi.go.jp/xyz/std/{z}/{x}/{y}.png"
        )

# __END__
