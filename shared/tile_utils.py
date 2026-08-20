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


def lonlat_to_tile_coords(
    lat: float, lon: float, z: int, extent: int = 4096
) -> tuple[int, int, float, float]:
    """緯度・経度・ズームレベルからタイル番号とタイル内相対座標（実数値）を計算する。

    Parameters
    ----------
    lat : float
        緯度 (-85.05112878 <= lat <= 85.05112878)
    lon : float
        経度 (-180.0 <= lon <= 180.0)
    z : int
        ズームレベル
    extent : int, optional
        タイルの解像度 (デフォルト: 4096)

    Returns
    -------
    tuple[int, int, float, float]
        (tile_x, tile_y, local_x, local_y)
        - tile_x, tile_y: タイルインデックス (int)
        - local_x, local_y: タイル内相対座標の実数値 (float, 0.0 <= val < extent)

    """
    n = 1 << z  # 2^z

    # 1. 経度・緯度からグローバルなタイル座標（小数含む）を算出 (Webメルカトル)
    global_x = (lon + 180.0) / 360.0 * n

    lat_rad = math.radians(lat)
    global_y = (
        (1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n
    )  # math.log(math.tan(...) + 1/math.cos(...)) と等価

    # 2. タイル番号 (整数部)
    tile_x = int(global_x)
    tile_y = int(global_y)

    # 3. タイル内相対座標 (小数部 * extent)
    local_x = (global_x - tile_x) * extent
    local_y = (global_y - tile_y) * extent

    return tile_x, tile_y, local_x, local_y


# --------------------------------------------------
# 使用例（東京駅: 経度 139.767125, 緯度 35.681236, z=13）
# --------------------------------------------------
if __name__ == "__main__":
    lat = 35.681236
    lon = 139.767125
    z = 13

    tile_x, tile_y, local_x, local_y = lonlat_to_tile_coords(lat, lon, z)

    print(f"ズームレベル: Z={z}")
    print(f"タイル座標  : ({tile_x}, {tile_y})")
    print(f"タイル内座標: ({local_x:.6f}, {local_y:.6f})")

    # 前述の固定小数点化（128倍 / << 7）して整数保存する場合の値
    scaled_x = round(local_x * 128)
    scaled_y = round(local_y * 128)
    print(f"DB格納用(128倍整数): local_x={scaled_x}, local_y={scaled_y}")

# __END__
