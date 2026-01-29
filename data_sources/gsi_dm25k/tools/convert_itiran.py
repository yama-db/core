#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import re
import struct
import unicodedata


def jis_to_unicode(men: int, ku: int, ten: int) -> tuple[str, str]:
    """
    JIS X 0213 (面-区-点) コードから Unicode コードポイントと漢字を求める関数。

    Args:
        men (int): 面 (1: JIS第一・二水準, 2: JIS第三・四水準)
        ku (int): 区 (1〜94)
        ten (int): 点 (1〜94)

    Returns:
        tuple[str, str]: (Unicodeコードポイント文字列, 漢字)
    """
    if not (1 <= men <= 2 and 1 <= ku <= 94 and 1 <= ten <= 94):
        return "エラー", "指定された面・区・点コードは無効です。"

    # 1. JIS X 0213 のバイト列の生成（EUC-JIS-2004の慣習に従う）

    # 区点番号の計算: (区 - 1) * 94 + (点 - 1)
    # 7ビットコード (JIS X 0208/0213の基本形式)
    jis_code = (ku - 1) * 94 + (ten - 1)

    # 8ビット化: 区番号と点番号をそれぞれ 0x20（32）でオフセットする
    # 実際は JIS → EUC 変換のルールに従う。

    # バイト列の作成（EUC-JIS-2004 形式）
    # 第1面（P1）は通常のEUC-JPと同じ（先頭バイトを 0x80 でマスク）
    # 第2面（P2）は先頭に 0x8F を付ける（3バイト表現）

    # 8ビット化された区と点
    b1 = ku + 0x20  # 区番号を 0x20 オフセット
    b2 = ten + 0x20  # 点番号を 0x20 オフセット

    if men == 1:
        # 第1面 (2バイト): 通常のEUC-JP/EUC-JIS-2004の形式
        # 8ビット化（0x80 を立てる）
        byte_sequence = struct.pack("BB", b1 | 0x80, b2 | 0x80)
        encoding = "euc_jis_2004"  # 適切なエンコーディング

    elif men == 2:
        # 第2面 (3バイト): 0x8F, B1, B2 の形式
        # B1, B2 はそれぞれ 0x80 でマスクされている
        byte_sequence = struct.pack("BBB", 0x8F, b1 | 0x80, b2 | 0x80)
        encoding = "euc_jis_2004"  # 適切なエンコーディング

    else:
        return "エラー", "面番号は1または2のみ有効です。"

    # 2. バイト列をUnicodeにデコードする
    try:
        kanji = byte_sequence.decode(encoding)

        # 3. Unicodeコードポイントを取得
        unicode_code = hex(ord(kanji)).upper()

        return f"U+{unicode_code[2:].zfill(4)}", kanji

    except UnicodeDecodeError:
        return "変換失敗", "指定された区点番号に対応する文字が見つかりませんでした。"
    except Exception as e:
        return "エラー", f"予期せぬエラー: {e}"


def cp932_gaiji_to_pua(cp932_val: int) -> int:
    """
    CP932 (Shift JIS-Windows拡張) の外字コード (2バイト) を
    Unicode PUA (私用領域) のコードポイントに変換する。

    Args:
        cp932_val (int): CP932外字の2バイトコード (例: 0xF040)

    Returns:
        int: 対応するUnicode PUAのコードポイント (例: 0xE000)。
             外字範囲外の場合はNoneを返す。
    """
    try:
        # CP932外字領域の境界定義 (Windows環境の標準)
        # 第一外字領域: F040 〜 F9FC (880文字)
        G1_START = 0xF040
        G1_END = 0xF9FC

        # 第二外字領域: E040 〜 EAE0 (Shift JIS-2004の慣習、Windows外字はF040〜F9FCの再利用)
        # Windows外字エリアでは、JIS X 0213の慣習に倣い、区点番号を再計算します。

        # 2. 第一外字領域 (F040〜F9FC) のチェック
        if G1_START <= cp932_val <= G1_END:
            # Shift JIS → 区点番号への変換に必要なオフセット
            # Shift JISの区点番号は (区 * 188) + (点) で計算される。

            # CP932外字の範囲は、区点番号の 94区 1点 〜 117区 94点 に対応する。
            # ただし、外字では特殊な計算が使われる。

            # 基本オフセット: F040 は Unicode E000 にマッピングされる
            offset = cp932_val - G1_START

            # PUA領域の開始: E000
            PUA_START = 0xE000

            # 3. 換算値
            # CP932のバイト値から、PUAへの通し番号を計算

            # CP932のバイト構成を解析 (例: F040 = F0, 40)
            high_byte = (cp932_val >> 8) & 0xFF
            low_byte = cp932_val & 0xFF

            # JIS区点コード (0x20〜0x7E) に戻す（非外字の場合の慣習）
            # 外字領域の特殊な計算（Windows固有）：

            # 1区ごとの点の数 (94)
            PUNCTUATION_PER_KU = 94

            # 区番号 (10進) の算出:
            ku_index = (high_byte - 0xF0) * PUNCTUATION_PER_KU

            # 点番号 (10進) の算出:
            # 0x40からのオフセット。点コード 0x40〜0x7E, 0x80〜0xFC

            if low_byte <= 0x7E:
                # 0x40〜0x7Eの範囲
                ten_index = low_byte - 0x40
            elif 0x80 <= low_byte <= 0xFC:
                # 0x80〜0xFCの範囲（連続した文字）
                ten_index = low_byte - 0x80 + (0x7E - 0x40 + 1)
            else:
                # 区の終端が 0xFC でない、または不正な点コード
                return None

            # 通し番号（インデックス）を計算
            gaiji_index = ku_index + ten_index

            # 最終的なPUAコードポイント
            pua_val = PUA_START + gaiji_index

            # PUA-A の上限 E757 を超えるかどうかのチェック
            if pua_val > 0xE757:
                # PUA-A (E000-E757) を超える分は第二外字として、次の領域(E758-)に配置される
                # このマッピングは非常に複雑で、OSのバージョンによっても異なるため、
                # 一般的な第一外字領域のみをシンプルに扱うことが多い。
                pass  # エラー処理を省略し、そのまま進める

            return pua_val

        else:
            # 外字領域外
            return None

    except ValueError:
        return None


# メイン処理

file_path = "workspace/itiran.csv"

try:
    with open(file_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = int(unicodedata.normalize("NFKC", row["コード"]), 16)
            pua_code = cp932_gaiji_to_pua(code)
            if code < 0xF940:
                pua_code += 0xE61F - 0xE32F
            else:
                pua_code += 0xE69C - 0xE34E
            kuten = row["ＪＩＳ第3、4水準"]
            if m := re.match(r"^P(\d+)-(\d+)-(\d+)$", kuten):
                men, ku, ten = m.groups()
                unicode_code, kanji = jis_to_unicode(
                    men=int(men), ku=int(ku), ten=int(ten)
                )
                print(f'"{kanji}", # {pua_code:04X},{code:04X},{kuten}')
            else:
                print(f'"❓", # {pua_code:04X},{code:04X}')

except FileNotFoundError:
    print(f"エラー: ファイル '{file_path}' が見つかりません。")
except UnicodeDecodeError as e:
    print(
        f"エラー: Shift JISとしてファイルをデコードできませんでした。ファイルが壊れているか、別のエンコーディングの可能性があります。"
    )
    print(f"詳細: {e}")
