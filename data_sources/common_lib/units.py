#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re


#
# 度分秒形式の文字列を10進度に変換
#
def dms2deg(dms_str: str) -> float:
    r = re.match(r"^(\d+)(\d\d)(\d\d(\.\d+)?)$", dms_str)
    d = float(r.group(1))
    m = float(r.group(2))
    s = float(r.group(3))
    return d + (m / 60) + (s / 3600)


# __END__
