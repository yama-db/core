#!/usr/bin/env python3
# -*- coding: utf-8 -*-


def is_in_cp932(char: str) -> bool:
    try:
        char.encode("cp932")
        return True
    except UnicodeEncodeError:
        return False


def is_in_jisx0213(char: str) -> bool:
    try:
        char.encode("shift_jis_2004")
        return True
    except UnicodeEncodeError:
        return False


def get_men_ku_ten(char: str) -> str:
    if not is_in_jisx0213(char):
        return "Not in JIS X 0213"
    try:
        b = char.encode("euc-jis-2004")
        length = len(b)
        first_byte = b[0]

        if length == 2:
            if 0xA1 <= first_byte <= 0xFE:
                men = 1
                ku = b[0] - 0xA0
                ten = b[1] - 0xA0
                return f"{men}-{ku}-{ten}"
            elif first_byte == 0x8E:
                return "JIS X 0201 (Half-width)"
            else:
                return "Unknown 2-byte code"
        elif length == 3:
            if first_byte == 0x8F:
                men = 2
                ku = b[1] - 0xA0
                ten = b[2] - 0xA0
                return f"{men}-{ku}-{ten}"
            else:
                return "Unknown 3-byte code"
        else:
            return "ASCII or Other"

    except UnicodeEncodeError:
        return "Not in JIS X 0213"


# __END__
