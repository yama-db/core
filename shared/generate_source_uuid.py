#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import uuid

MY_NAMESPACE = uuid.UUID("ef11cf74-18d0-456b-85e8-895a3d93719c")


# UUIDを生成する
def generate_source_uuid(source_name: str, remote_id: str) -> uuid.UUID:
    return uuid.uuid5(MY_NAMESPACE, f"{source_name}:{remote_id}")


if __name__ == "__main__":
    # テストコード
    source_name = "test_source"
    remote_id = "12345"
    generated_uuid = generate_source_uuid(source_name, remote_id)
    print(
        f"Generated UUID for source '{source_name}' and remote ID '{remote_id}': {generated_uuid}"
    )

# __END__
