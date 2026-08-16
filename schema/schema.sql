-- スキーマ定義（MySQL 8.0以降を想定）

CREATE TABLE administrative_regions (
    jis_code CHAR(5) COLLATE ascii_bin NOT NULL PRIMARY KEY COMMENT '行政区画コード',
    pref_name VARCHAR(10) NOT NULL COMMENT '都道府県名',
    city_name VARCHAR(50) NOT NULL COMMENT '市区町村名',
    full_name VARCHAR(60) GENERATED ALWAYS AS (
        concat(pref_name, city_name)
    ) STORED COMMENT '行政区画名称',
    pref_code CHAR(2) COLLATE ascii_bin GENERATED ALWAYS AS (
        left(jis_code, 2)
    ) STORED COMMENT '都道府県コード',
    city_qid VARCHAR(20) COLLATE ascii_bin NOT NULL COMMENT '市区町村 QID',
    pref_qid VARCHAR(20) COLLATE ascii_bin NOT NULL COMMENT '都道府県 QID',
    INDEX idx_pref_code (pref_code),
    INDEX idx_full_name (full_name)
) COMMENT '行政区画コード／名称';

CREATE TABLE administrative_boundaries (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '境界データID',
    jis_code CHAR(5) COLLATE ascii_bin NOT NULL COMMENT '行政区画コード',
    geom POLYGON /*!80003 SRID 4326 */ NOT NULL COMMENT '境界データ',
    FOREIGN KEY (jis_code) REFERENCES administrative_regions (jis_code),
    SPATIAL INDEX (geom)
) COMMENT '行政区画境界データ';

CREATE TABLE information_sources (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '情報源ID',
    info_type ENUM('DATASET', 'BOOK', 'WEBPAGE') NOT NULL COMMENT '情報源種別',
    source_table VARCHAR(64) COLLATE ascii_bin NOT NULL COMMENT '参照先テーブル名',
    display_name VARCHAR(100) NOT NULL UNIQUE COMMENT '表示用名称',
    url VARCHAR(2083) COMMENT '情報源URL',
    reliability_level TINYINT NOT NULL DEFAULT 50 COMMENT '信頼度順位（0が最高）'
) COMMENT '情報源リスト';

CREATE TABLE _stg_template_pois (
    source_uuid BINARY(16) PRIMARY KEY COMMENT 'UUID v5',
    raw_id VARCHAR(100) COLLATE ascii_bin NOT NULL COMMENT 'データ識別子',
    raw_type VARCHAR(100) NOT NULL COMMENT 'データ種別',
    mountain_id INT COMMENT '統合ID',
    names_json JSON NOT NULL COMMENT '山名・よみ',
    geom POINT /*!80003 SRID 4326 */ NOT NULL COMMENT '地理座標',
    lat DECIMAL(9, 6) GENERATED ALWAYS AS (
        st_latitude(geom)
    ) STORED COMMENT '緯度',
    lon DECIMAL(10, 6) GENERATED ALWAYS AS (
        st_longitude(geom)
    ) STORED COMMENT '経度',
    elevation DECIMAL(7, 2) COMMENT '標高[m]',
    x_z18 MEDIUMINT UNSIGNED COMMENT 'タイルX座標',
    y_z18 MEDIUMINT UNSIGNED COMMENT 'タイルY座標',
    z_min INT COMMENT '最小表示Zレベル',
    last_updated_at DATETIME COMMENT '更新日時',
    UNIQUE INDEX uq_raw_id (raw_id),
    SPATIAL INDEX (geom),
    INDEX idx_tile_x_y (x_z18, y_z18),
    INDEX idx_mountain_id (mountain_id)
) COMMENT 'POIテーブル共通構造';

/* names_jsonの例：
[
    {"name": "羊蹄山", "kana": "ようていざん", "type": "MAIN"},
    {"name": "蝦夷富士", "kana": "えぞふじ", "type": "ALIAS"},
]
*/

CREATE TABLE stg_gsi_gcp_pois LIKE _stg_template_pois;
ALTER TABLE stg_gsi_gcp_pois COMMENT '基盤地図情報（基準点・標高点）';

CREATE TABLE stg_gsi_1003_pois LIKE _stg_template_pois;
ALTER TABLE stg_gsi_1003_pois COMMENT '日本の主な山岳標高（1003山）';

CREATE TABLE stg_gsi_dm25k_pois LIKE _stg_template_pois;
ALTER TABLE stg_gsi_dm25k_pois COMMENT '数値地図25000（地名・公共施設）';

CREATE TABLE stg_gsi_vtexp_pois LIKE _stg_template_pois;
ALTER TABLE stg_gsi_vtexp_pois COMMENT 'ベクトルタイル提供実験（地名情報）';

CREATE TABLE stg_wikidata_pois LIKE _stg_template_pois;
ALTER TABLE stg_wikidata_pois COMMENT 'ウィキデータ';

CREATE TABLE stg_yamap_pois LIKE _stg_template_pois;
ALTER TABLE stg_yamap_pois COMMENT 'ヤマップ';

CREATE TABLE stg_yamareco_pois LIKE _stg_template_pois;
ALTER TABLE stg_yamareco_pois COMMENT 'ヤマレコ';

CREATE TABLE stg_legacy_pois LIKE _stg_template_pois;
ALTER TABLE stg_legacy_pois COMMENT '山名一覧 on the Web地図';

CREATE TABLE stg_book_web_pois (
    source_uuid BINARY(16) PRIMARY KEY COMMENT 'UUID v5',
    source_id INT NOT NULL COMMENT '情報源ID',
    raw_id VARCHAR(100) COLLATE ascii_bin NOT NULL COMMENT 'データ識別子',
    raw_type VARCHAR(100) NOT NULL COMMENT 'データ種別',
    mountain_id INT NOT NULL COMMENT '統合ID',
    names_json JSON NOT NULL COMMENT '山名・よみ',
    geom POINT /*!80003 SRID 4326 */ COMMENT '地理座標',
    lat DECIMAL(9, 6) GENERATED ALWAYS AS (
        if(geom IS NOT NULL, st_latitude(geom), NULL)
    ) STORED COMMENT '緯度',
    lon DECIMAL(10, 6) GENERATED ALWAYS AS (
        if(geom IS NOT NULL, st_longitude(geom), NULL)
    ) STORED COMMENT '経度',
    elevation DECIMAL(7, 2) COMMENT '標高[m]',
    x_z18 MEDIUMINT UNSIGNED COMMENT 'タイルX座標',
    y_z18 MEDIUMINT UNSIGNED COMMENT 'タイルY座標',
    z_min INT COMMENT '最小表示Zレベル',
    last_updated_at DATETIME COMMENT '更新日時',
    UNIQUE INDEX uq_source_raw_id (source_id, raw_id),
    INDEX idx_tile_x_y (x_z18, y_z18),
    INDEX idx_mountain_id (mountain_id)
) COMMENT '書籍・ウェブサイト';

CREATE TABLE mountain_pois (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '統合ID',
    is_used BOOLEAN NOT NULL DEFAULT FALSE COMMENT '使用中フラグ',
    main_child_id INT COMMENT '代表子POI ID',
    main_name VARCHAR(255) COMMENT '代表名称',
    main_kana VARCHAR(255) COMMENT '代表よみがな',
    geom POINT /*!80003 SRID 4326 */ NOT NULL COMMENT '地理座標',
    lat DECIMAL(9, 6) GENERATED ALWAYS AS (
        st_latitude(geom)
    ) STORED COMMENT '緯度',
    lon DECIMAL(10, 6) GENERATED ALWAYS AS (
        st_longitude(geom)
    ) STORED COMMENT '経度',
    elevation DECIMAL(7, 2) NOT NULL COMMENT '標高',
    x_z18 MEDIUMINT UNSIGNED COMMENT 'タイルX座標',
    y_z18 MEDIUMINT UNSIGNED COMMENT 'タイルY座標',
    z_min INT COMMENT '最小表示Zレベル',
    last_updated_at DATETIME COMMENT '更新日時',
    SPATIAL INDEX (geom),
    INDEX idx_tile_x_y (x_z18, y_z18),
    CONSTRAINT fk_mountain_main_child
    FOREIGN KEY (main_child_id) REFERENCES mountain_pois (id)
    ON DELETE SET NULL ON UPDATE CASCADE
) COMMENT '山岳統合POI';

CREATE TABLE poi_hierarchies (
    parent_id INT NOT NULL COMMENT '親POI ID',
    parent_name VARCHAR(255) NOT NULL COMMENT '親POI名称',
    child_id INT NOT NULL COMMENT '子POI ID',
    child_name VARCHAR(255) NOT NULL COMMENT '子POI名称',
    relation_type ENUM(
        'AREA_TO_PEAK',
        'MAIN_TO_SUB_PEAK'
    ) NOT NULL COMMENT '関係種別',
    PRIMARY KEY (parent_id, child_id),
    FOREIGN KEY (parent_id) REFERENCES mountain_pois (id)
    ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (child_id) REFERENCES mountain_pois (id)
    ON DELETE CASCADE ON UPDATE CASCADE,
    INDEX idx_child_id (child_id)
) COMMENT '統合POIの階層構造';

CREATE TABLE poi_links (
    mountain_id INT NOT NULL COMMENT '統合ID',
    source_id INT NOT NULL COMMENT '情報源ID',
    source_uuid BINARY(16) NOT NULL COMMENT '情報源UUID',
    linked_distance FLOAT NOT NULL COMMENT '統合・情報源間距離[m]',
    PRIMARY KEY (source_uuid, mountain_id),
    FOREIGN KEY (mountain_id) REFERENCES mountain_pois (id),
    FOREIGN KEY (source_id) REFERENCES information_sources (id),
    INDEX idx_source_uuid (source_uuid),
    INDEX idx_mountain_id (mountain_id)
) COMMENT '統合POIと情報源の関連付け';

CREATE TABLE char_trans_map (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    src_char VARCHAR(4) COLLATE utf8mb4_bin NOT NULL COMMENT '異体字',
    dst_char VARCHAR(4) COLLATE utf8mb4_bin NOT NULL COMMENT '変換先',
    hit_count INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '出現件数',
    sample_names TEXT COMMENT '出現例（カンマ区切り）',
    PRIMARY KEY (id),
    UNIQUE KEY uk_src_char (src_char),
    KEY idx_hit_count (hit_count DESC)
) COMMENT '異体字変換テーブル';

CREATE TABLE poi_names (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '名称ID',
    mountain_id INT NOT NULL COMMENT '統合ID',
    source_uuid BINARY(16) NOT NULL COMMENT '情報源UUID',
    source_id INT NOT NULL COMMENT '情報源ID',
    poi_name VARCHAR(255) COLLATE utf8mb4_bin NOT NULL COMMENT '名称',
    poi_name_normalized VARCHAR(255)
    COLLATE utf8mb4_bin NOT NULL COMMENT '検索用正規化名称',
    poi_kana VARCHAR(255) COLLATE utf8mb4_bin NOT NULL COMMENT 'よみがな',
    name_type ENUM(
        'MAIN', 'AREA', 'SUB_PEAK', 'ALIAS'
    ) NOT NULL DEFAULT 'MAIN' COMMENT '名称種別',
    is_preferred BOOLEAN NOT NULL DEFAULT FALSE COMMENT '代表名称フラグ',
    FOREIGN KEY (mountain_id) REFERENCES mountain_pois (id),
    FOREIGN KEY (source_id) REFERENCES information_sources (id),
    INDEX idx_source_uuid (source_uuid),
    INDEX idx_poi_name_normalized (poi_name_normalized),
    INDEX idx_mountain_id (mountain_id)
) COMMENT '統合POI名称';

CREATE TABLE poi_address_map (
    mountain_id INT NOT NULL COMMENT '統合ID',
    jis_code CHAR(5) COLLATE ascii_bin NOT NULL COMMENT '行政区画コード',
    PRIMARY KEY (mountain_id, jis_code),
    INDEX idx_jis_code (jis_code),
    CONSTRAINT fk_poi_address_mountain
    FOREIGN KEY (mountain_id) REFERENCES mountain_pois (id)
    ON DELETE CASCADE,
    CONSTRAINT fk_poi_address_region
    FOREIGN KEY (jis_code) REFERENCES administrative_regions (jis_code)
    ON DELETE RESTRICT
) COMMENT '統合POIと行政区画の関連付け';


CREATE TABLE mountain_records (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '山行記録ID',
    start_date DATE NOT NULL COMMENT '開始日',
    end_date DATE NOT NULL COMMENT '終了日',
    published_at DATETIME COMMENT '公開日',
    title VARCHAR(255) NOT NULL COMMENT 'タイトル',
    summary TEXT COMMENT '概略',
    public_url VARCHAR(2083) COMMENT '公開URL',
    image_url VARCHAR(2083) COMMENT '画像URL',
    INDEX idx_start_date (start_date),
    INDEX idx_published_at (published_at)
) COMMENT '山行記録';

CREATE TABLE visited_mountains (
    mountain_record_id INT NOT NULL COMMENT '山行記録ID',
    mountain_id INT NOT NULL COMMENT '統合ID',
    climb_date DATE COMMENT '登頂日',
    climb_order INT NOT NULL COMMENT '登頂順',
    PRIMARY KEY (mountain_record_id, mountain_id),
    CONSTRAINT fk_visited_mountains_record
    FOREIGN KEY (mountain_record_id) REFERENCES mountain_records (id)
    ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_visited_mountains_poi
    FOREIGN KEY (mountain_id) REFERENCES mountain_pois (id)
    ON DELETE RESTRICT ON UPDATE CASCADE,
    INDEX idx_mountain_id (mountain_id)
) COMMENT '登頂リスト';
