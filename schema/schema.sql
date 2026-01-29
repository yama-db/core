-- Schema for creating tables related to POI (Points of Interest) management

CREATE TABLE information_sources (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '情報源ID',
    source_type ENUM('DIGITAL', 'BOOK', 'JOURNAL') NOT NULL COMMENT '情報源の種別',
    display_name VARCHAR(100) NOT NULL COMMENT '表示用名称',
    reliability_level TINYINT DEFAULT 3 COMMENT '信頼度レベル(1-5)'
) COMMENT '情報源マスタテーブル';

CREATE TABLE digital_service_details (
    source_id INT PRIMARY KEY COMMENT '情報源ID',
    organization_name VARCHAR(100) COMMENT '運営組織名',
    base_url VARCHAR(255) COLLATE ascii_bin COMMENT 'サービスURL',
    last_imported_at DATETIME COMMENT '最終取込日時',
    FOREIGN KEY (source_id) REFERENCES information_sources(id)
) COMMENT 'デジタルサービス情報詳細';

CREATE TABLE book_details (
    source_id INT PRIMARY KEY COMMENT '情報源ID',
    formal_title VARCHAR(255) NOT NULL COMMENT '正式タイトル',
    ndl_id VARCHAR(100) COLLATE ascii_bin COMMENT 'NDL Search ID',
    author VARCHAR(100) COMMENT '著者',
    publisher VARCHAR(100) COMMENT '出版社',
    published_date DATE COMMENT '発行日',
    FOREIGN KEY (source_id) REFERENCES information_sources(id)
) COMMENT '書籍情報詳細';

CREATE TABLE administrative_regions (
    jis_code CHAR(5) COLLATE ascii_bin NOT NULL PRIMARY KEY COMMENT '行政区画コード',
    pref_name VARCHAR(10) NOT NULL COMMENT '都道府県名',
    city_name VARCHAR(50) NOT NULL COMMENT '市区町村名',
    wikidata_qid VARCHAR(20) COLLATE ascii_bin COMMENT 'Wikidata QID',
    pref_code CHAR(2) COLLATE ascii_bin GENERATED ALWAYS AS (LEFT(jis_code, 2)) VIRTUAL COMMENT '都道府県コード',
    INDEX idx_pref_code (pref_code)
) COMMENT '行政区画マスタテーブル';

CREATE TABLE administrative_boundaries (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '境界データID',
    jis_code CHAR(5) COLLATE ascii_bin NOT NULL COMMENT '行政区画コード',
    geom POLYGON NOT NULL /*!80003 SRID 4326 */ COMMENT '境界ジオメトリ',
    FOREIGN KEY (jis_code) REFERENCES administrative_regions(jis_code),
    SPATIAL INDEX(geom)
) COMMENT '行政区画境界データ';

CREATE TABLE poi_categories (
    id VARCHAR(20) COLLATE ascii_bin PRIMARY KEY COMMENT '種別ID',
    display_name VARCHAR(50) NOT NULL COMMENT '表示名称',
    icon_name VARCHAR(50) COMMENT 'アイコン名'
) COMMENT 'POI種別マスタテーブル';

CREATE TABLE stg_gsi_dm25k_pois (
    source_uuid BINARY(16) PRIMARY KEY COMMENT 'UUID v5',
    raw_remote_id VARCHAR(100) COLLATE ascii_bin COMMENT '元データのID',
    names_json JSON COMMENT '名称配列(別名含む)',
    geom POINT NOT NULL /*!80003 SRID 4326 */ COMMENT '地理位置',
    elevation_m DECIMAL(6, 1) COMMENT '標高（推定）',
    -- 個別の属性
    poi_type_raw VARCHAR(50) COMMENT '大分類-中分類-小分類',
    last_updated_at DATETIME COMMENT 'データ更新日',
    SPATIAL INDEX(geom)
) COMMENT '国土地理院 数値地図(地名情報)';

CREATE TABLE stg_gsi_vtexp_pois (
    source_uuid BINARY(16) PRIMARY KEY COMMENT 'UUID v5',
    raw_remote_id VARCHAR(100) COLLATE ascii_bin COMMENT '元データのID',
    names_json JSON COMMENT '名称配列(別名含む)',
    geom POINT NOT NULL /*!80003 SRID 4326 */ COMMENT '地理位置',
    elevation_m DECIMAL(6, 1) COMMENT '標高（推定）',
    -- 個別の属性
    poi_type_raw VARCHAR(50) COMMENT '注記分類',
    last_updated_at DATETIME COMMENT 'lfSpanFr',
    SPATIAL INDEX(geom)
) COMMENT '国土地理院 自然地名ベクトルタイル';

CREATE TABLE stg_gsi_gcp_pois (
    source_uuid BINARY(16) PRIMARY KEY COMMENT 'UUID v5',
    raw_remote_id VARCHAR(100) COLLATE ascii_bin COMMENT '元データのID',
    names_json JSON COMMENT '名称配列(点名)',
    geom POINT NOT NULL /*!80003 SRID 4326 */ COMMENT '地理位置',
    elevation_m DECIMAL(6, 1) COMMENT '標高',
    -- 個別の属性
    poi_type_raw VARCHAR(50) COMMENT '基準点の種類',
    last_updated_at DATETIME COMMENT 'データ更新日',
    SPATIAL INDEX(geom)
) COMMENT '国土地理院 基準点データ';

CREATE TABLE stg_yamap_pois (
    source_uuid BINARY(16) PRIMARY KEY COMMENT 'UUID v5',
    raw_remote_id VARCHAR(100) COLLATE ascii_bin COMMENT 'API上の数値ID等',
    names_json JSON COMMENT '名称配列(別名含む)',
    geom POINT NOT NULL /*!80003 SRID 4326 */ COMMENT '地理位置',
    elevation_m DECIMAL(6, 1) COMMENT '標高',
    -- 個別の属性
    poi_type_raw VARCHAR(50) COMMENT '元データの種別名',
    page_url VARCHAR(255) COLLATE ascii_bin COMMENT 'WebページURL',
    last_updated_at DATETIME COMMENT 'データ更新日',
    SPATIAL INDEX(geom)
) COMMENT 'YAMAP Landmarks';

CREATE TABLE stg_yamareco_pois (
    source_uuid BINARY(16) PRIMARY KEY COMMENT 'UUID v5',
    raw_remote_id VARCHAR(100) COLLATE ascii_bin COMMENT 'API上の数値ID等',
    names_json JSON COMMENT '名称配列(別名含む)',
    geom POINT NOT NULL /*!80003 SRID 4326 */ COMMENT '地理位置',
    elevation_m DECIMAL(6, 1) COMMENT '標高',
    -- 個別の属性
    poi_type_raw VARCHAR(255) COMMENT '元データの種別名',
    page_url VARCHAR(255) COLLATE ascii_bin COMMENT 'WebページURL',
    last_updated_at DATETIME COMMENT 'データ更新日',
    SPATIAL INDEX(geom)
) COMMENT 'YamaReco POIデータ';

CREATE TABLE stg_wikidata_pois (
    source_uuid BINARY(16) PRIMARY KEY COMMENT 'UUID v5',
    raw_remote_id VARCHAR(100) COLLATE ascii_bin COMMENT 'Wikidata QID',
    names_json JSON COMMENT '名称配列(別名含む)',
    geom POINT /*!80003 SRID 4326 */ COMMENT '地理位置',
    elevation_m DECIMAL(6, 1) COMMENT '標高',
    -- 個別の属性
    poi_type_raw VARCHAR(50) COMMENT '元データの種別名',
    page_url VARCHAR(255) COLLATE ascii_bin COMMENT 'WebページURL',
    last_updated_at DATETIME COMMENT 'データ更新日'
) COMMENT 'Wikidata POIデータ';

CREATE TABLE stg_book_pois (
    source_uuid BINARY(16) PRIMARY KEY COMMENT 'UUID v5',
    raw_remote_id VARCHAR(100) COLLATE ascii_bin COMMENT '掲載ページ・項番等',
    names_json JSON COMMENT '名称配列(別名含む)',
    geom POINT /*!80003 SRID 4326 */ COMMENT '地理位置（推定）',
    elevation_m DECIMAL(6, 1) COMMENT '標高',
    -- 個別の属性
    source_id INT NOT NULL COMMENT '情報源ID',
    unified_poi_id INT COMMENT '統合実体ID',
    poi_type_raw VARCHAR(50) COMMENT '分類(名峰、里山等)',
    description_text TEXT COMMENT '説明文',
    FOREIGN KEY (source_id) REFERENCES information_sources(id),
    FOREIGN KEY (unified_poi_id) REFERENCES unified_pois(id)
) COMMENT '書籍由来のPOIデータ';

CREATE TABLE unified_pois (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '統合実体ID',
    category_id VARCHAR(20) COLLATE ascii_bin NOT NULL COMMENT '種別ID',
    representative_name VARCHAR(255) COLLATE utf8mb4_bin COMMENT '代表名称(異体字保持)',
    representative_geom POINT NOT NULL /*!80003 SRID 4326 */ COMMENT '代表座標',
    display_lat DECIMAL(10, 7) GENERATED ALWAYS AS (ST_Latitude(representative_geom)) VIRTUAL COMMENT '緯度',
    display_lon DECIMAL(11, 7) GENERATED ALWAYS AS (ST_Longitude(representative_geom)) VIRTUAL COMMENT '経度',
    elevation_m DECIMAL(6, 1) COMMENT '代表標高',
    address_text VARCHAR(255) COMMENT '表示用所在地',
    min_zoom_level TINYINT NOT NULL DEFAULT 13 COMMENT '表示開始ズームレベル',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '作成日時',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新日時',
    FOREIGN KEY (category_id) REFERENCES poi_categories(id),
    SPATIAL INDEX(representative_geom),
    INDEX idx_category_zoom (category_id, min_zoom_level)
) COMMENT '統合POIテーブル';

CREATE TABLE poi_links (
    unified_poi_id INT NOT NULL COMMENT '統合実体ID',
    source_type ENUM (
        'GSI_GCP',
        'GSI_VTEXP',
        'GSI_DM25K',
        'YAMAP',
        'YAMARECO',
        'WIKIDATA',
        'BOOK'
    ) NOT NULL COMMENT '参照先テーブル種別',
    source_uuid BINARY(16) NOT NULL COMMENT '情報源UUID',
    PRIMARY KEY (unified_poi_id, source_uuid),
    FOREIGN KEY (unified_poi_id) REFERENCES unified_pois(id),
    INDEX idx_source_uuid (source_uuid)
) COMMENT 'POIと元データの関連付けテーブル';

CREATE TABLE poi_administrative_regions (
    unified_poi_id INT NOT NULL COMMENT '統合実体ID',
    jis_code CHAR(5) COLLATE ascii_bin NOT NULL COMMENT '行政区画コード',
    is_primary BOOLEAN DEFAULT FALSE COMMENT '主所在地フラグ',
    PRIMARY KEY (unified_poi_id, jis_code),
    FOREIGN KEY (unified_poi_id) REFERENCES unified_pois(id),
    FOREIGN KEY (jis_code) REFERENCES administrative_regions(jis_code),
    INDEX idx_jis_code (jis_code)
) COMMENT 'POIと行政区画の関連付け';

CREATE TABLE poi_hierarchies (
    parent_id INT NOT NULL COMMENT '親POI ID',
    child_id INT NOT NULL COMMENT '子POI ID',
    relation_type ENUM('MEMBER', 'SUB_RANGE') DEFAULT 'MEMBER' COMMENT '関係性',
    PRIMARY KEY (parent_id, child_id),
    FOREIGN KEY (parent_id) REFERENCES unified_pois(id),
    FOREIGN KEY (child_id) REFERENCES unified_pois(id)
) COMMENT 'POIの親子関係テーブル';

CREATE TABLE poi_names (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '名称ID',
    unified_poi_id INT NOT NULL COMMENT '統合実体ID',
    source_uuid BINARY(16) NOT NULL COMMENT '情報源UUID',
    name_text VARCHAR(255) COLLATE utf8mb4_bin NOT NULL COMMENT '表示用名称(異体字保持)',
    name_normalized VARCHAR(255) NOT NULL COMMENT '検索用正規化名称',
    name_reading VARCHAR(255) COMMENT '読み仮名',
    name_type ENUM('MAIN', 'ALIAS', 'OLD', 'LOCAL') DEFAULT 'MAIN' COMMENT '名称種別',
    is_preferred BOOLEAN DEFAULT FALSE COMMENT '代表名称フラグ',
    FOREIGN KEY (unified_poi_id) REFERENCES unified_pois(id),
    INDEX idx_source_uuid (source_uuid),
    INDEX idx_name_normalized (name_normalized)
) COMMENT 'POI名称テーブル';
