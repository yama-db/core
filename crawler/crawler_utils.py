# 共通ユーティリティ
from datetime import datetime, timedelta

USER_AGENT = "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
FIELDNAMES = [
    "raw_remote_id",
    "name",
    "kana",
    "lon",
    "lat",
    "elevation_m",
    "poi_type_raw",
    "last_updated_at",
]


def setup_database(conn):
    """テーブル作成とWALモードの設定"""
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pois (
            raw_remote_id INTEGER PRIMARY KEY,
            name TEXT,
            kana TEXT,
            lon REAL,
            lat REAL,
            elevation_m REAL,
            poi_type_raw TEXT,
            last_updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pois_queue (
            raw_remote_id INTEGER PRIMARY KEY,
            status INTEGER DEFAULT 0,  -- 0:未調査, 1:生存, 2:欠番(404), 3:200無効, 4:解析失敗, -1:通信エラー
            last_checked TEXT
        )
        """
    )
    conn.commit()


def get_system_mode(conn, max_initial_id=300000):
    """DBの状態から実行モードを判定する"""
    cur = conn.cursor()
    # 未調査の初期IDがあるか確認
    cur.execute(
        "SELECT COUNT(*) FROM pois_queue WHERE status = 0 AND raw_remote_id <= ?",
        (max_initial_id,),
    )
    if cur.fetchone()[0] > 0:
        return "INITIAL_CRAWL"

    # 最終確認から90日以上経過した生存データがあるか
    three_months_ago = (datetime.now() - timedelta(days=90)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    cur.execute(
        "SELECT COUNT(*) FROM pois_queue WHERE status = 1 AND last_checked < ?",
        (three_months_ago,),
    )
    if cur.fetchone()[0] > 100:
        return "MAINTENANCE_UPDATE"

    return "IDLE_WATCH"


def fetch_targets(conn, mode, limit):
    """モードに合わせた調査対象IDのリストを取得"""
    cur = conn.cursor()
    if mode in ["INITIAL_CRAWL", "IDLE_WATCH"]:
        cur.execute(
            """
            SELECT raw_remote_id
            FROM pois_queue
            WHERE status = 0
            ORDER BY raw_remote_id ASC
            LIMIT ?
            """,
            (limit,),
        )
    else:  # MAINTENANCE_UPDATE
        three_months_ago = (datetime.now() - timedelta(days=90)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        cur.execute(
            """
            SELECT raw_remote_id
            FROM pois_queue
            WHERE status = 1 AND last_checked < ?
            ORDER BY last_checked ASC LIMIT ?
            """,
            (three_months_ago, limit),
        )
    return [row[0] for row in cur.fetchall()]


def refill_queue(conn, target_stock=5000, threshold=1000):
    """未調査IDが減ったら補充する"""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM pois_queue WHERE status = 0")
    if cur.fetchone()[0] < threshold:
        cur.execute("SELECT MAX(raw_remote_id) FROM pois_queue")
        max_id = cur.fetchone()[0] or 0
        needed = target_stock - (
            cur.execute("SELECT COUNT(*) FROM pois_queue WHERE status = 0").fetchone()[
                0
            ]
        )
        next_ids = [(i, 0) for i in range(max_id + 1, max_id + needed + 1)]
        cur.executemany(
            "INSERT OR IGNORE INTO pois_queue (raw_remote_id, status) VALUES (?, ?)",
            next_ids,
        )
        conn.commit()
        print(f"[*] Queue refilled. Current Max ID: {max_id + needed}")


def update_queue_status(conn, raw_remote_id, new_status):
    """調査結果に基づきキューテーブルのステータスを更新"""
    cur = conn.cursor()
    cur.execute(
        "UPDATE pois_queue SET status=?, last_checked=CURRENT_TIMESTAMP WHERE raw_remote_id=?",
        (new_status, raw_remote_id),
    )
    conn.commit()


def save_to_database(conn, data):
    """本体テーブルへの保存 (UPSERT)"""
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO pois (raw_remote_id, name, kana, lon, lat, elevation_m, poi_type_raw, last_updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(raw_remote_id) DO UPDATE SET
            name=excluded.name,
            kana=excluded.kana,
            lon=excluded.lon, 
            lat=excluded.lat,
            elevation_m=excluded.elevation_m, 
            poi_type_raw=excluded.poi_type_raw,
            last_updated_at=excluded.last_updated_at
    """,
        tuple(data[i] for i in FIELDNAMES),
    )


def print_progress_summary(conn):
    cur = conn.cursor()
    cur.execute("SELECT status, COUNT(*) as count FROM pois_queue GROUP BY status")
    rows = cur.fetchall()
    stats = {row["status"]: row["count"] for row in rows}

    # 各ステータスの件数（キーがない場合は0）
    total = sum(stats.values())
    done = stats.get(1, 0)  # 生存
    not_found = stats.get(2, 0)  # 404欠番
    empty = stats.get(3, 0)  # 200無効
    parse_err = stats.get(4, 0)  # 解析失敗
    error = stats.get(-1, 0)  # 通信エラー
    pending = stats.get(0, 0)  # 未着手

    if total == 0:
        return

    progress = ((total - pending) / total) * 100

    print("\n" + "=" * 40)
    print(f"📊 クローリング進捗サマリー ({datetime.now().strftime('%Y-%m-%d %H:%M')})")
    print("-" * 40)
    print(f"✅ 完了(生存)   : {done:>8} 件")
    print(f"🚫 欠番(404)    : {not_found:>8} 件")
    print(f"⚠️  無効(Empty)  : {empty:>8} 件")
    print(f"❌ 解析失敗     : {parse_err:>8} 件")
    print(f"🔄 再試行待ち   : {error:>8} 件")
    print(f"⏳ 未調査       : {pending:>8} 件")
    print("-" * 40)
    print(f"📈 進捗率       : {progress:>8.2f} % (全 {total} 件中)")
    print("=" * 40 + "\n")


# __END__
