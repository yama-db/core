<?php
header("Access-Control-Allow-Origin: *");
header("Content-Type: application/geo+json; charset=utf-8");

$target = isset($_GET['db']) ? $_GET['db'] : 'yamap';

function get_db_config() {
    $cnf_path = 'crawler.my.cnf'; # CONFIG: Adjust path as needed
    if (!file_exists($cnf_path)) return null;
    $config = parse_ini_file($cnf_path, true, INI_SCANNER_RAW);
    $client = isset($config['client']) ? $config['client'] : null;
    if (!$client) return null;
    $host = $client['host'] ?? 'localhost';
    $dbname = $client['database'] ?? '';
    $port = $client['port'] ?? 3306;
    return [
        'dsn' => "mysql:host={$host};dbname={$dbname};port={$port};charset=utf8mb4",
        'user' => $client['user'] ?? '',
        'pass' => $client['password'] ?? ''
    ];
}

$db_config = get_db_config();
if (!$db_config) {
    http_response_code(500);
    echo json_encode(["error" => "Configuration file (.my.cnf) not found or invalid"]);
    exit;
}

try {
    $options = [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        PDO::ATTR_EMULATE_PREPARES => false,
    ];

    $pdo = new PDO($db_config['dsn'], $db_config['user'], $db_config['pass'], $options);

    if ($target === 'yamareco') {
        $query = <<<EOS
SELECT raw_remote_id AS id, name, lat, lon, elevation_m
FROM yamareco_pois
WHERE JSON_CONTAINS(poi_type_raw, 1)
  AND lat IS NOT NULL AND lon IS NOT NULL
ORDER BY id DESC
LIMIT 100
EOS;
    } else {
        $query = <<<EOS
SELECT raw_remote_id AS id, name, lat, lon, elevation_m
FROM yamap_pois
WHERE poi_type_raw IN ("19", "999")
  AND lat IS NOT NULL AND lon IS NOT NULL
ORDER BY id DESC
LIMIT 100
EOS;
    }

    $stmt = $pdo->query($query);
    $features = [];

    while ($row = $stmt->fetch()) {
        $features[] = [
            "type" => "Feature",
            "geometry" => [
                "type" => "Point",
                "coordinates" => [(float)$row['lon'], (float)$row['lat']]
            ],
            "properties" => [
                "name" => $row['name'],
                "elevation" => $row['elevation_m'] !== null ? (float)$row['elevation_m'] : null,
                "id" => $row['id']
            ]
        ];
    }

    echo json_encode([
        "type" => "FeatureCollection",
        "features" => $features
    ], JSON_UNESCAPED_UNICODE | JSON_NUMERIC_CHECK);

} catch (PDOException $e) {
    http_response_code(500);
    echo json_encode([
        "type" => "FeatureCollection",
        "features" => [],
        "error" => "Database error: " . $e->getMessage()
    ]);
} finally {
    $pdo = null;
}
?>
