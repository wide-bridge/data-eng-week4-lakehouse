-- scripts/lakehouse_queries.sql — 4주차 산출물: Trino 쿼리 스크립트
-- 실행: docker compose exec -T trino trino < scripts/lakehouse_queries.sql

-- 1. 연결 확인
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.lakehouse;

-- 2. 데이터 조회
SELECT * FROM iceberg.lakehouse.orders ORDER BY order_id;

-- 3. 집계
SELECT customer, count(*) AS orders, sum(amount) AS total
FROM iceberg.lakehouse.orders
GROUP BY customer ORDER BY total DESC;

SELECT channel,
       sum(amount) AS revenue,
       round(sum(amount) * 100.0 / sum(sum(amount)) OVER (), 1) AS pct
FROM iceberg.lakehouse.orders
GROUP BY channel ORDER BY revenue DESC;

SELECT order_date, count(*) AS orders, sum(amount) AS total_amount FROM iceberg.lakehouse.orders GROUP BY order_date ORDER BY order_date;

-- 4. 스냅샷 이력·현재 데이터 파일 (특정 스냅샷 타임 트래블 쿼리는 아님)
SELECT committed_at, snapshot_id, operation
FROM iceberg.lakehouse."orders$snapshots" ORDER BY committed_at;

SELECT file_path, record_count
FROM iceberg.lakehouse."orders$files";
