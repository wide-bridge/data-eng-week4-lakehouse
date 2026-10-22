-- Trino CLI에서 실행하는 읽기 전용 검증 쿼리.
-- 사전 조건: iceberg 카탈로그와 lakehouse.orders 테이블 및 memo 컬럼 존재.
-- 2026-10-22 실행 확인: 7행, 합계 192000.0, memo vip 1행, 중복 ID 없음.

DESCRIBE iceberg.lakehouse.orders;

SELECT order_id, customer, amount, order_date, channel, memo
FROM iceberg.lakehouse.orders
ORDER BY order_id;

-- 제공된 로그에서 확인된 결과: 4 / choi / vip.
SELECT order_id, customer, memo
FROM iceberg.lakehouse.orders
WHERE memo IS NOT NULL
ORDER BY order_id;

-- 쓰기가 없는 같은 시점의 Spark 집계와 비교한다.
SELECT count(*) AS row_count, sum(amount) AS total_amount
FROM iceberg.lakehouse.orders;

-- 키 중복 여부: 결과가 있으면 해당 ID가 여러 행에 존재한다.
SELECT order_id, count(*) AS occurrences
FROM iceberg.lakehouse.orders
GROUP BY order_id
HAVING count(*) > 1
ORDER BY order_id;
