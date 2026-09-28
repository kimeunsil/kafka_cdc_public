-- BigQuery 싱크는 Debezium 변경 이벤트(envelope)를 append-only 로 적재합니다.
--   before / after / op(c,u,d,r) / ts_ms / source(file, pos, row ...)
-- 변경 이력은 원본 테이블에 그대로 두고, "현재 상태"는 뷰로 제공하는 패턴입니다.
-- 예시: orders 테이블 (PK = id)

CREATE OR REPLACE VIEW `your-gcp-project-id.cdc_stream.orders_latest` AS
SELECT
  after.*,
  ts_ms AS _cdc_ts_ms
FROM (
  SELECT
    after,
    op,
    ts_ms,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(after.id, before.id)
      -- binlog 위치 기준으로 가장 마지막 이벤트 1건
      ORDER BY source.file DESC, source.pos DESC, source.row DESC
    ) AS rn
  FROM `your-gcp-project-id.cdc_stream.orders`
)
WHERE rn = 1
  AND op != 'd';   -- 마지막 이벤트가 삭제면 현재 상태에서 제외
