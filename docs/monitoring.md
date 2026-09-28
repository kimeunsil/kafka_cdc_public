# 모니터링

모든 컴포넌트(ZooKeeper, Kafka, Connect)는 `JMXPORT=9010` 으로 JMX를 노출합니다.
운영 당시에는 New Relic 을 사용했습니다.

| 대상 | 수집 방법 |
|---|---|
| 노드/파드 리소스, k8s 이벤트 | New Relic Kubernetes integration (+ kube-state-metrics) |
| 브로커 / 토픽 / 컨슈머 lag | New Relic `nri-kafka` (JMX 9010 수집) |
| 커넥터·태스크 상태 | registrar sidecar (FAILED 태스크 재시작 + Slack 알림), `python -m registrar status` |

New Relic 은 공식 Helm 차트로 설치하고 라이선스 키는 Secret 으로만 주입합니다.
키를 매니페스트·Dockerfile 에 직접 쓰지 마세요.

```bash
helm repo add newrelic https://helm-charts.newrelic.com
helm upgrade --install newrelic-bundle newrelic/nri-bundle \
  --namespace newrelic --create-namespace \
  --set global.licenseKey="$NEW_RELIC_LICENSE_KEY" \
  --set global.cluster=cdc-cluster \
  --set kube-state-metrics.enabled=true \
  --set nri-kube-events.enabled=true
```

Prometheus 스택을 쓴다면 JMX exporter 를 sidecar 또는 javaagent 로 붙여 동일 지표를 수집할 수 있습니다.

## 운영하며 보던 핵심 지표
- Connect 태스크 `FAILED` 여부 (registrar 가 자동 재시작하고 Slack 으로 알림)
- Debezium `MilliSecondsBehindSource` : 소스 binlog 대비 지연
- 싱크 컨슈머 그룹 lag : BigQuery 적재 지연
- 브로커 디스크 사용량 (retention 설정과 함께 확인)
