# Kafka 워커노드 에러 카운트 Consumer + Prometheus Exporter

Kafka topic(`topic-name-1`)에 워커노드(~510개, cluster2/3/4 각 ~170개)가 1시간마다(매시 정각+2분) 발행하는
에러 카운트 메시지를 상시 컨슈밍하여, Prometheus가 스크래핑할 수 있는 `/metrics` 엔드포인트로 노출하는
단일 상시 프로세스다. 상세 설계/제약 조건은 `CLAUDE.md` 참고.

## 요구사항

- Python 3
- `confluent-kafka` (librdkafka 시스템 라이브러리가 배포 서버에 설치되어 있어야 함)
- `prometheus_client`

```bash
pip install confluent-kafka prometheus_client
```

## 설정값 (코드 상단 상수, 실제 배치 전 확인/교체)

`worker_error_exporter.py` 상단에 정의되어 있다.

| 상수 | 현재 값 | 비고 |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `KAFKA_BROKER_N_PLACEHOLDER:9092` (콤마 구분) | 브로커 10대(각 9092 포트) 중 일부만 나열해도 무방 - **실제 주소로 교체 필수** |
| `KAFKA_TOPIC` | `topic-name-1` | |
| `EXPORTER_PORT` | `9200` | 확정값 (동일 서버의 Grafana 3000/node_exporter 3100/Prometheus 9090과 비충돌 확인됨) |
| `LOG_FILE_PATH` | `/var/log/worker-error-exporter/worker_error_exporter.log` | 실제 배치 서버 로그 경로로 교체 가능 |

## 직접 실행 (개발/테스트)

```bash
python3 worker_error_exporter.py
```

- 실행하면 `/metrics` HTTP 서버가 `EXPORTER_PORT`(기본 9200)에서 기동되고, 동시에 메인 스레드에서 Kafka Consumer가 시작된다.
- 종료는 `Ctrl+C` (KeyboardInterrupt를 잡아 Consumer를 정리하고 종료).
- `LOG_FILE_PATH` 디렉터리에 쓰기 권한이 있어야 한다(기본값은 `/var/log/...`라 로컬 테스트 시에는 코드 상단의 경로를 쓰기 가능한 경로로 임시로 바꿔서 실행 권장).

## 동작 확인

```bash
# /metrics 엔드포인트 확인
curl http://localhost:9200/metrics | grep worker_

# 로그 확인 (systemd로 실행 중이 아닐 때)
tail -f /var/log/worker-error-exporter/worker_error_exporter.log
```

- 정상 수신 시 `error_count{hostname="..."}`, `node_last_seen_timestamp{hostname="..."}` 두 Gauge가 갱신된다.
- 로그는 매일 자정 로테이션되며 최근 1주일치만 보관된다(`backupCount=6`).

## systemd 서비스로 등록 (상시 데몬 운영)

`worker-error-exporter.service`에 정의되어 있다. 등록 전 아래 placeholder를 실제 값으로 교체해야 한다.

- `User` / `Group`: 실행 계정
- `WorkingDirectory`, `ExecStart`의 배치 경로
- (필요 시) `ExecStart`의 python 인터프리터 경로 (venv 사용 시)

```bash
sudo cp worker-error-exporter.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now worker-error-exporter

# 상태/로그 확인
sudo systemctl status worker-error-exporter
journalctl -u worker-error-exporter -f
```

- `LogsDirectory=worker-error-exporter` 덕분에 systemd가 서비스 시작 전 `/var/log/worker-error-exporter`를
  `User`/`Group` 소유로 자동 생성해준다.
- `Restart=on-failure`로 등록되어 있어 프로세스가 죽으면 자동 재시작된다. 재시작 시 상태는 in-memory라
  초기화되지만, Consumer의 `group.id`가 매 실행마다 신규 UUID로 생성되고 커밋을 하지 않으므로 항상
  `auto.offset.reset=latest`부터 재구독된다(결측 허용, 기존 합의사항).

## Grafana 연동 (참고)

- Exporter는 `hostname` 라벨만 노출하며 cluster 구분 로직을 포함하지 않는다.
- cluster2/3/4 필터링/집계는 Grafana PromQL에서 정규식으로 처리한다: `hostname=~"^cluster2.*"` 등.
- 미수신 노드(stale) 판단은 `node_last_seen_timestamp`와 현재 시각의 차이를 4200초(70분, 발행 주기
  1시간 + 10분 버퍼) 기준으로 Grafana 쿼리에서 계산한다.
- 메트릭 이름(`error_count`, `node_last_seen_timestamp`)은 과거 데이터를 채워 넣는 별도 백필
  스크립트(`backfill_to_prometheus.py`)와 동일하게 맞춰져 있어, Grafana에서 같은 시리즈로 조회된다.

## 이 프로젝트 범위 밖

- Prometheus/Grafana 자체의 설치·구성 (대상 서버에 이미 서비스로 떠 있음)
- `prometheus.yml`의 `scrape_configs`에 이 Exporter를 타겟으로 등록하는 작업
