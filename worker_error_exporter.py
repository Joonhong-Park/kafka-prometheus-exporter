"""Kafka 워커노드 에러 카운트 Consumer + Prometheus Exporter.

CLAUDE.md의 3단계 개발 절차에 따라 단계별로 코드를 추가한다.
현재는 3단계(Consumer 스레드 + HTTP 서버 스레드 통합 실행)까지 구현되어 있다.
"""

import json
import logging
import os
import uuid
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from typing import Optional

from confluent_kafka import Consumer, KafkaError
from prometheus_client import Gauge, start_http_server

# --- 로그 설정 (1주일치만 보관) ---
LOG_FILE_PATH = "/var/log/worker-error-exporter/worker_error_exporter.log"  # TODO: 실제 배치 서버 로그 경로로 교체
os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# 매일 자정에 로테이션하고, backupCount=6이라 당일 로그 + 이전 6일치 백업까지 최근 7일치만 보관
_log_handler = TimedRotatingFileHandler(
    LOG_FILE_PATH, when="midnight", interval=1, backupCount=6, encoding="utf-8"
)
_log_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))


def _rotated_log_namer(default_name: str) -> str:
    """기본 로테이션 파일명(worker_error_exporter.log.2026-07-08)을
    worker_error_exporter-2026-07-08.log 형태로 바꿔 날짜별 파일임을 알아보기 쉽게 한다."""
    date_suffix = default_name.rsplit(".", 1)[-1]
    base, ext = os.path.splitext(LOG_FILE_PATH)
    return f"{base}-{date_suffix}{ext}"


_log_handler.namer = _rotated_log_namer
logger.addHandler(_log_handler)

# --- 설정값 (Placeholder - 실제 값 확정되면 교체) ---
# 브로커 10대(각 9092 포트)가 클러스터를 구성 - 목록에 host:port 항목을 추가/삭제해서 교체
# (전부 나열하지 않아도 초기 접속 후 나머지는 자동 discovery 되지만, 일부 브로커 다운 시 초기 접속 실패를 막기 위해 여러 대를 나열 권장)
KAFKA_BROKER_LIST = [
    "KAFKA_BROKER_1_PLACEHOLDER:9092",
    "KAFKA_BROKER_2_PLACEHOLDER:9092",
    "KAFKA_BROKER_3_PLACEHOLDER:9092",
]  # TODO: 실제 브로커 주소로 교체 (10대 중 일부만 나열해도 무방)
KAFKA_BOOTSTRAP_SERVERS = ",".join(KAFKA_BROKER_LIST)  # confluent-kafka는 콤마로 구분된 문자열을 요구
KAFKA_TOPIC = "topic-name-1"
EXPORTER_PORT = 9200  # 확정 (동일 서버의 Grafana 3000/node_exporter 3100/Prometheus 9090과 충돌 없음 확인됨)

# --- Prometheus Gauge 정의 ---
# hostname 라벨만 노출하고 cluster 구분 로직은 포함하지 않는다 (클러스터 필터링은 Grafana PromQL 책임)
worker_error_count = Gauge(
    "worker_error_count", "워커노드별 최신 에러 카운트", ["hostname"]
)
worker_last_seen_timestamp = Gauge(
    "worker_last_seen_timestamp", "워커노드별 마지막 메시지 수신 시각(epoch seconds)", ["hostname"]
)


def run_metrics_server() -> None:
    """Prometheus가 스크래핑할 /metrics HTTP 서버를 기동한다."""
    start_http_server(EXPORTER_PORT)
    logger.info("Prometheus /metrics 서버 시작: port=%d", EXPORTER_PORT)


def run_consumer() -> None:
    """Kafka Consumer를 생성해 topic-name-1을 구독하고, 수신 메시지를 파싱한다."""
    consumer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        # 재시작마다 신규 group.id를 사용해 committed offset이 남아 latest 정책이 무력화되는 것을 방지
        "group.id": f"worker-error-exporter-{uuid.uuid4()}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,  # 커밋을 하지 않아 재시작 시 항상 latest부터 재구독
        "security.protocol": "PLAINTEXT",  # 인증 방식 기본(SASL 미사용)으로 확인됨
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe([KAFKA_TOPIC])
    logger.info("Kafka Consumer 시작: bootstrap=%s, topic=%s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                # 파티션 EOF는 정상 상황이므로 무시하고 계속 polling
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                # 그 외 에러(연결 끊김 등)는 로그만 남기고 프로세스는 유지, 계속 재시도
                logger.error("Kafka 에러 발생: %s", msg.error())
                continue

            raw_value: Optional[bytes] = msg.value()
            if raw_value is None:
                continue

            try:
                payload = json.loads(raw_value.decode("utf-8"))
                hostname: str = payload["hostname"]
                count: int = payload["cnt"]
                timestamp_str: str = payload["@timestamp"]
                # "2026-07-08 15:02:02+09:00" (KST) -> epoch seconds
                last_seen_epoch: float = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S%z").timestamp()
            except (json.JSONDecodeError, KeyError, UnicodeDecodeError, ValueError) as e:
                # 파싱 실패 시 해당 메시지만 스킵하고 전체 프로세스는 계속 진행
                logger.warning("메시지 파싱 실패, 스킵: %s, raw=%s", e, raw_value)
                continue

            # 누적이 아닌 최신값으로 갱신
            worker_error_count.labels(hostname=hostname).set(count)
            worker_last_seen_timestamp.labels(hostname=hostname).set(last_seen_epoch)
            logger.info("메시지 반영: hostname=%s, count=%s, last_seen=%s", hostname, count, last_seen_epoch)

    except KeyboardInterrupt:
        logger.info("Consumer 종료 요청 수신 (KeyboardInterrupt)")
    finally:
        consumer.close()


if __name__ == "__main__":
    # start_http_server()는 내부적으로 daemon 스레드를 띄우고 즉시 반환하므로
    # 별도로 스레드를 감싸지 않고, 메인 스레드는 그대로 run_consumer()를 실행해 동시 동작을 구성한다.
    run_metrics_server()
    run_consumer()
