"""과거 error count 데이터를 Parquet에서 읽어 Prometheus로 백필하는 일회성 스크립트.

claude-backfill.md의 2단계 개발 절차에 따라 단계별로 코드를 추가한다.
현재는 2단계(Remote Write 포맷 변환 및 전송)까지 구현되어 있다.
상시 운영되는 Consumer+Exporter(worker_error_exporter.py)와는 별개의 프로젝트다.
"""

import logging
import struct
import time
from datetime import timedelta, timezone

import pandas as pd
import requests
import snappy

KST = timezone(timedelta(hours=9))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- 설정값 (Placeholder - 실제 값 확정되면 교체) ---
PARQUET_FILE_PATH = "PARQUET_FILE_PATH_PLACEHOLDER.parquet"  # TODO: 실제 로컬 경로로 교체
PROMETHEUS_REMOTE_WRITE_URL = "http://localhost:9090/api/v1/write"  # TODO: --web.enable-remote-write-receiver 활성화 여부 확인 필요
BATCH_SIZE = 100  # 한 번의 HTTP 요청에 담을 TimeSeries(호스트 x 메트릭) 개수
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2.0

# 상시 Exporter(worker_error_exporter.py)와 동일한 메트릭 이름을 써야 Grafana에서 같은 시리즈로 조회됨
METRIC_NAME_ERROR_COUNT = "error_count"
METRIC_NAME_LAST_SEEN = "node_last_seen_timestamp"

# Prometheus가 상시 Exporter를 스크래핑할 때 자동으로 붙이는 job/instance 라벨과 동일한 값을
# 백필 시에도 넣어야 한다. 라벨셋 전체가 시리즈의 정체성이라, 하나라도 다르면(예: instance 라벨
# 누락) Grafana에서 실시간 데이터와 이어지지 않고 완전히 별개의 시리즈로 취급된다.
JOB_NAME = "impalahdfserror-exporter"
INSTANCE_LABEL = "localhost:9200"  # prometheus.yml scrape_configs의 targets 값과 동일해야 함


def load_records() -> pd.DataFrame:
    """Parquet 파일을 읽어 컬럼을 위치 인덱스(0, 1, 2) 기준으로 hostname/timestamp/count에 매핑한다.

    Parquet은 컬럼명이 아닌 위치로 식별한다 (실제 count 컬럼명이 cnt로 저장되어 있는 등
    이름이 다를 수 있으므로 이름에 의존하지 않는다).
    """
    raw_df = pd.read_parquet(PARQUET_FILE_PATH, engine="pyarrow")
    total_rows = len(raw_df)

    df = pd.DataFrame(
        {
            "hostname": raw_df.iloc[:, 0],
            "timestamp_raw": raw_df.iloc[:, 1],
            "count_raw": raw_df.iloc[:, 2],
        }
    )

    # 벡터화 연산으로 파싱 (iterrows() 사용 금지). errors="coerce"로 개별 행 파싱 실패만
    # NaT/NaN으로 만들고 전체 스크립트는 중단되지 않게 한다.
    df["event_time"] = pd.to_datetime(df["timestamp_raw"], errors="coerce")
    # Parquet의 timestamp 컬럼이 문자열이 아니라 이미 datetime64로 저장되어 있으면 tz 정보 없이
    # tz-naive로 남는다 (실측: dtype datetime64[ns], 값 예시 2026-06-01 03:02:01, 오프셋 없음).
    # TODO: 이 값이 실제로 KST인지 데이터 소스(Impala/HDFS 파이프라인) 쪽에서 확인되지 않음 -
    # 현재는 claude-backfill.md의 "@timestamp 형식: KST" 설명만 근거로 가정. 틀리면 백필되는
    # 모든 시각이 9시간씩 밀리므로, 확인 전까지는 참고용으로만 신뢰할 것.
    if df["event_time"].dt.tz is None:
        df["event_time"] = df["event_time"].dt.tz_localize(KST)
    if df["event_time"].dt.tz is None:
        df["event_time"] = df["event_time"].dt.tz_localize(KST)
    df["count"] = pd.to_numeric(df["count_raw"], errors="coerce")

    # hostname 누락 또는 timestamp/count 파싱 실패 행은 백필 대상에서 제외
    valid_mask = df["hostname"].notna() & df["event_time"].notna() & df["count"].notna()
    valid_df = df.loc[valid_mask, ["hostname", "event_time", "count"]].reset_index(drop=True)

    skipped_rows = total_rows - len(valid_df)
    logger.info("전체 %d건 중 %d건 파싱 성공, %d건 스킵", total_rows, len(valid_df), skipped_rows)
    logger.info("샘플 레코드:\n%s", valid_df.head(5).to_string(index=False))

    return valid_df


# --- Prometheus Remote Write Protobuf 인코딩 ---
# prometheus_client에는 Remote Write용 protobuf 스키마(WriteRequest/TimeSeries/Label/Sample)가
# 포함되어 있지 않고, 별도 생성 코드(protoc) 없이도 스키마가 단순해서 wire format을 직접 인코딩한다.


def _encode_varint(value: int) -> bytes:
    """protobuf varint 인코딩 (길이/필드 번호/timestamp 등 음수 없는 정수 전제)."""
    result = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            result.append(byte | 0x80)
        else:
            result.append(byte)
            break
    return bytes(result)


def _encode_tag(field_number: int, wire_type: int) -> bytes:
    return _encode_varint((field_number << 3) | wire_type)


def _encode_label(name: str, value: str) -> bytes:
    """Label{name: string = 1, value: string = 2}."""
    name_bytes = name.encode("utf-8")
    value_bytes = value.encode("utf-8")
    return (
        _encode_tag(1, 2) + _encode_varint(len(name_bytes)) + name_bytes
        + _encode_tag(2, 2) + _encode_varint(len(value_bytes)) + value_bytes
    )


def _encode_sample(value: float, timestamp_ms: int) -> bytes:
    """Sample{value: double = 1, timestamp: int64(ms) = 2}."""
    return (
        _encode_tag(1, 1) + struct.pack("<d", value)
        + _encode_tag(2, 0) + _encode_varint(timestamp_ms)
    )


def _encode_timeseries(labels: list[tuple[str, str]], samples: list[tuple[float, int]]) -> bytes:
    """TimeSeries{labels: repeated Label = 1, samples: repeated Sample = 2}.

    samples는 호출 전에 timestamp 오름차순으로 정렬되어 있어야 한다 (Prometheus는 같은
    시리즈 내 샘플이 시간 역순으로 들어오면 거부하거나 오동작할 수 있음).
    """
    body = bytearray()
    for name, value in labels:
        encoded_label = _encode_label(name, value)
        body += _encode_tag(1, 2) + _encode_varint(len(encoded_label)) + encoded_label
    for sample_value, timestamp_ms in samples:
        encoded_sample = _encode_sample(sample_value, timestamp_ms)
        body += _encode_tag(2, 2) + _encode_varint(len(encoded_sample)) + encoded_sample
    return bytes(body)


def _encode_write_request(timeseries_list: list[bytes]) -> bytes:
    """WriteRequest{timeseries: repeated TimeSeries = 1}."""
    body = bytearray()
    for encoded_ts in timeseries_list:
        body += _encode_tag(1, 2) + _encode_varint(len(encoded_ts)) + encoded_ts
    return bytes(body)


def build_timeseries_list(df: pd.DataFrame) -> list[bytes]:
    """정제된 레코드를 hostname별로 묶어 error_count/node_last_seen_timestamp 두 메트릭에 대한
    TimeSeries를 만든다. 같은 hostname의 Sample들은 event_time 오름차순으로 정렬해서 하나의
    TimeSeries 안에 담는다 (호스트당 여러 시점의 값을 한 시리즈로 시간순 전송).
    """
    timeseries_list: list[bytes] = []

    for hostname, group in df.sort_values("event_time").groupby("hostname"):
        # tz-aware datetime을 astype(int64)로 직접 변환하면 pandas 버전에 따라 예외가 나므로,
        # Timestamp 뺄셈으로 epoch seconds를 구하는 안전한 방식을 사용한다.
        epoch_seconds = (group["event_time"] - pd.Timestamp("1970-01-01", tz="UTC")) / pd.Timedelta(seconds=1)
        timestamps_ms = (epoch_seconds * 1000).round().astype("int64").tolist()
        counts = group["count"].tolist()

        error_count_samples = list(zip(counts, timestamps_ms))
        # node_last_seen_timestamp 값은 "그 시점에 마지막으로 수신한 시각" 그 자체이므로,
        # 과거 백필 시점에는 이벤트 시각(epoch seconds)을 값으로 그대로 사용한다.
        last_seen_samples = list(zip(epoch_seconds.tolist(), timestamps_ms))

        common_labels = [("hostname", hostname), ("instance", INSTANCE_LABEL), ("job", JOB_NAME)]
        timeseries_list.append(
            _encode_timeseries(
                [("__name__", METRIC_NAME_ERROR_COUNT)] + common_labels,
                error_count_samples,
            )
        )
        timeseries_list.append(
            _encode_timeseries(
                [("__name__", METRIC_NAME_LAST_SEEN)] + common_labels,
                last_seen_samples,
            )
        )

    return timeseries_list


def send_batches(timeseries_list: list[bytes]) -> None:
    """TimeSeries 목록을 BATCH_SIZE 단위로 나눠 Remote Write endpoint로 전송한다.

    배치 하나 = 여러 TimeSeries(호스트 x 메트릭) 묶음이며, 개별 TimeSeries를 배치 중간에
    쪼개지 않으므로 시리즈 내부 시간 순서는 항상 보존된다.
    """
    failed_batches: list[int] = []
    total_batches = (len(timeseries_list) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_index in range(total_batches):
        batch = timeseries_list[batch_index * BATCH_SIZE : (batch_index + 1) * BATCH_SIZE]
        payload = snappy.compress(_encode_write_request(batch))

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.post(
                    PROMETHEUS_REMOTE_WRITE_URL,
                    data=payload,
                    headers={
                        "Content-Encoding": "snappy",
                        "Content-Type": "application/x-protobuf",
                        "X-Prometheus-Remote-Write-Version": "0.1.0",
                    },
                    timeout=30,
                )
                response.raise_for_status()
                logger.info("배치 %d/%d 전송 성공 (series %d개)", batch_index + 1, total_batches, len(batch))
                break
            except requests.RequestException as e:
                # Prometheus는 거부 사유(out-of-order, out-of-bounds 등)를 응답 본문에 텍스트로 담아
                # 주므로, 그냥 raise_for_status()의 요약 메시지만으로는 원인을 알 수 없다.
                error_detail = ""
                if getattr(e, "response", None) is not None:
                    error_detail = f" - 응답 본문: {e.response.text[:500]}"
                logger.warning(
                    "배치 %d/%d 전송 실패 (시도 %d/%d): %s%s",
                    batch_index + 1, total_batches, attempt, MAX_RETRIES, e, error_detail,
                )
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF_SECONDS * attempt)
                else:
                    failed_batches.append(batch_index)

    if failed_batches:
        logger.error(
            "전송 실패한 배치: %s (총 %d개 / %d개 중, 재실행 시 성공했던 배치까지 중복 전송될 수 있음)",
            failed_batches, len(failed_batches), total_batches,
        )
    else:
        logger.info("전체 %d개 배치 전송 완료", total_batches)


if __name__ == "__main__":
    records_df = load_records()
    all_timeseries = build_timeseries_list(records_df)
    logger.info("총 %d개 TimeSeries(호스트 x 메트릭) 생성", len(all_timeseries))
    send_batches(all_timeseries)
