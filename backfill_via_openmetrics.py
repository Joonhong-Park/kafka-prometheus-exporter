"""과거 error count 데이터를 Parquet에서 읽어 OpenMetrics 텍스트 파일로 만드는 백필 스크립트.

Prometheus 2.26.0은 out-of-order 샘플 수신 기능(2.39부터 추가)이 없어, Remote Write
(backfill_to_prometheus.py)로는 과거 시각 데이터를 받아주지 않고 "out of bound"로 거부한다.
이 버전에서도 되는 방법은 promtool의 오프라인 TSDB 블록 생성 기능뿐이며, 수신 시점 검사를
거치지 않아 버전 제약이 없다.

이 스크립트는 OpenMetrics 파일 생성까지만 담당한다. 이후 아래 절차는 스크립트 밖에서 진행한다.

    promtool tsdb create-blocks-from openmetrics --output-dir=<임시경로> backfill.om
    (생성된 블록 디렉터리를 Prometheus의 --storage.tsdb.path 밑으로 이동)
    (Prometheus 재시작 또는 reload로 새 블록 인식)

상시 운영되는 Consumer+Exporter(worker_error_exporter.py), Remote Write 방식
백필 스크립트(backfill_to_prometheus.py)와는 별개의 독립 실행 스크립트다.
"""

import logging
from datetime import timedelta, timezone

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))

# --- 설정값 (Placeholder - 실제 값 확정되면 교체) ---
PARQUET_FILE_PATH = "PARQUET_FILE_PATH_PLACEHOLDER.parquet"  # TODO: 실제 로컬 경로로 교체
OUTPUT_FILE_PATH = "backfill.om"  # promtool에 넘길 OpenMetrics 출력 파일

# 상시 Exporter가 스크래핑될 때 갖는 라벨셋과 동일하게 맞춰야 Grafana에서 같은 시리즈로 조회됨
METRIC_NAME_ERROR_COUNT = "error_count"
METRIC_NAME_LAST_SEEN = "node_last_seen_timestamp"
JOB_NAME = "impalahdfserror-exporter"
INSTANCE_LABEL = "localhost:9200"  # prometheus.yml scrape_configs의 targets 값과 동일해야 함


def load_records() -> pd.DataFrame:
    """Parquet 파일을 읽어 컬럼을 위치 인덱스(0, 1, 2) 기준으로 hostname/timestamp/count에 매핑한다.

    backfill_to_prometheus.py의 1단계와 동일한 로직 (별개의 일회성 스크립트로 유지하기 위해
    모듈 분리 없이 그대로 복제).
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

    df["event_time"] = pd.to_datetime(df["timestamp_raw"], errors="coerce")
    # Parquet의 timestamp 컬럼이 이미 datetime64로 저장되어 있으면 tz 정보 없이 tz-naive로
    # 남는다 (실측: dtype datetime64[ns], 값 예시 2026-06-01 03:02:01). 원본이 KST라는 전제로
    # 로컬라이즈한다.
    if df["event_time"].dt.tz is None:
        df["event_time"] = df["event_time"].dt.tz_localize(KST)
    df["count"] = pd.to_numeric(df["count_raw"], errors="coerce")

    valid_mask = df["hostname"].notna() & df["event_time"].notna() & df["count"].notna()
    valid_df = df.loc[valid_mask, ["hostname", "event_time", "count"]].reset_index(drop=True)

    skipped_rows = total_rows - len(valid_df)
    logger.info("전체 %d건 중 %d건 파싱 성공, %d건 스킵", total_rows, len(valid_df), skipped_rows)
    logger.info("샘플 레코드:\n%s", valid_df.head(5).to_string(index=False))

    return valid_df


def _escape_label_value(value: str) -> str:
    """OpenMetrics 라벨 값 이스케이프 (백슬래시, 큰따옴표, 줄바꿈 순서로 처리)."""
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def write_openmetrics_file(df: pd.DataFrame) -> None:
    """정제된 레코드를 OpenMetrics 텍스트 포맷으로 OUTPUT_FILE_PATH에 쓴다.

    같은 시리즈(hostname 조합) 내 샘플은 promtool 요구사항에 맞춰 timestamp 오름차순으로
    정렬해서 쓴다. 메트릭 패밀리(error_count, node_last_seen_timestamp)별로 묶어서 쓰고
    마지막에 '# EOF'로 마무리한다 (OpenMetrics 포맷 필수 사항).
    """
    common_labels = f'instance="{_escape_label_value(INSTANCE_LABEL)}",job="{_escape_label_value(JOB_NAME)}"'

    # hostname별로 정렬/그룹핑한 (hostname, epoch_seconds 목록, count 목록)을 한 번만 계산해서
    # error_count/node_last_seen_timestamp 두 메트릭 블록에서 재사용한다.
    per_host_samples: list[tuple[str, list[float], list[float]]] = []
    for hostname, group in df.sort_values("event_time").groupby("hostname"):
        epoch_seconds = (
            (group["event_time"] - pd.Timestamp("1970-01-01", tz="UTC")) / pd.Timedelta(seconds=1)
        ).tolist()
        per_host_samples.append((_escape_label_value(str(hostname)), epoch_seconds, group["count"].tolist()))

    with open(OUTPUT_FILE_PATH, "w", encoding="utf-8") as f:
        f.write(f"# TYPE {METRIC_NAME_ERROR_COUNT} gauge\n")
        for hostname_label, epoch_seconds, counts in per_host_samples:
            for count, ts in zip(counts, epoch_seconds):
                f.write(f'{METRIC_NAME_ERROR_COUNT}{{hostname="{hostname_label}",{common_labels}}} {count} {ts:.3f}\n')

        f.write(f"# TYPE {METRIC_NAME_LAST_SEEN} gauge\n")
        for hostname_label, epoch_seconds, _ in per_host_samples:
            for ts in epoch_seconds:
                f.write(f'{METRIC_NAME_LAST_SEEN}{{hostname="{hostname_label}",{common_labels}}} {ts} {ts:.3f}\n')

        f.write("# EOF\n")

    total_samples = sum(len(epoch_seconds) for _, epoch_seconds, _ in per_host_samples) * 2
    logger.info(
        "OpenMetrics 파일 작성 완료: %s (호스트 %d개, 샘플 %d개)",
        OUTPUT_FILE_PATH, len(per_host_samples), total_samples,
    )


if __name__ == "__main__":
    records_df = load_records()
    write_openmetrics_file(records_df)
    logger.info(
        "다음 명령으로 TSDB 블록을 생성하세요: "
        "promtool tsdb create-blocks-from openmetrics --output-dir=<임시경로> %s "
        "-- 생성된 블록 디렉터리를 Prometheus의 --storage.tsdb.path 밑으로 옮긴 뒤 재시작하세요.",
        OUTPUT_FILE_PATH,
    )
