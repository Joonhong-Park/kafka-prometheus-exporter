"""과거 error count 데이터를 Parquet에서 읽어 Prometheus로 백필하는 일회성 스크립트.

claude-backfill.md의 2단계 개발 절차에 따라 단계별로 코드를 추가한다.
현재는 1단계(로컬 Parquet 파일 읽기)까지 구현되어 있다.
상시 운영되는 Consumer+Exporter(worker_error_exporter.py)와는 별개의 프로젝트다.
"""

import logging

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- 설정값 (Placeholder - 실제 값 확정되면 교체) ---
PARQUET_FILE_PATH = "PARQUET_FILE_PATH_PLACEHOLDER.parquet"  # TODO: 실제 로컬 경로로 교체


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
    df["count"] = pd.to_numeric(df["count_raw"], errors="coerce")

    # hostname 누락 또는 timestamp/count 파싱 실패 행은 백필 대상에서 제외
    valid_mask = df["hostname"].notna() & df["event_time"].notna() & df["count"].notna()
    valid_df = df.loc[valid_mask, ["hostname", "event_time", "count"]].reset_index(drop=True)

    skipped_rows = total_rows - len(valid_df)
    logger.info("전체 %d건 중 %d건 파싱 성공, %d건 스킵", total_rows, len(valid_df), skipped_rows)
    logger.info("샘플 레코드:\n%s", valid_df.head(5).to_string(index=False))

    return valid_df


if __name__ == "__main__":
    load_records()
