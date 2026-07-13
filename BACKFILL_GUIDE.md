# 과거 데이터 백필 가이드 (Prometheus 2.26.0 기준)

`claude-backfill.md`에 정의된 백필 스크립트를 실제 운영 환경(Prometheus 2.26.0)에 적용하면서
겪은 문제와 해결 과정을 정리한 실행 가이드다. 처음 이 작업을 하는 사람도 순서대로 따라 할 수
있도록 배경 설명과 명령어를 함께 남긴다.

## 배경: 왜 Remote Write가 아니라 promtool을 쓰는가

`backfill_to_prometheus.py`는 Prometheus Remote Write API로 과거 데이터를 전송하는
스크립트였다. 하지만 **Prometheus 2.26.0에는 out-of-order 샘플 수신 기능이 없다**
(이 기능은 2.39부터 추가됨). 그래서 Remote Write로 과거 시각 데이터를 보내면 항상
`400 Bad Request - out of bound`로 거부당한다.

대신 `backfill_via_openmetrics.py` + `promtool tsdb create-blocks-from`을 쓴다. 이 방식은
Prometheus의 HTTP 수신 경로(ingestion)를 거치지 않고, **TSDB 블록 파일을 오프라인으로 직접
생성**해서 데이터 디렉터리에 끼워 넣는 방식이라 버전 제약이 없다.

`backfill_to_prometheus.py`는 나중에 Prometheus를 2.39+로 업그레이드하면 유효해지므로
코드는 남겨두되, **지금 버전(2.26.0)에서는 `backfill_via_openmetrics.py`를 쓴다.**

## 사전 확인 사항

```bash
# Prometheus 버전 확인
/opt/prometheus/prometheus --version

# promtool 위치 확인
ls /opt/prometheus/promtool
```

- `storage.tsdb.retention.time`(현재 45d)은 백필 대상 데이터가 45일 이내면 별도로 늘릴 필요 없음.
  45일보다 오래된 과거 데이터를 백필하려면 이 값도 그만큼 늘려야 한다(전역 설정이라 디스크
  사용량에 영향을 준다는 점 주의).

## 1단계: Prometheus systemd 플래그 추가

```bash
systemctl cat prometheus   # 현재 ExecStart 확인
sudo systemctl edit --full prometheus
```

`ExecStart`에 아래 두 플래그를 추가한다 (기존 옵션은 그대로 두고 끝에 추가).

```ini
ExecStart=/opt/prometheus/prometheus \
  --config.file=/etc/prometheus/prometheus.yml \
  --storage.tsdb.path=/data/06/prometheus/data \
  --storage.tsdb.retention.time=45d \
  --storage.tsdb.allow-overlapping-blocks \
  --web.enable-admin-api
```

- `--storage.tsdb.allow-overlapping-blocks`: 백필한 블록의 시간 범위가 기존 라이브 데이터
  블록과 겹칠 때 Prometheus가 시작을 거부하지 않도록 함 (겹치는 블록은 이후 백그라운드
  compaction이 자동으로 병합해준다).
- `--web.enable-admin-api`: 잘못 들어간 데이터를 나중에 지울 수 있게 Admin API(삭제 기능)를 켬.

```bash
sudo systemctl daemon-reload
sudo systemctl restart prometheus

# 반영 확인
ps aux | grep prometheus | grep -E "allow-overlapping-blocks|enable-admin-api"
```

## 2단계: Parquet 데이터의 타임존 확인 (중요)

실제 데이터로 확인한 결과, Parquet의 `@timestamp` 컬럼은 다음과 같은 특성을 가진다.

```python
import pandas as pd
raw_df = pd.read_parquet("실제_경로.parquet", engine="pyarrow")
print(raw_df.dtypes)          # datetime64[ns]  <- tz 정보 없음(tz-naive)
print(raw_df.iloc[:5, 1])     # 예: 2026-06-01 03:02:01
```

> **주의**: 이 tz-naive 값은 KST가 아니라 **이미 UTC 벽시계 시각**이다. 처음에는 KST로
> 가정하고 `tz_localize(KST)`를 했다가, 백필된 모든 데이터가 실제보다 9시간 과거로 저장되는
> 문제가 있었다. `backfill_via_openmetrics.py`는 현재 `tz_localize(timezone.utc)`로 수정되어
> 있으니, 만약 데이터 소스가 바뀌어 이 전제가 다시 깨지면 같은 문제가 재발할 수 있다.

## 3단계: 백필 스크립트 설정값 확인/교체

`backfill_via_openmetrics.py` 상단의 설정값을 확인한다.

```python
PARQUET_FILE_PATH = "..."          # 실제 Parquet 경로로 교체
OUTPUT_FILE_PATH = "backfill.om"   # 그대로 사용
METRIC_NAME_ERROR_COUNT = "error_count"
METRIC_NAME_LAST_SEEN = "node_last_seen_timestamp"
JOB_NAME = "impalahdfserror-exporter"       # prometheus.yml의 job_name과 동일해야 함
INSTANCE_LABEL = "localhost:9200"           # prometheus.yml의 targets 값과 동일해야 함
LIVE_DATA_START_KST = "..."                 # 아래 설명 참고
```

### `LIVE_DATA_START_KST` 값을 구하는 방법 (중요, 흔한 실수)

이 값은 "라이브 Exporter가 실제로 데이터를 기록하기 시작한 시각"이다. 이 시각 이후 데이터는
백필 대상에서 자동으로 제외되어, 라이브 데이터와 겹치지 않게 한다.

**틀린 방법**: `systemctl show worker-error-exporter -p ActiveEnterTimestamp`
→ 이 값은 서비스가 **가장 최근에 재시작된 시각**일 뿐이다. 코드를 수정할 때마다 서비스를
재시작하므로 이 값은 계속 갱신되고, 실제 라이브 데이터가 최초로 기록된 시각과는 무관하다.
이 값을 잘못 쓰면, 실제로는 이미 라이브 데이터가 존재하는 구간까지 백필 대상에 포함시켜서
데이터가 중복/오염될 수 있다.

**올바른 방법**: Grafana 차트에서 라이브 시계열이 **최초로 값을 갖기 시작한 시각**을 직접
확인해서 그 값을 쓴다.

## 4단계: OpenMetrics 파일 생성

```bash
python3 backfill_via_openmetrics.py
```

실행 로그에서 아래 항목을 확인한다.

```
전체 N건 중 M건 파싱 성공, S건 스킵, 라이브 구간(>= ...) 겹침으로 X건 제외
```

- `M`(파싱 성공)이 예상한 전체 건수와 비슷한지
- `X`(라이브 구간 겹침으로 제외된 건수)가 너무 크면 `LIVE_DATA_START_KST`를 너무 이르게
  잡은 것일 수 있으니 재확인

성공하면 실행 디렉터리에 `backfill.om` 파일이 생성된다.

## 5단계: promtool로 TSDB 블록 생성

```bash
/opt/prometheus/promtool tsdb create-blocks-from openmetrics \
  --output-dir=/tmp/backfill-blocks \
  backfill.om
```

- 데이터가 시간당 1건씩이라 생성되는 블록이 매우 많을 수 있다(수백 개). 이 버전의 promtool은
  블록 크기를 조절하는 옵션(`--max-block-duration` 등)이 없어서 통제 불가능하지만, **문제되지
  않는다** — Prometheus가 평소처럼 백그라운드 compaction으로 알아서 점점 큰 블록으로 합쳐준다.

```bash
ls /tmp/backfill-blocks/   # ULID 형태 디렉터리들이 생성되었는지 확인
```

## 6단계: 블록을 Prometheus 데이터 경로로 이동

```bash
ls -la /data/06/prometheus/data/ | head   # 기존 블록 소유권 확인
sudo mv /tmp/backfill-blocks/* /data/06/prometheus/data/
sudo chown -R <prometheus_user>:<prometheus_group> /data/06/prometheus/data/01H*   # 필요 시
```

## 7단계: Prometheus 재시작

```bash
sudo systemctl restart prometheus
journalctl -u prometheus -f   # "Server is ready to receive web requests." 확인
```

재시작하면 데이터 디렉터리를 다시 스캔하면서 새 블록을 바로 인식한다. 별도 대기나 reload는
필요 없다. 다만 블록 개수가 많으면 기동 시간이 평소보다 조금(수 초~1분) 더 걸릴 수 있다.

**에러가 나는 경우**: `err opening storage failed, invalid block sequence, time ranges overlap`
→ 1단계에서 `--storage.tsdb.allow-overlapping-blocks`를 빼먹은 것. 추가 후 재시도.

## 8단계: 검증

Prometheus에 직접 쿼리해서 확인 (Grafana를 거치지 않아 캐싱 문제를 배제할 수 있음).

```bash
curl -g 'http://localhost:9090/api/v1/query_range' \
  --data-urlencode 'query=error_count{job="impalahdfserror-exporter"}' \
  --data-urlencode 'start=2026-06-01T00:00:00+09:00' \
  --data-urlencode 'end=2026-06-02T00:00:00+09:00' \
  --data-urlencode 'step=3600s'
```

Grafana에서도 과거 날짜 범위로 확인한다. 쿼리 작성 시 주의사항은 아래 "Grafana 쿼리 작성 시
주의사항" 섹션 참고.

---

## Grafana 쿼리 작성 시 주의사항

### 1. Table 패널에서 과거 데이터가 안 보임

Table 패널은 기본적으로 **Instant** 쿼리(선택한 시간 범위의 끝 시점 하나만 조회)를 쓴다.
백필 데이터는 시간당 1건뿐이라, Prometheus 기본 5분 lookback 범위를 벗어난 임의 시점을
조회하면 "값 없음"이 된다. 쿼리 옵션에서 `Instant`를 끄고 `Range`로 바꾸면 Time series와
동일하게 전체 구간의 샘플을 가져온다.

### 2. 시간에 따라 그래프에서 데이터가 사라짐 (Min interval 문제)

시간당 1건(매시 2~3분)만 있는 데이터에 `Min interval`을 10분 이상으로 주면, 쿼리 격자가
샘플 시각(2~3분)을 아예 지나쳐서 매시간 데이터를 전부 놓친다 (Prometheus의 기본 5분
lookback 때문에 격자가 "샘플 시각 이후 5분 이내"에 들어와야만 값을 잡아온다).

**해결**: `Min interval`을 조정하는 대신 쿼리 자체를 `last_over_time`으로 감싼다.

```promql
last_over_time(error_count{job="impalahdfserror-exporter"}[1h])
last_over_time(node_last_seen_timestamp{job="impalahdfserror-exporter"}[1h])
```

`last_over_time(metric[1h])`은 각 격자 시점 기준 과거 1시간 이내 최신 샘플을 찾아오므로,
Min interval 값과 무관하게 항상 안정적으로 표시된다.

집계 함수(`sum`, `avg` 등)와 함께 쓸 때는 **`last_over_time`을 안쪽에, 집계 함수를 바깥쪽에**
둔다.

```promql
# 클러스터별 합계 예시 (hostname 접두사로 cluster 라벨 생성 후 집계)
sum by (cluster) (
  label_replace(
    last_over_time(error_count[1h]),
    "cluster", "$1", "hostname", "^(cluster[234])[0-9]*\\..*"
  )
)
```

### 3. 그래프 x축이 항상 정시(00분)에만 찍힘

Grafana는 쿼리 격자를 안정적으로 유지하기 위해 조회 시작 시각을 step 크기 단위로
epoch(1970-01-01 UTC) 기준 내림 처리한다. KST는 UTC와 정확히 1시간 단위 차이라서, step이
1시간이면 격자는 항상 KST 기준으로도 정시(00분)에만 생긴다 — 이건 구조적 특성이라 설정으로
바꿀 수 없다. `last_over_time`을 쓰고 있다면 값 자체는 이미 정확하므로(2~3분에 찍힌 값이
다음 정시 격자에 정확히 반영됨), x축 눈금이 00분인 것은 표시상의 문제일 뿐 실제 데이터
정합성과는 무관하다.

---

## 잘못 백필했을 때 되돌리는 방법

블록이 이미 compaction으로 병합된 뒤에는 파일 단위로 골라 지울 수 없다. Prometheus의
**Admin API 삭제 기능(tombstone 기반)**을 쓴다 — 블록 경계와 무관하게 저장된 데이터 위에서
동작해서, 합쳐진 뒤에도 사용 가능하다 (1단계에서 `--web.enable-admin-api`를 미리 켜둬야 함).

```bash
# 1. 삭제 (start/end는 Grafana에 표시되는 KST 값을 +09:00 오프셋으로 그대로 사용 가능)
curl -g -X POST -i 'http://localhost:9090/api/v1/admin/tsdb/delete_series' \
  --data-urlencode 'match[]={job="impalahdfserror-exporter"}' \
  --data-urlencode 'start=2026-06-01T02:55:00+09:00' \
  --data-urlencode 'end=2026-07-07T07:10:00+09:00'
# 응답이 204 No Content 인지 확인

# 2. 실제로 디스크에서 지워지도록 정리
curl -X POST http://localhost:9090/api/v1/admin/tsdb/clean_tombstones

# 3. Prometheus에 직접 재확인 (Grafana 캐싱과 무관하게 실제 삭제 여부 확인)
curl -g 'http://localhost:9090/api/v1/query_range' \
  --data-urlencode 'query=error_count{job="impalahdfserror-exporter"}' \
  --data-urlencode 'start=2026-06-01T00:00:00+09:00' \
  --data-urlencode 'end=2026-06-02T00:00:00+09:00' \
  --data-urlencode 'step=3600s'
# result: [] 이면 삭제 성공. Grafana에는 여전히 보이면 브라우저 강력 새로고침(Ctrl+Shift+R)
```

삭제 확인 후 3~7단계를 다시 실행해 올바른 데이터로 재백필한다.

---

## 트러블슈팅 요약

| 증상 | 원인 | 해결 |
|---|---|---|
| `400 Bad Request - out of bound` (Remote Write) | Prometheus 2.26.0에 out-of-order 수신 기능 없음 | `backfill_to_prometheus.py` 대신 `backfill_via_openmetrics.py` + promtool 사용 |
| `admin APIs disabled` | `--web.enable-admin-api` 미반영 | systemd unit에 플래그 추가 후 재시작 |
| `invalid block sequence, time ranges overlap` | `--storage.tsdb.allow-overlapping-blocks` 미반영 | systemd unit에 플래그 추가 후 재시작 |
| `cannot compare tz-naive and tz-aware datetime-like objects` | Parquet timestamp가 tz-naive인데 tz-aware 값과 연산 | `tz_localize()`로 명시적 타임존 부여 후 연산 |
| `name 'KST' is not defined` | 코드에 KST 상수 정의/import 누락 (구버전 파일) | 최신 스크립트로 교체 |
| 백필 데이터가 9시간 과거로 찍힘 | tz-naive 값을 KST로 잘못 가정 (실제로는 UTC) | `tz_localize(timezone.utc)`로 수정 |
| Table 패널에서 과거 데이터 안 보임 | Table 패널의 `Instant` 쿼리 모드 | `Instant` 끄고 `Range`로 변경 |
| Min interval 10분 이상에서 그래프 사라짐 | 쿼리 격자가 5분 lookback 밖으로 벗어남 | `last_over_time(metric[1h])`로 쿼리 변경 |
| 삭제 후에도 Grafana에 계속 보임 | Grafana 브라우저/렌더링 캐시 | 강력 새로고침, Prometheus에 직접 재확인 |
