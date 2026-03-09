# MinIO Bucket Bootstrap

## 기본 동작
- `processor`는 시작 시 `S3_BUCKET`이 없으면 자동으로 생성합니다.
- 구현 위치: `processor/sink.py`의 `ensure_s3_bucket`, 호출 위치: `processor/app.py`.

## 환경변수 체크
- `S3_ENDPOINT`: MinIO 내부 주소 (예: `http://coinstream-minio:9000`)
- `S3_ACCESS_KEY`, `S3_SECRET_KEY`: MinIO 계정
- `S3_BUCKET`: 기본 `crypto`

## Render 점검 순서
1. `processor` 서비스 env가 위 4개와 일치하는지 확인합니다.
2. `processor` 재배포 후 로그에서 `NoSuchBucket` 에러가 없는지 확인합니다.
3. 필요 시 `processor` Shell에서 아래 한 줄로 객체 생성 여부를 확인합니다.

```bash
/usr/local/bin/python -c "import os,boto3;s3=boto3.client('s3',endpoint_url=os.environ['S3_ENDPOINT'],aws_access_key_id=os.environ['S3_ACCESS_KEY'],aws_secret_access_key=os.environ['S3_SECRET_KEY']);r=s3.list_objects_v2(Bucket=os.environ['S3_BUCKET'],Prefix='ohlc_1m/',MaxKeys=10);print('KeyCount=',r.get('KeyCount',0));print([x['Key'] for x in r.get('Contents',[])])"
```
