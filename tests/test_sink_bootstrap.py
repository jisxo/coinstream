from processor.sink import ensure_s3_bucket
from typing import List


class FakeS3Client:
    def __init__(self, buckets: List[str]) -> None:
        self._buckets = buckets[:]
        self.created: List[str] = []

    def list_buckets(self) -> dict:
        return {"Buckets": [{"Name": name} for name in self._buckets]}

    def create_bucket(self, Bucket: str) -> None:
        self.created.append(Bucket)
        self._buckets.append(Bucket)


def test_ensure_s3_bucket_creates_missing_bucket() -> None:
    client = FakeS3Client(["other"])
    ensure_s3_bucket(client, "crypto")
    assert client.created == ["crypto"]


def test_ensure_s3_bucket_skips_existing_bucket() -> None:
    client = FakeS3Client(["crypto"])
    ensure_s3_bucket(client, "crypto")
    assert client.created == []
