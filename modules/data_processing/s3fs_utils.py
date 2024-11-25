from s3fs import S3FileSystem
from s3fs.core import _error_wrapper, version_id_kw
from typing import Optional
import asyncio


class S3ParallelFileSystem(S3FileSystem):
    """S3FileSystem subclass that supports parallel downloads"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def _cat_file(
        self,
        path: str,
        version_id: Optional[str] = None,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> bytes:
        bucket, key, vers = self.split_path(path)
        version_kw = version_id_kw(version_id or vers)

        # If start/end specified, use single range request
        if start is not None or end is not None:
            head = {"Range": await self._process_limits(path, start, end)}
            return await self._download_chunk(bucket, key, head, version_kw)

        # For large files, use parallel downloads
        try:
            obj_size = (
                await self._call_s3(
                    "head_object", Bucket=bucket, Key=key, **version_kw, **self.req_kw
                )
            )["ContentLength"]
        except Exception as e:
            # Fall back to single request if HEAD fails
            return await self._download_chunk(bucket, key, {}, version_kw)

        CHUNK_SIZE = 1 * 1024 * 1024  # 1MB chunks
        if obj_size <= CHUNK_SIZE:
            return await self._download_chunk(bucket, key, {}, version_kw)

        # Calculate chunks for parallel download
        chunks = []
        for start in range(0, obj_size, CHUNK_SIZE):
            end = min(start + CHUNK_SIZE - 1, obj_size - 1)
            range_header = f"bytes={start}-{end}"
            chunks.append({"Range": range_header})

        # Download chunks in parallel
        async def download_all_chunks():
            tasks = [
                self._download_chunk(bucket, key, chunk_head, version_kw) for chunk_head in chunks
            ]
            chunks_data = await asyncio.gather(*tasks)
            return b"".join(chunks_data)

        return await _error_wrapper(download_all_chunks, retries=self.retries)

    async def _download_chunk(self, bucket: str, key: str, head: dict, version_kw: dict) -> bytes:
        """Helper function to download a single chunk"""

        async def _call_and_read():
            resp = await self._call_s3(
                "get_object",
                Bucket=bucket,
                Key=key,
                **version_kw,
                **head,
                **self.req_kw,
            )
            try:
                return await resp["Body"].read()
            finally:
                resp["Body"].close()

        return await _error_wrapper(_call_and_read, retries=self.retries)
