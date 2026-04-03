"""文件存储服务。"""

import asyncio
from pathlib import Path

from fastapi import UploadFile

UPLOAD_DIR = Path("upload")
UPLOAD_DIR.mkdir(exist_ok=True)


def _read_file_contents(file) -> bytes:
    """读取文件内容（同步）。"""
    return file.file.read()


def _write_file(file_path: Path, contents: bytes) -> None:
    """写入文件（同步）。"""
    with open(file_path, "wb") as f:
        f.write(contents)


async def save_upload_file(file: UploadFile, task_id: str) -> tuple[str, str]:
    """保存上传文件并返回文件路径和原始文件名。

    Args:
        file: 上传的文件对象。
        task_id: 任务 ID，用于生成新文件名。

    Returns:
        包含文件绝对路径和原始文件名的元组。

    Raises:
        ValueError: 文件大小超过 10MB 限制。
    """
    contents = await asyncio.to_thread(_read_file_contents, file)

    if len(contents) > 10 * 1024 * 1024:
        raise ValueError("文件大小超过 10MB 限制")

    original_name = file.filename or "unknown"
    ext = original_name.rsplit(".", 1)[-1] if "." in original_name else ""
    new_filename = f"{task_id}.{ext}" if ext else task_id

    file_path = UPLOAD_DIR / new_filename
    await asyncio.to_thread(_write_file, file_path, contents)

    return str(file_path.absolute()), original_name
