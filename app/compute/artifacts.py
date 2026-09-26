from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path

from app.core.errors import ValidationError

_CHUNK_SIZE = 1024 * 1024


class RelativePathError(ValidationError):
    pass


@dataclass(frozen=True, slots=True)
class VerifiedFile:
    absolute_path: Path
    size_bytes: int
    sha256: str


def normalize_relative_path(relative_path: str) -> str:
    """规范化工作者提交的相对路径，拒绝绝对路径、盘符与目录穿越。"""
    if not isinstance(relative_path, str) or not relative_path.strip():
        raise RelativePathError("制品相对路径不能为空")
    raw = relative_path.replace("\\", "/").strip()
    candidate = Path(raw)
    if candidate.is_absolute() or candidate.drive or candidate.root:
        raise RelativePathError(f"制品路径必须是相对路径：{relative_path}")
    parts = [part for part in candidate.parts if part not in ("", "/")]
    if not parts or any(part in (".", "..") for part in parts):
        raise RelativePathError(f"制品路径不合法：{relative_path}")
    normalized = "/".join(parts)
    if len(normalized) > 4096:
        raise RelativePathError("制品相对路径过长")
    return normalized


class ArtifactStore:
    """基于本地文件系统的结果制品仓库，只负责只读校验与解析。

    物理文件按任务命名空间隔离（``task-<id>/<工作者提交的相对路径>``），
    元数据中保存的仍是工作者提交的相对路径，避免不同任务互相覆盖。
    """

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def base_for_task(self, task_id: int) -> Path:
        return self.root.resolve() / f"task-{int(task_id)}"

    def resolve(self, relative_path: str, task_id: int | None = None) -> Path:
        normalized = normalize_relative_path(relative_path)
        base = self.root.resolve() if task_id is None else self.base_for_task(int(task_id))
        target = (base / normalized).resolve()
        if base not in target.parents and target != base:
            raise RelativePathError(f"制品路径越界：{relative_path}")
        return target

    def verify(
        self,
        relative_path: str,
        *,
        expected_size: int,
        expected_sha256: str | None = None,
        task_id: int | None = None,
    ) -> VerifiedFile:
        normalized = normalize_relative_path(relative_path)
        target = self.resolve(normalized, task_id)
        if not target.exists():
            raise ValidationError(f"制品文件不存在：{normalized}")
        if target.is_symlink():
            raise ValidationError(f"制品不能是符号链接：{normalized}")
        if not target.is_file():
            raise ValidationError(f"制品路径不是普通文件：{normalized}")
        actual_size = target.stat().st_size
        if expected_size is not None and actual_size != int(expected_size):
            raise ValidationError(
                f"制品大小与清单不符：{normalized}",
                context={"declared": int(expected_size), "actual": actual_size},
            )
        actual_sha = self._sha256(target)
        if expected_sha256 and actual_sha != expected_sha256.lower():
            raise ValidationError(
                f"制品摘要与清单不符：{normalized}",
                context={"declared": expected_sha256.lower(), "actual": actual_sha},
            )
        return VerifiedFile(absolute_path=target, size_bytes=actual_size, sha256=actual_sha)

    @staticmethod
    def _sha256(path: Path) -> str:
        hasher = hashlib.sha256()
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            with os.fdopen(fd, "rb") as handle:
                while chunk := handle.read(_CHUNK_SIZE):
                    hasher.update(chunk)
        except BaseException:
            try:
                os.close(fd)
            except OSError:
                pass
            raise
        return hasher.hexdigest()
