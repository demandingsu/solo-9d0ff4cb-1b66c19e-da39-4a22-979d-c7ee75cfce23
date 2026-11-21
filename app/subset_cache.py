"""按交付内容缓存已通过外键校验的 SQLite 子库。

同一次上传（同一 SessionState）内，下载键由解析后四张表的主键集合决定；
源库在一次上传期间不可变，因此主键集合相同即四张表记录完全相同。

并发约定：
- 同一键同时只有一个生成者，其余请求等待并复用结果；
- 不同键的生成在锁外进行，可以并行；
- 生成/校验失败时所有等待者都收到 SubsetBuildError，失败项不留在缓存中，
  之后的请求可以重新生成；
- 最多保留最近生成的两份子库；被淘汰的条目从缓存移除，若仍有下载在途，
  等最后一个使用者释放后再删除文件。
"""
from __future__ import annotations

import logging
import os
import threading
from collections import OrderedDict
from collections.abc import Callable

logger = logging.getLogger("app.subset_cache")

BUILDING = "building"
READY = "ready"
FAILED = "failed"


class SubsetBuildError(RuntimeError):
    """子库生成或外键校验失败。"""


class CacheEntry:
    """一份子库的缓存条目；users 为持有该条目（等待中或下载中）的请求数。"""

    __slots__ = (
        "deleted",
        "error",
        "evicted",
        "fingerprint",
        "path",
        "status",
        "users",
    )

    def __init__(self, fingerprint: str):
        self.fingerprint = fingerprint
        self.status = BUILDING
        self.path: str | None = None
        self.error: str | None = None
        self.users = 0
        self.evicted = False
        self.deleted = False


class SubsetCache:
    def __init__(self, max_entries: int = 2):
        if max_entries < 1:
            raise ValueError("max_entries 至少为 1")
        self._max_entries = max_entries
        self._lock = threading.Lock()
        self._changed = threading.Condition(self._lock)
        # 顺序即生成完成顺序；只有 ready 条目参与淘汰计数
        self._entries: OrderedDict[str, CacheEntry] = OrderedDict()

    @property
    def max_entries(self) -> int:
        return self._max_entries

    def get_or_build(
        self, fingerprint: str, builder: Callable[[], str]
    ) -> tuple[CacheEntry, str]:
        """取得可下载的条目；不存在时由当前请求调用 builder 生成。

        builder 必须返回生成好的文件路径，并在失败时抛异常。
        相同内容正在由别的请求生成时，本请求等待并复用其结果，
        保证同一内容同时只生成一次。
        返回 (entry, outcome)，outcome 为 "built"（本请求生成）或
        "reused"（命中缓存/等待到他人生成的结果）。
        调用方取得条目后须在下载结束（或放弃使用）时调用 release()。
        """
        with self._changed:
            entry = self._entries.get(fingerprint)
            if entry is None:
                # 未命中：由本请求生成，并先占用一个使用者名额
                entry = CacheEntry(fingerprint)
                entry.users = 1
                self._entries[fingerprint] = entry
                logger.info("子库缓存未命中，开始生成：%s", fingerprint[:12])
            elif entry.status == READY:
                entry.users += 1
                logger.info("子库缓存命中，复用已生成文件：%s", fingerprint[:12])
                return entry, "reused"
            else:
                # 另一请求正在生成：等待其完成，避免重复生成
                entry.users += 1
                logger.info("子库正在生成，等待复用：%s", fingerprint[:12])
                while entry.status == BUILDING:
                    self._changed.wait()
                if entry.status == READY:
                    return entry, "reused"
                entry.users -= 1
                self._delete_if_orphaned_locked(entry)
                raise SubsetBuildError(entry.error or "子库生成失败")

        try:
            path = builder()
            if not path:
                raise RuntimeError("子库生成器未返回文件路径")
        except BaseException as exc:
            message = str(exc) or exc.__class__.__name__
            logger.warning("子库生成失败：%s（%s）", message, fingerprint[:12])
            with self._changed:
                entry.status = FAILED
                entry.error = message
                # 失败项不留在缓存，后续请求可直接重试
                if self._entries.get(fingerprint) is entry:
                    del self._entries[fingerprint]
                entry.users -= 1
                entry.evicted = True
                self._delete_if_orphaned_locked(entry)
                self._changed.notify_all()
            if isinstance(exc, Exception):
                raise SubsetBuildError(message) from exc
            raise

        with self._changed:
            entry.status = READY
            entry.path = path
            # 刚生成的排到最新，参与“最近两份”淘汰
            self._entries.move_to_end(fingerprint)
            self._evict_locked()
            self._changed.notify_all()
        logger.info("子库生成完成并通过外键校验：%s", path)
        return entry, "built"

    def release(self, entry: CacheEntry) -> None:
        """下载结束（或等待失败）后调用；可能触发被淘汰文件的延迟删除。"""
        with self._changed:
            if entry.users > 0:
                entry.users -= 1
            self._delete_if_orphaned_locked(entry)

    def snapshot(self) -> dict[str, dict]:
        """调试/测试用：当前缓存条目的轻量快照。"""
        with self._lock:
            return {
                fp: {
                    "status": e.status,
                    "path": e.path,
                    "users": e.users,
                    "evicted": e.evicted,
                    "deleted": e.deleted,
                }
                for fp, e in self._entries.items()
            }

    def _evict_locked(self) -> None:
        ready = [
            fp for fp, e in self._entries.items() if e.status == READY
        ]
        while len(ready) > self._max_entries:
            fp = ready.pop(0)
            entry = self._entries.pop(fp)
            entry.evicted = True
            self._delete_if_orphaned_locked(entry)

    def _delete_if_orphaned_locked(self, entry: CacheEntry) -> None:
        if not entry.evicted or entry.deleted or entry.users > 0:
            return
        entry.deleted = True
        if not entry.path:
            return
        try:
            os.unlink(entry.path)
        except FileNotFoundError:
            pass
        except OSError:
            # 删除失败不应影响正在返回的下载；文件由后续上传清理兜底
            logger.warning("淘汰的子库文件删除失败：%s", entry.path, exc_info=True)
