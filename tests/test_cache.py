"""子库缓存测试：并发去重、并行生成、失败重试、容量淘汰与延迟删除。"""
import os
import threading
import time

import pytest

from app.subset_cache import SubsetBuildError, SubsetCache


def _touch(path, content=b"x"):
    with open(path, "wb") as f:
        f.write(content)
    return str(path)


def test_same_key_built_once_and_reused(tmp_path):
    cache = SubsetCache(max_entries=2)
    calls = []

    def builder():
        calls.append(1)
        return _touch(tmp_path / "a.db")

    e1, _ = cache.get_or_build("same", builder)
    cache.release(e1)
    e2, _ = cache.get_or_build("same", builder)
    cache.release(e2)
    assert len(calls) == 1
    assert e1 is e2


def test_concurrent_same_key_waits_for_single_build(tmp_path):
    """同时到达的同内容请求只生成一次，等待者复用同一文件。"""
    cache = SubsetCache(max_entries=2)
    calls = []
    started = threading.Event()
    unblock = threading.Event()
    results = []
    errors = []

    def builder():
        calls.append(1)
        started.set()
        assert unblock.wait(5)
        return _touch(tmp_path / "shared.db")

    def worker():
        try:
            entry, _ = cache.get_or_build("same", builder)
            results.append(entry)
            cache.release(entry)
        except Exception as exc:  # pragma: no cover - 失败路径由其他测试覆盖
            errors.append(exc)

    first = threading.Thread(target=worker)
    first.start()
    assert started.wait(5)
    waiters = [threading.Thread(target=worker) for _ in range(4)]
    for t in waiters:
        t.start()
    # 给等待者时间进入等待状态，再放行生成者
    time.sleep(0.1)
    assert not calls[1:]
    unblock.set()
    for t in [first, *waiters]:
        t.join(5)
        assert not t.is_alive()
    assert not errors
    assert len(calls) == 1
    assert len(results) == 5
    assert len({id(e) for e in results}) == 1
    files = list((tmp_path).glob("shared.db"))
    assert len(files) == 1


def test_different_keys_build_in_parallel(tmp_path):
    """不同内容不互相阻塞：两个生成器同时在构建区内。"""
    cache = SubsetCache(max_entries=4)
    barrier = threading.Barrier(2, timeout=5)
    seen = []

    def make_builder(name):
        def builder():
            seen.append(name)
            barrier.wait()  # 两个构建必须同时到达，证明未被串行化
            return _touch(tmp_path / f"{name}.db")

        return builder

    errors = []

    def worker(key):
        try:
            entry, _ = cache.get_or_build(key, make_builder(key))
            cache.release(entry)
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [
        threading.Thread(target=worker, args=("left",)),
        threading.Thread(target=worker, args=("right",)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(5)
    assert not errors
    assert sorted(seen) == ["left", "right"]


def test_failure_notifies_all_waiters_and_allows_retry(tmp_path):
    """生成失败：生成者与所有等待者都收到错误，失败不缓存，随后可重试。"""
    cache = SubsetCache(max_entries=2)
    calls = []
    started = threading.Event()
    unblock = threading.Event()
    outcomes = []

    def failing_builder():
        calls.append("fail")
        started.set()
        assert unblock.wait(5)
        raise RuntimeError("外键校验失败：模拟")

    def succeed_builder():
        calls.append("ok")
        return _touch(tmp_path / "retry.db")

    def worker(builder):
        try:
            entry, _ = cache.get_or_build("same", builder)
            cache.release(entry)
        except SubsetBuildError as exc:
            outcomes.append(str(exc))

    first = threading.Thread(target=worker, args=(failing_builder,))
    first.start()
    assert started.wait(5)
    waiters = [
        threading.Thread(target=worker, args=(failing_builder,))
        for _ in range(3)
    ]
    for t in waiters:
        t.start()
    time.sleep(0.1)
    unblock.set()
    for t in [first, *waiters]:
        t.join(5)
    assert len(outcomes) == 4
    assert all("外键校验失败" in msg for msg in outcomes)
    assert cache.snapshot() == {}

    entry, _ = cache.get_or_build("same", succeed_builder)
    cache.release(entry)
    assert calls.count("ok") == 1
    assert (tmp_path / "retry.db").exists()


def test_eviction_keeps_two_recent_and_defers_deletion_in_use(tmp_path):
    """最多缓存两份；使用中的被淘汰文件在释放后才删除。"""
    cache = SubsetCache(max_entries=2)
    paths = {}

    def builder_for(name):
        def builder():
            paths[name] = _touch(tmp_path / f"{name}.db")
            return paths[name]

        return builder

    first, _ = cache.get_or_build("a", builder_for("a"))
    # 模拟下载中：a 仍被持有，不 release
    second, _ = cache.get_or_build("b", builder_for("b"))
    cache.release(second)
    assert set(cache.snapshot()) == {"a", "b"}

    third, _ = cache.get_or_build("c", builder_for("c"))
    cache.release(third)
    snap = cache.snapshot()
    # 最近生成两份为 b、c，a 被淘汰；但 a 在途，文件暂时保留
    assert set(snap) == {"b", "c"}
    assert os.path.exists(paths["a"])

    cache.release(first)
    assert not os.path.exists(paths["a"])
    assert os.path.exists(paths["b"]) and os.path.exists(paths["c"])


def test_eviction_of_unused_file_is_immediate(tmp_path):
    cache = SubsetCache(max_entries=2)
    paths = {}
    for name in ("a", "b", "c"):
        entry, _ = cache.get_or_build(
            name,
            lambda n=name: paths.setdefault(n, _touch(tmp_path / f"{n}.db")),
        )
        cache.release(entry)
    assert not os.path.exists(paths["a"])
    assert os.path.exists(paths["b"]) and os.path.exists(paths["c"])


def test_max_entries_must_be_positive():
    with pytest.raises(ValueError):
        SubsetCache(max_entries=0)
