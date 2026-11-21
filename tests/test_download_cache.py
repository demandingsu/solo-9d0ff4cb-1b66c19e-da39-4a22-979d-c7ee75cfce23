"""下载复用端到端测试：不同勾选同内容复用、并发去重/并行、失败重试。"""
import asyncio
import os
import sqlite3
import threading
import time

import pytest
from fastapi.testclient import TestClient

from app import main

# 两种勾选方式，解析后四张表记录完全相同：
#   A：只勾 T1（沿引用拉入 R901/T9/G3/C2）
#   B：勾 T1 与 T9，并剔除 T9 中未被引用到的 R902、R903
SAME_CONTENT_SELECTIONS = [
    {"test": ["1"]},
    {"test": ["1", "9"], "excl": ["902", "903"]},
]


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(main, "DATA_DIR", str(tmp_path / "data"))
    main.SESSIONS.clear()
    return TestClient(main.app)


def _upload(client, sample_db):
    with open(sample_db, "rb") as f:
        resp = client.post(
            "/upload",
            files={"file": ("lab.db", f, "application/x-sqlite3")},
            follow_redirects=False,
        )
    assert resp.status_code == 303
    return resp.cookies["sid"]


def _client_for(sid) -> TestClient:
    client = TestClient(main.app)
    client.cookies.set("sid", sid)
    return client


def _download(client):
    resp = client.post("/download")
    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"].startswith("application/x-sqlite3")
    return resp


def _download_bytes(client):
    return _download(client).content


def test_different_selections_same_content_share_one_subset(
    client, sample_db, monkeypatch
):
    sid = _upload(client, sample_db)
    calls = []
    real_build = main.build_subset

    def counting_build(*args, **kwargs):
        calls.append(1)
        return real_build(*args, **kwargs)

    monkeypatch.setattr(main, "build_subset", counting_build)

    payloads = []
    for data in SAME_CONTENT_SELECTIONS:
        client.post("/select", data=data, follow_redirects=False)
        # 预览页同样不应触发子库生成
        assert client.get("/preview").status_code == 200
        payloads.append(_download_bytes(client))

    assert len(calls) == 1, "相同内容应只生成一次"
    assert payloads[0] == payloads[1]
    st = main.SESSIONS[sid]
    assert len(st.cache.snapshot()) == 1

    # 第二次下载应命中缓存而非重新生成
    resp = _download(client)
    assert resp.headers["x-subset-cache"] == "reused"
    assert len(calls) == 1

    # 下载到的确实是解析结果对应的子库
    conn = sqlite3.connect("file:subset?mode=memory", uri=True)
    conn.deserialize(payloads[0])
    try:
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        assert {r[0] for r in conn.execute("SELECT id FROM readings")} == {
            101, 102, 103, 901,
        }
    finally:
        conn.close()


def test_same_content_concurrent_downloads_build_once(
    client, sample_db, monkeypatch
):
    sid = _upload(client, sample_db)
    client.post("/select", data={"test": ["1"]}, follow_redirects=False)

    calls = []
    lock = threading.Lock()
    entered = threading.Event()
    unblock = threading.Event()
    real_build = main.build_subset

    def slow_build(*args, **kwargs):
        with lock:
            calls.append(1)
        entered.set()
        assert unblock.wait(10)
        return real_build(*args, **kwargs)

    monkeypatch.setattr(main, "build_subset", slow_build)

    results = []
    errors = []

    def worker():
        try:
            results.append(_download_bytes(_client_for(sid)))
        except Exception as exc:  # pragma: no cover - 失败时输出诊断
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    assert entered.wait(10)
    time.sleep(0.2)  # 其余请求进入等待
    unblock.set()
    for t in threads:
        t.join(10)
        assert not t.is_alive()

    assert not errors
    assert len(calls) == 1
    assert len(results) == 4
    assert len({payload for payload in results}) == 1
    assert len(main.SESSIONS[sid].cache.snapshot()) == 1


def test_different_content_concurrent_downloads_build_in_parallel(
    client, sample_db, monkeypatch
):
    """同一上传内，内容 A 的生成尚未结束时，内容 B 可以同时生成。"""
    sid = _upload(client, sample_db)
    real_build = main.build_subset
    barrier = threading.Barrier(2, timeout=10)
    built = []
    lock = threading.Lock()

    def parallel_build(*args, **kwargs):
        with lock:
            built.append(1)
        barrier.wait()  # 两种不同内容必须同时处于生成中
        return real_build(*args, **kwargs)

    monkeypatch.setattr(main, "build_subset", parallel_build)

    results = {}
    errors = []

    def worker(tag):
        try:
            results[tag] = _download_bytes(_client_for(sid))
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    # 下载线程开始后，会话选择状态仍可被另一请求改写，
    # 从而产生“同一上传、不同内容”的并发下载。
    a = threading.Thread(target=worker, args=("a",))
    a.start()
    time.sleep(0.3)  # A 已进入生成（barrier 等待 B）
    client.post("/select", data={"test": ["2"]}, follow_redirects=False)
    b = threading.Thread(target=worker, args=("b",))
    b.start()
    a.join(10)
    b.join(10)

    assert not errors
    assert len(built) == 2
    assert results["a"] != results["b"]
    assert len(main.SESSIONS[sid].cache.snapshot()) == 2


def test_build_failure_reaches_all_waiters_then_retry_succeeds(
    client, sample_db, monkeypatch
):
    sid = _upload(client, sample_db)
    client.post("/select", data={"test": ["1"]}, follow_redirects=False)

    real_build = main.build_subset
    state = {"fail": True, "fail_calls": 0, "ok_calls": 0}
    entered = threading.Event()
    unblock = threading.Event()
    lock = threading.Lock()

    def flaky_build(*args, **kwargs):
        with lock:
            if state["fail"]:
                state["fail_calls"] += 1
            else:
                state["ok_calls"] += 1
        if state["fail"]:
            entered.set()
            assert unblock.wait(10)
            raise RuntimeError("模拟外键校验失败")
        return real_build(*args, **kwargs)

    monkeypatch.setattr(main, "build_subset", flaky_build)

    statuses = []
    bodies = []

    def worker():
        resp = _client_for(sid).post("/download")
        statuses.append(resp.status_code)
        bodies.append(resp.text)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    assert entered.wait(10)
    time.sleep(0.2)
    unblock.set()
    for t in threads:
        t.join(10)
        assert not t.is_alive()

    assert state["fail_calls"] == 1, "失败的一次生成不应被重复触发"
    assert statuses == [500] * 4
    assert all("子库生成失败" in body for body in bodies)
    assert all("模拟外键校验失败" in body for body in bodies)
    assert main.SESSIONS[sid].cache.snapshot() == {}

    # 失败结果未缓存：随后可直接重试成功
    state["fail"] = False
    payload = _download_bytes(_client_for(sid))
    assert state["ok_calls"] == 1
    assert len(payload) > 0
    assert len(main.SESSIONS[sid].cache.snapshot()) == 1


def test_conflict_still_blocks_before_cache(client, sample_db, monkeypatch):
    sid = _upload(client, sample_db)
    client.post(
        "/select", data={"test": ["1", "9"], "excl": ["901"]}, follow_redirects=False
    )
    calls = []
    monkeypatch.setattr(
        main, "build_subset", lambda *a, **k: calls.append(1)
    )
    resp = client.post("/download")
    assert resp.status_code == 409
    assert not calls
    assert main.SESSIONS[sid].cache.snapshot() == {}


def test_build_failure_leaves_no_partial_file(client, sample_db, monkeypatch):
    """生成/校验失败时删除半成品；错误页可重试，重试成功后正常下载。"""
    sid = _upload(client, sample_db)
    client.post("/select", data={"test": ["1"]}, follow_redirects=False)
    st = main.SESSIONS[sid]

    real_build = main.build_subset
    state = {"fail": True}

    def flaky_build(source_path, out_path, res):
        if state["fail"]:
            # 模拟已写出部分内容后外键校验失败
            with open(out_path, "wb") as f:
                f.write(b"partial")
            raise RuntimeError("子库外键校验失败：模拟")
        return real_build(source_path, out_path, res)

    monkeypatch.setattr(main, "build_subset", flaky_build)

    resp = client.post("/download")
    assert resp.status_code == 500
    assert "子库生成失败" in resp.text
    assert not [
        n for n in os.listdir(st.workdir) if n.startswith("subset_")
    ], "失败后不应留下文件"

    state["fail"] = False
    payload = _download_bytes(client)
    assert len(payload) > 0
    assert len(st.cache.snapshot()) == 1


def test_only_two_most_recent_subsets_are_kept(client, sample_db):
    """每次上传最多保留最近生成的两份子库，更早的文件被删除。"""
    sid = _upload(client, sample_db)
    st = main.SESSIONS[sid]

    # 三种不同内容，依次下载
    for test_id in ("1", "2", "3"):
        client.post("/select", data={"test": [test_id]}, follow_redirects=False)
        _download(client)

    snap = st.cache.snapshot()
    assert len(snap) == 2
    files = sorted(
        name for name in os.listdir(st.workdir) if name.startswith("subset_")
    )
    assert len(files) == 2
    cached_paths = {os.path.basename(e["path"]) for e in snap.values()}
    assert set(files) == cached_paths

    # 重新下载最早被淘汰的内容：重新生成一份，仍只保留两份
    client.post("/select", data={"test": ["1"]}, follow_redirects=False)
    _download(client)
    assert len(st.cache.snapshot()) == 2
    assert (
        len([n for n in os.listdir(st.workdir) if n.startswith("subset_")]) == 2
    )


def test_download_in_progress_survives_eviction(client, sample_db):
    """下载中（响应尚未发送完）的子库被淘汰时不删除文件，发送完再删除。"""
    sid = _upload(client, sample_db)
    st = main.SESSIONS[sid]
    client.post("/select", data={"test": ["1"]}, follow_redirects=False)

    # 直接调用下载端点拿到响应，但先不发送（模拟下载仍在进行中）
    request = _request_with_sid(sid)
    pending = main.download(request)
    assert isinstance(pending, main._ReleasingFileResponse)
    entry = next(iter(st.cache.snapshot().values()))
    in_flight_path = entry["path"]
    assert os.path.exists(in_flight_path)

    # 再用两份不同内容把第一份淘汰
    for test_id in ("2", "3"):
        client.post("/select", data={"test": [test_id]}, follow_redirects=False)
        _download(client)

    assert in_flight_path not in {
        e["path"] for e in st.cache.snapshot().values()
    }
    assert os.path.exists(in_flight_path), "在途下载结束前不应删除文件"

    # 模拟响应发送结束，触发 release
    asyncio.run(
        pending(
            {"type": "http", "method": "POST", "headers": []},
            _receive_once,
            _discard_send,
        )
    )
    assert not os.path.exists(in_flight_path), "最后一个使用者释放后应删除文件"


def _request_with_sid(sid: str):
    from starlette.requests import Request

    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/download",
            "headers": [(b"cookie", f"sid={sid}".encode())],
            "query_string": b"",
        }
    )


async def _receive_once():
    # 文件发送完毕后 FileResponse 会等待 http.disconnect 才结束
    return {"type": "http.disconnect"}


async def _discard_send(message):
    return None
