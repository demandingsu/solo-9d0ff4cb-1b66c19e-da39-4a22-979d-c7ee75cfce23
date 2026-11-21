"""隔声试验数据交付复核 Web 应用（FastAPI）。

流程：上传 SQLite 库 → 勾选试验 / 剔除异常读数 → 预览引用解析结果
（含跨组拉入链与冲突）→ 确认后下载 SQLite 子库。
"""
from __future__ import annotations

import html
import os
import shutil
import sqlite3
import uuid
from dataclasses import dataclass, field

from fastapi import FastAPI, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from .core import build_subset, load_model, resolve, validate_schema
from .subset_cache import CacheEntry, SubsetBuildError, SubsetCache

DATA_DIR = os.environ.get("DATA_DIR", "./data")
MAX_UPLOAD_MB = int(os.environ.get("MAX_UPLOAD_MB", "200"))

app = FastAPI(title="隔声试验数据交付复核")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
)


@dataclass
class SessionState:
    workdir: str
    source_name: str
    selected_tests: set[int] = field(default_factory=set)
    excluded_readings: set[int] = field(default_factory=set)
    # 每次上传独立的子库缓存：不同勾选只要解析结果相同即命中同一条目
    cache: SubsetCache = field(default_factory=SubsetCache)

    @property
    def db_path(self) -> str:
        return os.path.join(self.workdir, "source.db")

    def next_subset_path(self) -> str:
        # 并行的不同内容构建各自取唯一文件名
        return os.path.join(self.workdir, f"subset_{uuid.uuid4().hex}.db")


# 单进程内存会话；重启即失效（上传文件保留在 DATA_DIR 下，由运维清理）
SESSIONS: dict[str, SessionState] = {}


def _get_session(request: Request) -> SessionState | None:
    sid = request.cookies.get("sid")
    return SESSIONS.get(sid) if sid else None


def _load(st: SessionState):
    conn = sqlite3.connect(f"file:{st.db_path}?mode=ro", uri=True)
    try:
        return load_model(conn)
    finally:
        conn.close()


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse(
        request, "upload.html", {"error": None}
    )


@app.post("/upload", response_class=HTMLResponse)
async def upload(request: Request, file: UploadFile):
    error: str | None = None
    if not file.filename:
        error = "未选择文件"
    data = await file.read()
    if error is None and len(data) > MAX_UPLOAD_MB * 1024 * 1024:
        error = f"文件超过大小限制（{MAX_UPLOAD_MB} MB）"

    if error is None:
        # 旧会话的工作目录一并清理
        old = _get_session(request)
        if old:
            SESSIONS.pop(request.cookies.get("sid"), None)
            shutil.rmtree(old.workdir, ignore_errors=True)

        sid = uuid.uuid4().hex
        workdir = os.path.join(DATA_DIR, sid)
        os.makedirs(workdir, exist_ok=True)
        path = os.path.join(workdir, "source.db")
        with open(path, "wb") as f:
            f.write(data)
        try:
            conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            errors = validate_schema(conn)
            conn.close()
        except sqlite3.DatabaseError:
            errors = ["文件不是有效的 SQLite 数据库"]
        if errors:
            shutil.rmtree(workdir, ignore_errors=True)
            return templates.TemplateResponse(
                request,
                "upload.html",
                {"error": "；".join(errors)},
                status_code=400,
            )
        SESSIONS[sid] = SessionState(workdir, file.filename or "source.db")
        resp = RedirectResponse("/select", status_code=303)
        resp.set_cookie("sid", sid, httponly=True)
        return resp

    return templates.TemplateResponse(
        request, "upload.html", {"error": error}, status_code=400
    )


# 读数表展示时的优先列序，其余列按出现顺序排后
_PREFERRED_READING_COLS = [
    "id", "kind", "frequency_hz", "level_db",
    "control_reading_id", "calibration_id", "note",
]


@app.get("/select", response_class=HTMLResponse)
def select_page(request: Request):
    st = _get_session(request)
    if not st:
        return RedirectResponse("/", status_code=303)
    model = _load(st)

    headers: list[str] = []
    for r in model.readings.values():
        for col in r:
            if col not in headers:
                headers.append(col)
    headers = [c for c in _PREFERRED_READING_COLS if c in headers] + [
        c for c in headers if c not in _PREFERRED_READING_COLS
    ]

    groups = []
    for gid in sorted(model.groups):
        g = model.groups[gid]
        tests = []
        for t in sorted(model.tests.values(), key=lambda x: x["id"]):
            if t["group_id"] != gid:
                continue
            readings = [
                {
                    "id": r["id"],
                    "cells": [r.get(c) for c in headers],
                    "excluded": r["id"] in st.excluded_readings,
                }
                for r in (
                    model.readings[rid]
                    for rid in model.readings_by_test.get(t["id"], [])
                )
            ]
            tests.append(
                {
                    "row": t,
                    "readings": readings,
                    "selected": t["id"] in st.selected_tests,
                }
            )
        groups.append({"row": g, "tests": tests})

    return templates.TemplateResponse(
        request,
        "select.html",
        {
            "groups": groups,
            "headers": headers,
            "source": st.source_name,
        },
    )


@app.post("/select")
async def select_save(request: Request):
    st = _get_session(request)
    if not st:
        return RedirectResponse("/", status_code=303)
    form = await request.form()
    model = _load(st)

    selected = {int(v) for v in form.getlist("test")} & set(model.tests)
    # 剔除仅对所选试验内的读数生效，其余静默忽略
    excluded = {
        int(v)
        for v in form.getlist("excl")
        if (r := model.readings.get(int(v))) and r["test_id"] in selected
    }
    st.selected_tests = selected
    st.excluded_readings = excluded
    return RedirectResponse("/preview", status_code=303)


@app.get("/preview", response_class=HTMLResponse)
def preview(request: Request):
    st = _get_session(request)
    if not st:
        return RedirectResponse("/", status_code=303)
    model = _load(st)
    res = resolve(model, st.selected_tests, st.excluded_readings)
    return templates.TemplateResponse(
        request,
        "preview.html",
        {
            "res": res,
            "model": model,
            "selected": sorted(st.selected_tests),
            "excluded": sorted(st.excluded_readings),
            "pulled_in": sorted(res.pulled_in.items()),
            "source": st.source_name,
        },
    )


@app.post("/download")
def download(request: Request):
    """生成（或复用）当前选择的交付子库并下载。

    内容相同的子库在一次上传内只生成一次：同时到达的请求等待首个请求
    生成完成后复用同一文件；不同内容各自并行生成。
    """
    st = _get_session(request)
    if not st:
        return RedirectResponse("/", status_code=303)
    model = _load(st)
    # 下载前重新解析，防止预览之后状态被修改导致不一致
    res = resolve(model, st.selected_tests, st.excluded_readings)
    if not res.ok:
        return HTMLResponse(
            "<h2>已阻止下载</h2><p>存在未解决的引用冲突（悬空或指向已剔除读数）。"
            "请返回 <a href='/preview'>预览</a> 查看引用链，"
            "或回 <a href='/select'>选择页</a> 修改。</p>",
            status_code=409,
        )

    fingerprint = res.fingerprint()
    name = os.path.splitext(st.source_name)[0] + "_subset.db"

    def build() -> str:
        out_path = st.next_subset_path()
        try:
            build_subset(st.db_path, out_path, res)
        except BaseException:
            # 失败时不留半成品文件
            try:
                os.unlink(out_path)
            except OSError:
                pass
            raise
        return out_path

    try:
        entry, outcome = st.cache.get_or_build(fingerprint, build)
    except SubsetBuildError as exc:
        return HTMLResponse(
            _download_error_page(str(exc)), status_code=500
        )

    try:
        response = _stream_entry(st, entry, name)
    except BaseException:
        st.cache.release(entry)
        raise
    response.headers["x-subset-cache"] = outcome
    return response


class _ReleasingFileResponse(FileResponse):
    """下载响应结束后释放缓存条目的使用者名额。

    流式发送期间条目 users > 0，即使已被淘汰也不会删文件；
    响应结束（含客户端断开/异常）后再按需删除。
    """

    def __init__(self, *args, on_close, **kwargs):
        super().__init__(*args, **kwargs)
        self._on_close = on_close

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        finally:
            self._on_close()


def _stream_entry(st: SessionState, entry: CacheEntry, name: str):
    def on_close() -> None:
        st.cache.release(entry)

    return _ReleasingFileResponse(
        entry.path,
        filename=name,
        media_type="application/x-sqlite3",
        on_close=on_close,
    )


def _download_error_page(detail: str) -> str:
    return (
        "<h2>子库生成失败</h2>"
        f"<p class='err'>本次子库生成或外键校验失败：{html.escape(detail)}</p>"
        "<p>所有等待该子库的请求都已收到失败提示；失败结果未缓存，"
        "可返回 <a href='/preview'>预览页</a> 重新下载，"
        "或回 <a href='/select'>选择页</a> 修改。</p>"
    )
