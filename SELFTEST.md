# 交付自测记录：预览页下载复用

## 实际实现

| 需求 | 实现 |
| --- | --- |
| 同一次上传内，勾选不同但补齐后四张表记录完全相同 → 共用子库 | `Resolution.fingerprint()`（`app/core.py`）对解析后的 `group_ids/calibration_ids/test_ids/reading_ids` 排序序列化后取 SHA-256。源库在一次上传内不可变、行由主键唯一确定，故主键集合相同即四张表记录完全相同。缓存挂在 `SessionState.cache`，随上传新建，不跨上传复用。 |
| 相同内容同时请求只生成一次 | `SubsetCache.get_or_build()`（`app/subset_cache.py`）用 `Condition`：首个请求登记 `building` 条目并在锁外生成；同键后续请求登记使用者后等待条件变量，生成完成即复用同一文件。`tests/test_cache.py::test_concurrent_same_key_waits_for_single_build`、`tests/test_download_cache.py::test_same_content_concurrent_downloads_build_once` 以栅栏/事件强制并发验证。 |
| 不同内容可并行生成 | 生成在缓存锁外进行，锁只保护条目表；条目键不同互不等待。`test_different_keys_build_in_parallel` 用 `threading.Barrier(2)` 证明两个生成器同时处于临界区；Web 层 `test_different_content_concurrent_downloads_build_in_parallel` 同样用 Barrier。 |
| 引用冲突仍阻止下载 | 下载端点先 `resolve()`，`not res.ok` 时 409，且不进入缓存/生成（`test_conflict_still_blocks_before_cache` 断言 `build_subset` 未被调用、缓存为空）。 |
| 生成/校验失败：所有等待者收到错误，之后可重试 | 生成者捕获异常 → 条目标 `failed`、从缓存删除、`notify_all()`；所有等待者从 `Condition` 醒来后抛 `SubsetBuildError`，端点统一返回 500「子库生成失败」页（含原因）。失败项不缓存，下一次请求重新生成。`test_failure_notifies_all_waiters_and_allows_retry`（4 个并发请求全部收到错误，随后重试成功且只生成一次）；`test_build_failure_leaves_no_partial_file` 验证半成品文件被删除并重试成功。 |
| 每次上传最多缓存最近两份 | `_evict_locked()` 只统计 `ready` 条目，按完成顺序淘汰最早项；`max_entries=2`。`test_eviction_*`、`test_only_two_most_recent_subsets_are_kept` 覆盖（含淘汰后重新下载）。 |
| 淘汰文件等在途下载结束再删除 | 条目带 `users` 计数：取得条目（等待/下载）时 +1，`_ReleasingFileResponse.__call__` 的 `finally` 中 `release()`。淘汰只标记 `evicted`，`users>0` 时不删；最后一个使用者释放后 `unlink`。`test_eviction_keeps_two_recent_and_defers_deletion_in_use` 与 `test_download_in_progress_survives_eviction`（直接驱动 ASGI 响应，发送结束后文件才消失）。 |

其他：`X-Subset-Cache: built|reused` 响应头标示生成/复用；`build()` 失败时删除半成品；
子库文件名改为随机后缀，避免并行构建撞名；README 增补「子库复用与并发」说明。

## 自测结果

```
$ .venv/bin/python -m pytest -q
33 passed, 1 warning in 2.9s
```

其中原有 18 项全部保留通过；新增 `tests/test_cache.py`（7 项，缓存并发/失败/淘汰单元测试）
与 `tests/test_download_cache.py`（8 项，Web 流程端到端）。并发测试连续运行 3 次均通过。

Web/预览页实际演示（本机 uvicorn，`DATA_DIR` 临时目录，演示库由 `scripts/make_sample_db.py` 生成）：

1. 上传 → `303 /select`；勾选 `test=1` → 预览页 200，页面含「确认并下载 SQLite 子库」及跨组带入的「读数 #901」；
2. POST `/download` → 200，`x-subset-cache: built`，下载文件 `PRAGMA foreign_key_check` 为空，读数 `{101,102,103,901}`；
3. 改为勾选 `test=1,9` 并剔除 `902,903`（解析后内容相同）→ `/download` → 200，
   `x-subset-cache: reused`，下载字节与第 2 步 `cmp` 完全一致；
4. 依次下载 T2、T3 三种不同内容后，会话目录中 `subset_*.db` 始终只有 2 个；重新下载最早被淘汰的 T1 会重新生成且仍只保留 2 个；
5. 同时发起 6 个相同内容下载，全部 `x-subset-cache: reused`，会话目录仍只有两个子库文件；
6. 构造外键悬空的额外列（`groups.extra_test_id → tests(id)`，值指向未交付试验）使导出后
   `foreign_key_check` 失败：`/download` 返回 500「子库生成失败」，页面含
   `子库外键校验失败：[('groups', 1, 'tests', 0)]`，会话目录无 `subset_*` 残留；再次请求仍为 500（未缓存失败），
   修正后可正常下载（该重试路径由 pytest 覆盖）。

## Docker Compose

`docker compose config` 校验通过（顶部结构正常）。但当前执行环境**没有 Docker 守护进程**
（无 docker CLI/daemon，`/var/run/docker.sock` 不存在；用户命名空间被禁用，无法运行 rootless
dockerd；镜像仓库地址不可达），因此**未能实际执行 `docker compose up --build` 启动容器**。
上面第 1–6 步为同一应用代码在宿主机 uvicorn 下通过 HTTP 完成，Dockerfile/Compose 未改动。

## 遗留事项

- 未能在本环境实际启动 Docker Compose 容器，需在具备 Docker 的机器上复核
  `docker compose up --build` 与 `http://localhost:8000` 的预览页下载演示；
- 缓存与在途计数为单进程内存实现，与既有限制一致：多 worker（`uvicorn --workers>1`）
  或多实例部署时各自持有缓存，无法跨进程去重，且淘汰只作用于本进程；
- 缓存只按主键集合判定内容；若同一次上传期间源库被外部改写，指纹不感知（现有流程源库只读上传副本，风险低）；
- 失败信息直接来自 `foreign_key_check` 元组，未做面向业务人员的中文化包装；
- 上传目录的整体清理仍依赖运维（既有已知限制），本次只保证单次上传最多两份子库。
