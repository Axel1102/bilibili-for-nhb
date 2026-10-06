# bilibili-for-nhb

面向研究数据采集的 B 站 UP 主批量脚本，从人工筛选后的 CSV/XLSX 名单出发，分两阶段完成：

1. 先下载所有 UP 主的完整视频目录。
2. 再逐个保存视频标题、简介、详情、一级评论、子评论和当前弹幕 XML 快照。

数据按“每个 UP 主一个目录”保存，默认每 100 人分一批。所有网络分页都原子写入并记录检查点，断电或进程重启后重复同一命令即可续跑。

## Linux 服务器快速开始

需要 Python 3.11+ 和 [uv](https://docs.astral.sh/uv/)：

```bash
git clone https://github.com/Axel1102/bilibili-for-nhb.git
cd bilibili-for-nhb
uv sync
uv run playwright install --with-deps chromium
```

如果服务器不使用 uv：

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/playwright install --with-deps chromium
```

复制筛选名单（可参考 `examples/creators.csv`）：

```bash
mkdir -p data/bilibili/csv
cp /path/to/new.csv data/bilibili/csv/new.csv
```

把已登录 B 站浏览器的完整 Cookie 请求头保存到仓库之外，并设为仅当前用户可读：

```bash
chmod 600 /secure/path/bilibili.cookie
```

执行：

```bash
# 生成精确 result=1 的名单和分批
uv run python scripts_lite/bili_creator_pipeline.py prepare

# 先拉取全部 UP 主的视频目录
uv run python scripts_lite/bili_creator_pipeline.py catalog \
  --batch-index 0 --server-headless \
  --cookie-file /secure/path/bilibili.cookie

# 采集详情、评论、子评论和弹幕（默认开启）
uv run python scripts_lite/bili_creator_pipeline.py crawl \
  --batch-index 0 --server-headless \
  --cookie-file /secure/path/bilibili.cookie

# 查看进度、有效内容和已完成 UP 主
uv run python scripts_lite/bili_creator_stats.py --show unfinished
```

### 多进程并行采集

先用单进程完成 `catalog`，再启动并行采集。每个 worker 使用独立的 Chromium profile，脚本会根据各 UP 主剩余视频数自动均衡分片：

```bash
uv run python scripts_lite/bili_parallel_runner.py \
  --output-root /path/to/creator_video_catalog_server_ready \
  --cookie-file /secure/path/bilibili.cookie \
  --workers 3
```

日志保存在 `<output-root>/state/parallel_logs/worker_*.log`。`--workers` 没有硬性上限，但进程越多，服务器内存和 CPU 占用越高，也越容易触发平台风控；建议逐步从 3 个增加。并行模式默认把每个 worker 的请求间隔调整为 3–6 秒。

### 导出视频 URL、标题和简介

导出所有已获取到 `detail.json` 的视频；评论或弹幕还未完成的视频也会导出，并通过 `fully_completed` 字段标记：

```bash
uv run python scripts_lite/bili_export_video_metadata.py \
  --dataset-root /path/to/creator_video_catalog_server_ready
```

默认生成 `<dataset-root>/exports/video_metadata.csv` 和 `video_metadata.jsonl`。`original_description` 是 B 站原简介，空白的 `generated_description` 可用于后续生成结果。添加 `--completed-only` 可仅导出评论、子评论和弹幕均已完成的视频。

### 并发生成视频 description

下面的脚本读取上述导出文件，下载完整音轨，上传到百炼做 ASR，再由 Qwen 根据标题、原简介和转写生成 description。如果 ASR 失败、没有有效转写，或文本模型判断必须查看画面，默认只把该视频加入待处理队列，不下载或上传视频。Cookie 只用于 B 站下载，不会发给百炼；音频会上传到百炼。每条结果独立保存，重启时从已完成阶段继续：

```bash
export DASHSCOPE_API_KEY='你的百炼 API Key'
python scripts_lite/bili_generate_descriptions.py \
  --input /path/to/creator_video_catalog_server_ready/exports/video_metadata.jsonl \
  --output-dir /path/to/creator_video_catalog_server_ready/descriptions \
  --cookie-file /path/to/bilibili.cookie \
  --workers 12 \
  --download-workers 3 \
  --video-mode queue
```

`--workers` 是实际工作线程数，`--download-workers` 只限制同时访问 B 站下载音频的线程数。每个线程分别写入 `<output-dir>/worker_logs/worker_*.log`，其中记录音频下载、ASR、文本模型和排队阶段。默认 `--video-mode queue` 不执行视频理解，待处理清单写入 `video_understanding_queue.jsonl` 和 `.csv`。

以后需要自动继续视频理解时可改为 `--video-mode auto`，并用 `--video-workers` 限制同时进行的视频上传与模型调用数。视频理解默认使用 `qwen3.8-omni-flash`，下载视频的最高分辨率为 360p，可通过 `--video-model` 和 `--max-video-height` 调整。默认成功后删除本地音频和低清视频；加 `--keep-audio` 或 `--keep-video` 可保留。

汇总结果为 `results.jsonl` 和 `results.csv`。`description_source` 表示最终描述来自 `text` 还是 `video`；`text_description` 保留视频理解前的文本阶段描述。失败项写入 `failures.jsonl`。对其他 `partial` 结果可用 `--retry-partial` 重试。

正式批量处理前建议添加 `--limit 10` 验证 API 额度、下载速度和输出质量。完整 ASR 转写保存在每个视频的 `items/<video_id>/asr.json` 中，汇总文件只保存 description 和状态字段，避免重复存储大段转写。

### 统计 ASR、description 和音频进度

```bash
python scripts_lite/bili_description_stats.py \
  --output-dir /path/to/creator_video_catalog_server_ready/descriptions
```

脚本会自动读取同一数据集下的 `exports/video_metadata.jsonl`，统计输入总数、`result.json`、ASR、非空转写、description、只有 ASR 没有 description、当前仍保留的音频，以及可以确认曾成功下载过音频的视频数。完整统计同时写入 `<output-dir>/description_stats.json`。

可用 `all` 代替 prepare/catalog/crawl 三步。详细说明见：

- [数据结构与参数](docs/bili_creator_pipeline.md)
- [Linux 无界面部署与 systemd](docs/bili_linux_server.md)

## 视频内容分析

本脚本不下载视频正文。`video.json` 会保留视频链接、BV/AV 号和每个分 P 的 CID，后续可以独立进行合规下载、音频转写、字幕分析和关键帧分析。不建议保存播放 API 的 CDN 直链，它们通常会过期。

## 仓库边界

这是一个可独立在 Linux 服务器运行的 B 站聚焦包。仓库故意不包含研究名单、采集结果、Cookie、浏览器 profile 或虚拟环境。

WBI 签名逻辑改编自 [MediaCrawler](https://github.com/NanmiCoder/MediaCrawler)。本仓库仅限非商业学习和研究使用，详见 [LICENSE](LICENSE)。请合理控制请求频率，遵守平台条款、robots.txt、适用法律与研究伦理要求。
