# B 站 UP 主数据采集流程

脚本：`scripts_lite/bili_creator_pipeline.py`

## 采集内容

- UP 主视频列表：先独立分页获取并保存。
- 视频详情：标题、完整简介、发布时间、统计数据、BV/AV、分 P 和 CID、视频链接。
- 评论：一级评论及其全部子评论，按 API 分页逐页保存。
- 弹幕：默认抓取每个 CID 当前可访问的 XML 弹幕快照。历史弹幕和完整性不作保证。

脚本不会下载视频正文。`video.json` 会保留视频链接、BV/AV 和每个分 P 的 CID，后续可以建立单独的下载、音频转写、字幕分析或关键帧分析流程。不要保存播放接口返回的 CDN 直链，因为这类地址通常会过期。

## 默认目录

```text
data/bili/creator_dataset/
  inputs/
    creators_selected.csv
    manifest.json
    batches/batch_001.csv
  batches/batch_001/creators/<uid>_<name>/
    creator.json
    videos.jsonl
    catalog/pages/page_00001.json
    videos/<bvid>/
      detail.json
      comments/top/page_00001.json
      comments/sub/<root_rpid>/pages/page_00001.json
      danmaku/cid_<cid>.json
      video.json
      complete.json
  state/catalog_errors.jsonl
  state/crawl_errors.jsonl
```

每个网络分页先写入临时文件，再原子替换正式文件。只有正式文件保存成功后才推进进度。重新执行相同命令会跳过已完成的 UP 主、视频和评论页。

## 使用方法

在项目根目录执行：

```bash
# 1. 从 data/bilibili/csv/new.csv 读取名单，生成精确标记为 1 的 215 个 UP 主 CSV，并每 100 人分批
uv run python scripts_lite/bili_creator_pipeline.py prepare

# 2. 先获取第一批 100 个 UP 主的全部视频列表
uv run python scripts_lite/bili_creator_pipeline.py catalog --batch-index 1

# 3. 获取第一批视频详情、简介、一级评论、子评论和当前弹幕
uv run python scripts_lite/bili_creator_pipeline.py crawl --batch-index 1

# 如需临时关闭弹幕
uv run python scripts_lite/bili_creator_pipeline.py crawl --batch-index 1 --no-danmaku
```

`--batch-index 0` 表示处理全部批次。建议先运行第一批并检查数据质量、登录状态和风控情况，再依次运行第 2、3 批。

模糊判断值默认排除。如确实要包含 `1？`、`1？已毕业` 等以 `1` 开头的值，可在 `prepare` 或 `all` 命令添加 `--include-ambiguous`。

默认输入是 `data/bilibili/csv/new.csv`。如以后需要从原始工作簿重新生成，可显式传入 `--input-xlsx 保留.xlsx`；该参数会覆盖默认 CSV。

## 小规模验证

第一次运行建议限制每个 UP 主的视频数：

```bash
uv run python scripts_lite/bili_creator_pipeline.py crawl \
  --batch-index 1 \
  --max-videos-per-creator 2 \
  --max-comments-per-video 40
```

确认输出字段和账号状态后，再去掉两个上限参数。默认请求间隔为 1.5–3.5 秒并带重试；不要把间隔和并发调得过于激进。
