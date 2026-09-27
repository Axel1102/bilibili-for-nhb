# Linux 无界面服务器部署

这套脚本可以在没有桌面环境的 Linux 服务器上长期运行。`--server-headless` 会强制使用 Playwright 自带的 Chromium，不依赖系统 Chrome、CDP 或图形界面。

## 1. 安装

建议使用 Ubuntu 22.04/24.04 或 Debian 12/13，Python 3.11 以上。在仓库根目录执行：

```bash
uv sync
uv run playwright install --with-deps chromium
```

`playwright install --with-deps chromium` 会同时安装 Chromium 和 Linux 系统依赖。安装系统包时可能需要 sudo 权限。

## 2. 配置 Cookie

在已登录 B 站的浏览器里，从开发者工具的 Network 请求头复制完整 `Cookie` 值，保存为单行文本。Cookie 等同于登录凭据，不要提交到 Git：

```bash
sudo install -d -m 700 /etc/bilibili-for-nhb
sudoedit /etc/bilibili-for-nhb/bilibili.cookie
sudo chmod 600 /etc/bilibili-for-nhb/bilibili.cookie
```

也可以用 `BILIBILI_COOKIE` 环境变量，但独立文件更适合 systemd，也不容易意外出现在命令历史中。

## 3. 运行

把筛选后的名单放到 `data/bilibili/csv/new.csv`，然后先生成分批清单、再拉视频目录，最后采集详情/评论/子评论/弹幕：

```bash
uv run python scripts_lite/bili_creator_pipeline.py prepare

uv run python scripts_lite/bili_creator_pipeline.py catalog \
  --batch-index 0 \
  --server-headless \
  --cookie-file /etc/bilibili-for-nhb/bilibili.cookie

uv run python scripts_lite/bili_creator_pipeline.py crawl \
  --batch-index 0 \
  --server-headless \
  --cookie-file /etc/bilibili-for-nhb/bilibili.cookie
```

也可以用一条 `all` 命令完成上述步骤。弹幕默认开启。所有网络分页和视频都有检查点，重启同一命令会续跑。

```bash
uv run python scripts_lite/bili_creator_stats.py --show unfinished
```

## 4. systemd 托管

复制 `deploy/bili-creator-crawler.service.example` 到 `/etc/systemd/system/bili-creator-crawler.service`，修改其中的用户、项目路径和 Cookie 路径：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now bili-creator-crawler.service
sudo journalctl -u bili-creator-crawler.service -f
```

浏览器进程崩溃时，脚本会以非零状态退出，systemd 会重启它；已成功保存的页不会重爬。Cookie 失效时会明确报错，更新 Cookie 文件后重启服务即可。

## 数据安全

- `data/`、Cookie、浏览器 profile 和虚拟环境都不应提交。
- 请使用专用 B 站账号，保持 1.5–3.5 秒默认请求间隔，遵守平台条款与研究伦理要求。
- 弹幕为当前 XML 快照，不保证覆盖全部历史弹幕。
