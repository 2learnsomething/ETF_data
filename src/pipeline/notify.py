"""
通知推送模块

支持 ServerChan / 企业微信，从 ETF_Rotation_Strategy 复用 WeChatNotifier。
推送 pipeline 运行结果摘要。
"""
from __future__ import annotations

import sys
from datetime import datetime

# 复用 ETF_Rotation_Strategy 的通知模块
sys.path.insert(0, "/home/fangyao_xu/ETF_Rotation_Strategy")


def send_pipeline_report(results: list[dict], dry_run: bool = False) -> bool:
    """
    推送 pipeline 运行结果。

    Args:
        results: pipeline.run_all() 的返回结果
        dry_run: 是否为干跑模式

    Returns:
        是否发送成功
    """
    ok = sum(1 for r in results if r["status"] == "ok")
    fail = len(results) - ok
    total_rows = sum(r.get("rows_written", 0) for r in results)
    total_time = sum(r["elapsed"] for r in results)

    title = "ETF Data Pipeline"
    if dry_run:
        title += " [DRY-RUN]"
    if fail > 0:
        title += f" ⚠️ {fail} failed"

    lines = [
        f"## {title}",
        f"**时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"**结果**: {ok} ok, {fail} failed, {total_rows:,} rows in {total_time:.1f}s",
        "",
    ]

    for r in results:
        status = "✓" if r["status"] == "ok" else "✗"
        line = f"- {status} **{r['task']}**: {r.get('rows_written', 0):,} rows ({r['elapsed']}s)"
        if r.get("error"):
            line += f" — `{r['error'][:80]}`"
        if r.get("validation"):
            line += f" — {r['validation']}"
        lines.append(line)

    try:
        from Notify.wechat import WeChatNotifier
        notifier = WeChatNotifier()
        notifier.send_text("\n".join(lines))
        return True
    except ImportError:
        # Fallback: 用 config 里的 sendkey 推 ServerChan
        return _send_serverchan(lines)

    except Exception as e:
        print(f"[notify] WeChatNotifier failed: {e}")
        return _send_serverchan(lines)


def _send_serverchan(lines: list[str]) -> bool:
    """通过 ServerChan 推送"""
    import urllib.parse
    import urllib.request
    from src.utils.config_helper import get_config

    try:
        cfg = get_config("notify")
        sendkey = cfg.get("sendkey", "")
        if not sendkey or sendkey.startswith("${"):
            print("[notify] no sendkey configured, skip")
            return False

        title = lines[0].replace("## ", "")
        content = "\n".join(lines)
        url = f"https://sctapi.ftqq.com/{sendkey}.send"
        data = urllib.parse.urlencode({
            "title": title[:32],
            "desp": content,
        }).encode()
        urllib.request.urlopen(url, data=data, timeout=10)
        return True
    except Exception as e:
        print(f"[notify] ServerChan failed: {e}")
        return False
