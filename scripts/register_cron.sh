#!/usr/bin/env bash
# 注册 ETF_data 定时任务（通过 Hermes cron）
# 用法: bash scripts/register_cron.sh
#
# 注意: 任务已被取消，如需重新注册：
#   hermes cron create --name etf-data-daily --schedule "30 15 * * 1-5" \
#     --prompt "cd /home/fangyao_xu/ETF_data && python src/scheduler/daily_update.py --notify"

echo "ETF_data 定时任务管理"
echo ""
echo "可选调度:"
echo "  每日增量:      hermes cron create --name etf-data-daily \\"
echo "                 --schedule '30 15 * * 1-5' \\"
echo "                 --prompt 'cd /home/fangyao_xu/ETF_data && python src/scheduler/daily_update.py'"
echo ""
echo "  每周对账:      hermes cron create --name etf-data-weekly-check \\"
echo "                 --schedule '0 10 * * 0' \\"
echo "                 --prompt 'cd /home/fangyao_xu/ETF_data && python src/scheduler/backfill.py --notify'"
