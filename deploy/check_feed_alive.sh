#!/usr/bin/env bash
# =============================================================================
# txf-producer 活性檢查(2026-07-21 建立)
#
# 為什麼需要它:systemd 只知道「行程在不在」。producer 可能**連得上但不再送 tick**
# ——Shioaji 斷線重連失敗、迴圈卡死、訂閱悄悄掉了——此時行程活著、
# `systemctl status` 顯示 active,**systemd 永遠不會發現**。
# 這正是本 workspace 一直在對付的「靜默失效」,而生產端最關鍵的那支原本沒有這道防線。
#
# 做法:檢查 topic 的最新 offset 在 N 秒後是否前進。沒前進 = 沒有新資料進來。
#
# ⚠️ 只在「應該有行情」的時段檢查。TXF 交易時段(台北時間):
#      日盤 08:45–13:45、夜盤 15:00–翌日 05:00,週一~週五。
#    其餘時間沒有 tick 是**正常**的,不可告警,否則會天天誤報而讓人忽略真警報。
#
# 用法:
#   ./check_feed_alive.sh              # 檢查,有問題回傳非 0
#   ./check_feed_alive.sh --restart    # 檢查失敗時自動重啟 producer(需 sudo 權限)
#
# 建議掛法:systemd timer 每 5 分鐘跑一次(見本檔尾端的 unit 範例)。
# =============================================================================
set -uo pipefail

BROKER="${KAFKA_BROKER:-localhost:9092}"
TOPIC="${CHECK_TOPIC:-txf-tick}"
WAIT_SEC="${WAIT_SEC:-45}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
AUTO_RESTART=0
[ "${1:-}" = "--restart" ] && AUTO_RESTART=1

log() { echo "$(date '+%F %T') | $*"; }

# --- 只在交易時段檢查 -------------------------------------------------------
dow=$(date +%u)          # 1=Mon .. 7=Sun
hm=$(date +%H%M)
in_session() {
    # 週六 05:00 之後到週日全天:完全休市
    [ "$dow" -eq 7 ] && return 1
    [ "$dow" -eq 6 ] && [ "$hm" -ge 0500 ] && return 1
    # 週一 08:45 之前:上一個夜盤已於週六 05:00 收
    [ "$dow" -eq 1 ] && [ "$hm" -lt 0845 ] && return 1
    # 每日 05:00–08:45(夜盤收到日盤開)與 13:45–15:00(盤間)無行情
    [ "$hm" -ge 0500 ] && [ "$hm" -lt 0845 ] && return 1
    [ "$hm" -ge 1345 ] && [ "$hm" -lt 1500 ] && return 1
    return 0
}
if ! in_session; then
    log "非交易時段(週$dow $hm),跳過檢查"
    exit 0
fi

# --- 取兩次 offset,看有沒有前進 --------------------------------------------
get_offset() {
    "$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BROKER" \
        --topic "$TOPIC" --time -1 2>/dev/null | awk -F: '{s+=$3} END{print s+0}'
}

a=$(get_offset)
if [ -z "$a" ] || [ "$a" = "0" ]; then
    log "❌ 取不到 $TOPIC 的 offset(broker 不通?)"
    exit 2
fi
sleep "$WAIT_SEC"
b=$(get_offset)
delta=$((b - a))

if [ "$delta" -gt 0 ]; then
    log "✅ $TOPIC 正常:${WAIT_SEC}s 內新增 $delta 則"
    exit 0
fi

# --- 沒前進 = 有問題 --------------------------------------------------------
log "🚨 $TOPIC 在 ${WAIT_SEC}s 內**零新增**(offset 停在 $a)——"
log "   行程狀態=$(systemctl is-active txf-producer 2>/dev/null),但沒有新資料進來。"
log "   這是 systemd 抓不到的靜默失效。"

if [ "$AUTO_RESTART" = "1" ]; then
    log "   --restart 已指定 → 重啟 txf-producer"
    systemctl restart txf-producer && log "   已重啟" || log "   ❌ 重啟失敗(權限?)"
fi
exit 1

# =============================================================================
# systemd timer 範例(每 5 分鐘檢查一次):
#
#   /etc/systemd/system/txf-feed-check.service
#     [Unit]
#     Description=TXF feed liveness check
#     [Service]
#     Type=oneshot
#     ExecStart=/home/shioaji_svc/txf-streaming-server/deploy/check_feed_alive.sh
#
#   /etc/systemd/system/txf-feed-check.timer
#     [Unit]
#     Description=Run TXF feed check every 5 min
#     [Timer]
#     OnBootSec=5min
#     OnUnitActiveSec=5min
#     [Install]
#     WantedBy=timers.target
#
#   sudo systemctl daemon-reload && sudo systemctl enable --now txf-feed-check.timer
#   journalctl -u txf-feed-check -f        # 看結果
#
# ⚠️ 要自動重啟的話,ExecStart 加 --restart,且該 service 需以 root 執行
#    (或給 shioaji_svc 一條 sudoers 白名單:僅允許 systemctl restart txf-producer)。
#    **建議先只告警、觀察數日確認無誤報,再開自動重啟** —— 誤判造成的重啟會製造
#    bidask 破洞(bidask 只存在 Kafka,Shioaji 無歷史 API,斷幾秒就永久少幾秒)。
# =============================================================================
