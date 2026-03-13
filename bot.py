import requests
import time
import os
from datetime import datetime

BOT_TOKEN = os.environ.get('BOT_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')

BITGET_SYMBOLS_URL = "https://api.bitget.com/api/v2/spot/public/symbols"
PUMPFUN_URL = "https://frontend-api.pump.fun/coins?offset=0&limit=50&sort=created_timestamp&order=DESC&includeNsfw=false"

known_cex_symbols = set()
known_dex_tokens = set()


def send_telegram(message):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id": CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }, timeout=10)
    except Exception as e:
        print(f"Telegram送信エラー: {e}")


def get_cex_symbols():
    try:
        response = requests.get(BITGET_SYMBOLS_URL, timeout=10)
        data = response.json()
        if data.get('code') == '00000':
            return {
                item['symbol']
                for item in data['data']
                if item.get('status') == 'online'
            }
    except Exception as e:
        print(f"Bitget APIエラー: {e}")
    return set()


def check_cex_listings():
    global known_cex_symbols
    current = get_cex_symbols()
    if not known_cex_symbols:
        known_cex_symbols = current
        print(f"[CEX] 初期化完了: {len(known_cex_symbols)}ペア監視中")
        return
    for symbol in current - known_cex_symbols:
        if symbol.endswith('USDT'):
            base = symbol.replace('USDT', '')
            msg = (
                f"🏦 <b>[CEX] Bitget新規上場！</b>\n\n"
                f"トークン: <b>${base}</b>\n"
                f"時刻: {datetime.now().strftime('%H:%M:%S')}\n\n"
                f"✅ Bitget審査済み（比較的安全）\n"
                f"🔗 https://www.bitget.com/spot/{base}USDT_SPBL"
            )
            send_telegram(msg)
            print(f"[CEX新規] {symbol}")
    known_cex_symbols = current


def check_pumpfun():
    global known_dex_tokens
    try:
        response = requests.get(PUMPFUN_URL, timeout=10)
        coins = response.json()
        if not isinstance(coins, list):
            return

        if not known_dex_tokens:
            for coin in coins:
                mint = coin.get('mint', '')
                if mint:
                    known_dex_tokens.add(mint)
            print(f"[DEX] 初期化完了: {len(known_dex_tokens)}トークン記憶")
            return

        for coin in coins:
            mint = coin.get('mint', '')
            if not mint or mint in known_dex_tokens:
                continue
            known_dex_tokens.add(mint)

            market_cap = coin.get('usd_market_cap', 0) or 0
            name = coin.get('name', '?')
            symbol = coin.get('symbol', '?')
            replies = coin.get('reply_count', 0) or 0

            if market_cap >= 20000 and replies >= 3:
                msg = (
                    f"🚀 <b>[DEX/Solana] 新規ミームコイン！</b>\n\n"
                    f"名前: <b>{name} (${symbol})</b>\n"
                    f"時価総額: ${market_cap:,.0f}\n"
                    f"コメント数: {replies}件\n"
                    f"時刻: {datetime.now().strftime('%H:%M:%S')}\n\n"
                    f"⚠️ 高リスク・高リターン\n"
                    f"🔗 https://pump.fun/{mint}"
                )
                send_telegram(msg)
                print(f"[DEX新規] {name} ${symbol} MC=${market_cap:,.0f}")

    except Exception as e:
        print(f"Pump.funエラー: {e}")


def main():
    print("通知ボットくん 起動中...")
    send_telegram(
        "✅ <b>通知ボットくん 起動しました！</b>\n\n"
        "📊 監視対象：\n"
        "🏦 CEX: Bitget取引所（60秒ごと）\n"
        "🚀 DEX: Pump.fun Solanaミームコイン（30秒ごと）\n\n"
        "新規トークンを検知したらすぐに通知します！"
    )

    loop = 0
    while True:
        check_cex_listings()
        check_pumpfun()
        time.sleep(30)
        check_pumpfun()
        time.sleep(30)
        loop += 1
        if loop % 20 == 0:
            print(f"[{datetime.now().strftime('%H:%M')}] 稼働中 CEX={len(known_cex_symbols)} DEX={len(known_dex_tokens)}")


if __name__ == "__main__":
    main()
