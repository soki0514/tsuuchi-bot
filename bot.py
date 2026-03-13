import requests
import time
import os
from datetime import datetime

BOT_TOKEN = os.environ.get('BOT_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')

BITGET_SYMBOLS_URL = "https://api.bitget.com/api/v2/spot/public/symbols"

known_symbols = set()


def send_telegram(message):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id": CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }, timeout=10)
    except Exception as e:
        print(f"Telegram送信エラー: {e}")


def get_symbols():
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


def check_trust_score(symbol):
    """
    簡易的な信頼スコア判定
    USDTペアのみ対象（流動性が高い）
    """
    if not symbol.endswith('USDT'):
        return False, "USDTペアではない"

    # 既知の怪しいパターンを除外（例：テストトークン）
    suspicious_keywords = ['TEST', 'SCAM', 'FAKE', 'DEMO']
    base = symbol.replace('USDT', '')
    for kw in suspicious_keywords:
        if kw in base.upper():
            return False, f"怪しいキーワード検出: {kw}"

    return True, "基本チェック通過"


def check_new_listings():
    global known_symbols

    current_symbols = get_symbols()

    if not known_symbols:
        known_symbols = current_symbols
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 初期化完了: {len(known_symbols)}ペアを監視中")
        return

    new_symbols = current_symbols - known_symbols

    for symbol in new_symbols:
        passed, reason = check_trust_score(symbol)

        if passed:
            message = (
                f"🚨 <b>新規上場検知！</b>\n\n"
                f"トークン: <b>{symbol}</b>\n"
                f"時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                f"✅ {reason}\n\n"
                f"⚡ Bitget Walletで確認して、30分以内に利確を検討！"
            )
            send_telegram(message)
            print(f"[新規上場] {symbol} → 通知送信")
        else:
            print(f"[スキップ] {symbol} → {reason}")

    known_symbols = current_symbols


def main():
    print("通知ボットくん 起動中...")
    send_telegram(
        "✅ <b>通知ボットくん 起動しました！</b>\n\n"
        "Bitgetの新規上場トークンを60秒ごとに監視しています。\n"
        "新規USDTペアが上場したらすぐに通知します！"
    )

    while True:
        check_new_listings()
        time.sleep(60)  # 60秒ごとにチェック


if __name__ == "__main__":
    main()
