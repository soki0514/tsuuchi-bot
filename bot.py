import requests
import time
import os
from datetime import datetime

# 環境変数の読み込み
BOT_TOKEN = os.environ.get('BOT_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')

BITGET_SYMBOLS_URL = "https://api.bitget.com/api/v2/spot/public/symbols"
DEXSCREENER_PROFILES_URL = "https://api.dexscreener.com/token-profiles/latest/v1"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json"
}

known_cex_symbols = set()
known_dex_tokens = set()

def send_telegram(message):
    if not BOT_TOKEN or not CHAT_ID:
        print("⚠️ Telegramの設定(BOT_TOKEN/CHAT_ID)が空です。")
        return
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": False # プレビューで見やすくする
        }, timeout=10)
        if r.status_code != 200:
            print(f"Telegram送信エラー: {r.status_code} - {r.text}")
        else:
            print(f"Telegram送信成功！")
    except Exception as e:
        print(f"Telegram接続エラー: {e}")

def get_cex_symbols():
    try:
        response = requests.get(BITGET_SYMBOLS_URL, headers=HEADERS, timeout=10)
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

def check_dexscreener():
    global known_dex_tokens
    try:
        response = requests.get(DEXSCREENER_PROFILES_URL, headers=HEADERS, timeout=15)
        if response.status_code == 429:
            print(f"[DEX] 速度制限(429)中... 少し休みます")
            time.sleep(10)
            return
        elif response.status_code != 200:
            print(f"[DEX] HTTPエラー: {response.status_code}")
            return

        tokens = response.json()
        if not isinstance(tokens, list):
            return

        if not known_dex_tokens:
            for token in tokens:
                addr = token.get('tokenAddress', '')
                if addr:
                    known_dex_tokens.add(addr)
            print(f"[DEX] 初期化完了: {len(known_dex_tokens)}トークン記憶")
            return

        for token in tokens:
            addr = token.get('tokenAddress', '')
            if not addr or addr in known_dex_tokens:
                continue

            known_dex_tokens.add(addr)
            chain = token.get('chainId', '?')

            if chain == 'solana':
                trade_url = f"https://pump.fun/{addr}"
                chain_label = "Solana"
            elif chain == 'bsc':
                trade_url = f"https://pancakeswap.finance/swap?outputCurrency={addr}"
                chain_label = "BSC"
            else:
                trade_url = f"https://dexscreener.com/{chain}/{addr}"
                chain_label = chain.upper()

            # ★ 修正ポイント：addr[:20] を addr に変更（フル表示）
            msg = (
                f"🚀 <b>[DEX/{chain_label}] 新規トークン検知！</b>\n\n"
                f"チェーン: {chain_label}\n"
                f"時刻: {datetime.now().strftime('%H:%M:%S')}\n"
                f"アドレス: <code>{addr}</code>\n\n"
                f"⚠️ 高リスク・高リターン（詐欺注意）\n"
                f"📊 https://dexscreener.com/{chain}/{addr}\n"
                f"🔗 {trade_url}"
            )
            send_telegram(msg)
            print(f"[DEX新規] chain={chain} addr={addr}")

    except Exception as e:
        print(f"DexScreenerエラー: {e}")

def main():
    print("通知ボットくん 起動中...")
    send_telegram("✅ <b>通知ボットくん 起動しました！</b>")

    loop = 0
    while True:
        check_cex_listings()
        check_dexscreener()
        # 429エラーを避けるため、チェック間隔を少し長め(90秒)にするのがおすすめ
        time.sleep(90)
        loop += 1
        if loop % 10 == 0:
            print(f"[{datetime.now().strftime('%H:%M')}] 稼働中 CEX={len(known_cex_symbols)} DEX={len(known_dex_
