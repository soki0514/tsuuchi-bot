import requests
import time
import os
from datetime import datetime
from collections import Counter

BOT_TOKEN = os.environ.get('BOT_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')

BITGET_SYMBOLS_URL = "https://api.bitget.com/api/v2/spot/public/symbols"
PUMPFUN_URL = "https://frontend-api.pump.fun/coins?offset=0&limit=50&sort=created_timestamp&order=DESC&includeNsfw=false"
SOLANA_RPC = "https://api.mainnet-beta.solana.com"

PUMPFUN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://pump.fun/",
    "Origin": "https://pump.fun",
}

DEXSCREENER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json"
}

known_cex_symbols = set()
known_pumpfun_tokens = set()


def send_telegram(message):
    if not BOT_TOKEN or not CHAT_ID:
        print("BOT_TOKEN/CHAT_IDが空です")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }, timeout=10)
        if r.status_code != 200:
            print(f"Telegram送信エラー: {r.status_code} - {r.text}")
        else:
            print("Telegram送信成功！")
    except Exception as e:
        print(f"Telegram接続エラー: {e}")


def analyze_wallets(token_address):
    try:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [token_address, {"limit": 50}]
        }
        r = requests.post(SOLANA_RPC, json=payload, timeout=10)
        if r.status_code != 200:
            return None

        sigs = r.json().get('result', [])
        if not sigs:
            return None

        wallets = []
        for sig_info in sigs[:30]:
            sig = sig_info.get('signature', '')
            if not sig:
                continue
            tx_payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTransaction",
                "params": [sig, {"encoding": "json", "maxSupportedTransactionVersion": 0}]
            }
            tx_r = requests.post(SOLANA_RPC, json=tx_payload, timeout=8)
            if tx_r.status_code != 200:
                continue
            tx_data = tx_r.json().get('result')
            if not tx_data:
                continue
            account_keys = tx_data.get('transaction', {}).get('message', {}).get('accountKeys', [])
            if account_keys:
                wallets.append(account_keys[0])

        if not wallets:
            return None

        total = len(wallets)
        unique = len(set(wallets))
        counter = Counter(wallets)
        top3 = counter.most_common(3)
        top3_count = sum(c for _, c in top3)
        top3_ratio = (top3_count / total * 100) if total > 0 else 0
        max_single = top3[0][1] if top3 else 0
        max_single_ratio = (max_single / total * 100) if total > 0 else 0

        top5 = counter.most_common(5)
        top5_detail = []
        top5_total_count = 0
        for i, (addr, count) in enumerate(top5):
            ratio = count / total * 100
            short_addr = addr[:6] + "..." + addr[-4:]
            top5_detail.append(f"  {'ABCDE'[i]}. {short_addr}: {ratio:.1f}% ({count}件)")
            top5_total_count += count

        others_count = total - top5_total_count
        others_unique = unique - min(len(top5), unique)
        if others_count > 0:
            others_ratio = others_count / total * 100
            top5_detail.append(f"  その他: {others_ratio:.1f}% ({others_count}件 / {others_unique}人)")

        return {
            'total_txns': total,
            'unique_wallets': unique,
            'top3_ratio': top3_ratio,
            'max_single_ratio': max_single_ratio,
            'top5_detail': top5_detail,
        }
    except Exception as e:
        print(f"ウォレット分析エラー: {e}")
        return None


def analyze_dexscreener(token_address):
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
        response = requests.get(url, headers=DEXSCREENER_HEADERS, timeout=10)
        if response.status_code != 200:
            return None
        data = response.json()
        pairs = data.get('pairs', [])
        if not pairs:
            return None
        pair = max(pairs, key=lambda p: (p.get('liquidity') or {}).get('usd', 0))
        liquidity = (pair.get('liquidity') or {}).get('usd', 0) or 0
        buys_5m = (pair.get('txns') or {}).get('m5', {}).get('buys', 0) or 0
        sells_5m = (pair.get('txns') or {}).get('m5', {}).get('sells', 0) or 0
        price_change_5m = (pair.get('priceChange') or {}).get('m5', 0) or 0
        return {
            'liquidity': liquidity,
            'buys_5m': buys_5m,
            'sells_5m': sells_5m,
            'price_change_5m': price_change_5m,
        }
    except Exception as e:
        print(f"DexScreener分析エラー: {e}")
        return None


def get_cex_symbols():
    try:
        response = requests.get(BITGET_SYMBOLS_URL, headers=DEXSCREENER_HEADERS, timeout=10)
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
    global known_pumpfun_tokens
    try:
        response = requests.get(PUMPFUN_URL, headers=PUMPFUN_HEADERS, timeout=15)
        if response.status_code != 200:
            print(f"[Pump.fun] HTTPエラー: {response.status_code}")
            return

        coins = response.json()
        if not isinstance(coins, list):
            print(f"[Pump.fun] 予期しないレスポンス形式")
            return

        if not known_pumpfun_tokens:
            for coin in coins:
                mint = coin.get('mint', '')
                if mint:
                    known_pumpfun_tokens.add(mint)
            print(f"[Pump.fun] 初期化完了: {len(known_pumpfun_tokens)}トークン記憶")
            return

        for coin in coins:
            mint = coin.get('mint', '')
            if not mint or mint in known_pumpfun_tokens:
                continue
            known_pumpfun_tokens.add(mint)

            name = coin.get('name', '?')
            symbol = coin.get('symbol', '?')
            market_cap = coin.get('usd_market_cap', 0) or 0
            replies = coin.get('reply_count', 0) or 0
            creator = coin.get('creator', '')
            twitter = coin.get('twitter', '')
            telegram = coin.get('telegram', '')
            website = coin.get('website', '')

            sns_lines = []
            if twitter:
                sns_lines.append(f"🐦 Twitter: {twitter}")
            if telegram:
                sns_lines.append(f"💬 Telegram: {telegram}")
            if website:
                sns_lines.append(f"🌐 Web: {website}")
            sns_text = "\n".join(sns_lines) if sns_lines else "⚪ SNSなし"

            creator_short = creator[:6] + "..." + creator[-4:] if creator else "不明"

            print(f"[Pump.fun新規] {name} ${symbol} MC=${market_cap:,.0f} 分析開始...")
            time.sleep(30)

            dex = analyze_dexscreener(mint)
            print(f"[Pump.fun] ウォレット分析中... {mint[:20]}")
            wallet_data = analyze_wallets(mint)

            if dex:
                dex_text = (
                    f"💧 流動性: ${dex['liquidity']:,.0f}\n"
                    f"📈 価格変動: {dex['price_change_5m']:+.1f}%/5分\n"
                    f"🛒 買い{dex['buys_5m']}件 / 売り{dex['sells_5m']}件 (5分)\n"
                )
            else:
                dex_text = "📊 価格データ取得中...\n"

            if wallet_data:
                top5_lines = "\n".join(wallet_data['top5_detail'])
                wallet_text = (
                    f"👛 <b>ウォレット分析</b> (直近{wallet_data['total_txns']}取引)\n"
                    f"ユニーク: {wallet_data['unique_wallets']}人 / 上位3人合計: {wallet_data['top3_ratio']:.0f}%\n"
                    f"{top5_lines}\n"
                )
                if wallet_data['max_single_ratio'] >= 50:
                    wallet_judge = "🚨 自作自演の疑い強い"
                elif wallet_data['top3_ratio'] < 30 and wallet_data['unique_wallets'] >= 15:
                    wallet_judge = "✅ 多様なウォレット"
                else:
                    wallet_judge = "🟡 やや集中気味"
            else:
                wallet_text = "👛 ウォレットデータ取得中...\n"
                wallet_judge = ""

            msg = (
                f"🚀 <b>[Pump.fun] 新規トークン検知！</b>\n\n"
                f"名前: <b>{name} (${symbol})</b>\n"
                f"時刻: {datetime.now().strftime('%H:%M:%S')}\n"
                f"時価総額: ${market_cap:,.0f}\n"
                f"コメント: {replies}件\n"
                f"作成者: <code>{creator_short}</code>\n\n"
                f"{dex_text}\n"
                f"{wallet_text}\n"
                f"{wallet_judge}\n\n"
                f"{sns_text}\n\n"
                f"📊 https://dexscreener.com/solana/{mint}\n"
                f"🔗 https://pump.fun/{mint}"
            )
            send_telegram(msg)
            print(f"[Pump.fun通知] {name} ${symbol} MC=${market_cap:,.0f}")

    except Exception as e:
        print(f"Pump.funエラー: {e}")


def main():
    print("通知ボットくん 起動中...")
    send_telegram(
        "✅ <b>通知ボットくん 起動しました！</b>\n\n"
        "📊 監視対象：\n"
        "🏦 CEX: Bitget取引所（新規上場）\n"
        "🚀 DEX: Pump.fun直接監視（30秒ごと）\n\n"
        "🔍 分析内容：\n"
        "・流動性・買い/売り比率\n"
        "・ウォレット多様性（自作自演チェック）\n"
        "・SNS情報・作成者アドレス"
    )

    loop = 0
    while True:
        check_cex_listings()
        check_pumpfun()
        time.sleep(30)
        loop += 1
        if loop % 20 == 0:
            print(f"[{datetime.now().strftime('%H:%M')}] 稼働中 CEX={len(known_cex_symbols)} Pump.fun={len(known_pumpfun_tokens)}")


if __name__ == "__main__":
    main()
