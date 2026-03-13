import requests
import time
import os
from datetime import datetime
from collections import Counter

BOT_TOKEN = os.environ.get('BOT_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')

BITGET_SYMBOLS_URL = "https://api.bitget.com/api/v2/spot/public/symbols"
DEXSCREENER_PROFILES_URL = "https://api.dexscreener.com/token-profiles/latest/v1"
SOLANA_RPC = "https://api.mainnet-beta.solana.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json"
}

known_cex_symbols = set()
known_dex_tokens = set()


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


def analyze_token(token_address):
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
        response = requests.get(url, headers=HEADERS, timeout=10)
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


def score_token(analysis, wallet_data):
    score = 0
    signals = []

    if analysis:
        liq = analysis['liquidity']
        if liq >= 50000:
            score += 25
            signals.append("✅ 流動性十分 ($50K+)")
        elif liq >= 20000:
            score += 15
            signals.append("🟡 流動性やや低め ($20K+)")
        elif liq >= 10000:
            score += 8
            signals.append("⚠️ 流動性低い ($10K+)")
        else:
            signals.append("🔴 流動性危険 ($10K未満)")

        buys = analysis['buys_5m']
        sells = analysis['sells_5m']
        if buys >= 30:
            score += 20
            signals.append(f"✅ 買い活発 ({buys}件/5分)")
        elif buys >= 10:
            score += 12
            signals.append(f"🟡 買いまあまあ ({buys}件/5分)")
        else:
            signals.append(f"🔴 買い少ない ({buys}件/5分)")

        total = buys + sells
        if total > 0:
            buy_ratio = buys / total
            if buy_ratio >= 0.7:
                score += 15
                signals.append(f"✅ 買い優勢 ({int(buy_ratio*100)}%が買い)")
            elif buy_ratio >= 0.5:
                score += 8
                signals.append(f"🟡 買い/売りほぼ同等")
            else:
                signals.append(f"🔴 売り優勢（逃げている人多い）")

        pc = analysis['price_change_5m']
        if pc > 20:
            score += 15
            signals.append(f"✅ 急上昇中 (+{pc:.1f}%/5分)")
        elif pc > 0:
            score += 8
            signals.append(f"🟡 上昇中 (+{pc:.1f}%/5分)")
        else:
            signals.append(f"🔴 下落中 ({pc:.1f}%/5分)")

    if wallet_data:
        unique = wallet_data['unique_wallets']
        top3_ratio = wallet_data['top3_ratio']
        max_ratio = wallet_data['max_single_ratio']

        if unique >= 20 and top3_ratio < 30:
            score += 25
            signals.append(f"✅ 多様なウォレット (ユニーク{unique}人、上位3人={top3_ratio:.0f}%)")
        elif unique >= 10 and top3_ratio < 50:
            score += 15
            signals.append(f"🟡 まあまあ多様 (ユニーク{unique}人、上位3人={top3_ratio:.0f}%)")
        else:
            signals.append(f"🔴 自作自演の疑い (ユニーク{unique}人、上位3人={top3_ratio:.0f}%)")

        if max_ratio >= 50:
            signals.append(f"🚨 1つのウォレットが{max_ratio:.0f}%の取引 → ほぼ確実に自作自演")
    else:
        signals.append("⚪ ウォレット分析データなし")

    return min(score, 100), signals


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
            print("[DEX] 速度制限(429)... 休憩中")
            time.sleep(30)
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

            print(f"[DEX] 新規検知 → 30秒後に分析開始 addr={addr[:20]}")
            time.sleep(30)

            analysis = analyze_token(addr)

            wallet_data = None
            if chain == 'solana':
                print(f"[DEX] ウォレット分析中... addr={addr[:20]}")
                wallet_data = analyze_wallets(addr)

            score, signals = score_token(analysis, wallet_data)

            signals_text = "\n".join(signals) if signals else "データなし"
            score_emoji = "🟢" if score >= 60 else "🟡" if score >= 35 else "🔴"

            if analysis:
                stats = (
                    f"💧 流動性: ${analysis['liquidity']:,.0f}\n"
                    f"📈 価格変動: {analysis['price_change_5m']:+.1f}%/5分\n"
                    f"🛒 買い{analysis['buys_5m']}件 / 売り{analysis['sells_5m']}件 (5分)\n"
                )
            else:
                stats = "📊 データ取得中...\n"

            if wallet_data:
                top5_lines = "\n".join(wallet_data['top5_detail'])
                wallet_text = (
                    f"👛 <b>ウォレット分析</b> (直近{wallet_data['total_txns']}取引)\n"
                    f"ユニーク: {wallet_data['unique_wallets']}人 / 上位3人合計: {wallet_data['top3_ratio']:.0f}%\n"
                    f"{top5_lines}\n"
                )
            else:
                wallet_text = ""

            msg = (
                f"🚀 <b>[DEX/{chain_label}] 新規トークン検知！</b>\n\n"
                f"時刻: {datetime.now().strftime('%H:%M:%S')}\n"
                f"アドレス: <code>{addr}</code>\n\n"
                f"{score_emoji} <b>安全スコア: {score}/100</b>\n\n"
                f"{stats}"
                f"{wallet_text}\n"
                f"<b>📋 分析:</b>\n{signals_text}\n\n"
                f"📊 https://dexscreener.com/{chain}/{addr}\n"
                f"🔗 {trade_url}"
            )
            send_telegram(msg)
            print(f"[DEX新規通知] chain={chain} score={score} addr={addr[:20]}")

    except Exception as e:
        print(f"DexScreenerエラー: {e}")


def main():
    print("通知ボットくん 起動中...")
    send_telegram(
        "✅ <b>通知ボットくん 起動しました！</b>\n\n"
        "📊 監視対象：\n"
        "🏦 CEX: Bitget取引所\n"
        "🚀 DEX: DexScreener 全チェーン\n\n"
        "🔍 分析内容：\n"
        "・安全スコア（100点満点）\n"
        "・流動性・買い/売り比率\n"
        "・ウォレット多様性（自作自演チェック）"
    )

    loop = 0
    while True:
        check_cex_listings()
        check_dexscreener()
        time.sleep(60)
        loop += 1
        if loop % 10 == 0:
            print(f"[{datetime.now().strftime('%H:%M')}] 稼働中 CEX={len(known_cex_symbols)} DEX={len(known_dex_tokens)}")


if __name__ == "__main__":
    main()
