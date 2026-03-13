import requests
import time
import os
from datetime import datetime
from collections import Counter

BOT_TOKEN = os.environ.get('BOT_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')
HELIUS_KEY = os.environ.get('HELIUS_API_KEY', '')

BITGET_SYMBOLS_URL = "https://api.bitget.com/api/v2/spot/public/symbols"
SOLANA_RPC = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}"

PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json"
}

known_cex_symbols = set()
known_token_mints = set()
last_signature = None


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


def solana_rpc(method, params):
    try:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params
        }
        r = requests.post(SOLANA_RPC, json=payload, timeout=15)
        if r.status_code == 200:
            data = r.json()
            if data.get('error'):
                print(f"[RPC Error] {method}: {data['error']}")
                return None
            return data.get('result')
        else:
            print(f"[RPC HTTPエラー] {method}: {r.status_code}")
    except Exception as e:
        print(f"Solana RPCエラー ({method}): {e}")
    return None


def get_new_pumpfun_transactions():
    global last_signature
    try:
        params = [PUMPFUN_PROGRAM, {"limit": 20, "commitment": "confirmed"}]
        if last_signature:
            params[1]["until"] = last_signature

        result = solana_rpc("getSignaturesForAddress", params)
        if not result:
            return []

        if result:
            last_signature = result[0].get('signature', '')

        return result
    except Exception as e:
        print(f"トランザクション取得エラー: {e}")
        return []


def parse_new_token(signature):
    try:
        result = None
        for attempt in range(3):
            result = solana_rpc("getTransaction", [
                signature,
                {
                    "encoding": "jsonParsed",
                    "maxSupportedTransactionVersion": 0,
                    "commitment": "confirmed"
                }
            ])
            if result:
                break
            time.sleep(0.5)

        if not result:
            return None

        post_balances = result.get('meta', {}).get('postTokenBalances', [])
        pre_balances = result.get('meta', {}).get('preTokenBalances', [])
        pre_mints = {b.get('mint') for b in pre_balances}

        for balance in post_balances:
            mint = balance.get('mint', '')
            if mint and mint not in pre_mints:
                print(f"[新規mint発見] {mint[:20]}")
                return mint

    except Exception as e:
        print(f"トークン解析エラー: {e}")
    return None


def wait_for_first_trade(mint, max_attempts=30):
    """最初の取引が来るまで待つ（5秒×30回 = 最大2.5分）"""
    print(f"[Pump.fun] 初回取引を待機中... {mint[:20]}")
    for attempt in range(max_attempts):
        time.sleep(5)
        sigs = solana_rpc("getSignaturesForAddress", [
            mint, {"limit": 3, "commitment": "confirmed"}
        ])
        if sigs and len(sigs) >= 2:
            # 作成TX + 取引TX で2件以上 = 誰かが取引した
            print(f"[Pump.fun] 初回取引検知！ {mint[:20]} (取引数:{len(sigs)})")
            return True
        if attempt % 6 == 5:
            print(f"[Pump.fun] 待機中... {attempt+1}/30 {mint[:20]}")
    print(f"[Pump.fun] タイムアウト（2.5分以内に取引なし）: {mint[:20]}")
    return False


def analyze_wallets(token_address):
    try:
        sigs_result = solana_rpc("getSignaturesForAddress", [
            token_address, {"limit": 50}
        ])
        if not sigs_result:
            return None

        wallets = []
        for sig_info in sigs_result[:30]:
            sig = sig_info.get('signature', '')
            if not sig:
                continue
            time.sleep(0.15)
            tx = solana_rpc("getTransaction", [
                sig,
                {"encoding": "json", "maxSupportedTransactionVersion": 0}
            ])
            if not tx:
                continue
            account_keys = tx.get('transaction', {}).get('message', {}).get('accountKeys', [])
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
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            return None
        data = response.json()
        pairs = data.get('pairs', [])
        if not pairs:
            return None
        pair = max(pairs, key=lambda p: (p.get('liquidity') or {}).get('usd', 0))
        return {
            'liquidity': (pair.get('liquidity') or {}).get('usd', 0) or 0,
            'buys_5m': (pair.get('txns') or {}).get('m5', {}).get('buys', 0) or 0,
            'sells_5m': (pair.get('txns') or {}).get('m5', {}).get('sells', 0) or 0,
            'price_change_5m': (pair.get('priceChange') or {}).get('m5', 0) or 0,
        }
    except Exception as e:
        print(f"DexScreener分析エラー: {e}")
        return None


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


def check_pumpfun_onchain():
    global known_token_mints

    txns = get_new_pumpfun_transactions()
    if not txns:
        return

    for tx_info in txns:
        sig = tx_info.get('signature', '')
        if not sig:
            continue

        if tx_info.get('err'):
            continue

        time.sleep(0.2)
        mint = parse_new_token(sig)
        if not mint or mint in known_token_mints:
            continue

        known_token_mints.add(mint)
        print(f"[Pump.fun新規] mint={mint[:20]}")

        # 誰かが最初の取引をするまで待つ
        traded = wait_for_first_trade(mint)
        if not traded:
            continue  # 2.5分待っても取引なし → スキップ

        # 取引が来た！30秒待ってデータを取得
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
            f"🚀 <b>[Pump.fun] 新規トークン 初回取引検知！</b>\n\n"
            f"時刻: {datetime.now().strftime('%H:%M:%S')}\n"
            f"Mint: <code>{mint}</code>\n\n"
            f"{dex_text}\n"
            f"{wallet_text}\n"
            f"{wallet_judge}\n\n"
            f"📊 https://dexscreener.com/solana/{mint}\n"
            f"🔗 https://pump.fun/{mint}"
        )
        send_telegram(msg)
        print(f"[Pump.fun通知] mint={mint[:20]}")


def main():
    print("通知ボットくん 起動中...")
    send_telegram(
        "✅ <b>通知ボットくん 起動しました！</b>\n\n"
        "📊 監視対象：\n"
        "🏦 CEX: Bitget取引所（新規上場）\n"
        "🚀 DEX: Solanaブロックチェーン直接監視\n\n"
        "🔍 通知タイミング：\n"
        "・新規トークン作成後、最初の取引が来た瞬間"
    )

    print("[Pump.fun] 初期化中...")
    init_sigs = solana_rpc("getSignaturesForAddress", [
        PUMPFUN_PROGRAM, {"limit": 5}
    ])
    if init_sigs:
        global last_signature
        last_signature = init_sigs[0].get('signature', '')
        print(f"[Pump.fun] 初期化完了 最新sig={last_signature[:20]}")
    else:
        print("[Pump.fun] 初期化失敗 - RPC接続を確認してください")

    loop = 0
    while True:
        check_cex_listings()
        check_pumpfun_onchain()
        time.sleep(20)
        loop += 1
        if loop % 30 == 0:
            print(f"[{datetime.now().strftime('%H:%M')}] 稼働中 CEX={len(known_cex_symbols)} 検知済み={len(known_token_mints)}")


if __name__ == "__main__":
    main()
