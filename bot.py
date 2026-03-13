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

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO_TOPIC     = "0x0000000000000000000000000000000000000000000000000000000000000000"
PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json"
}

EVM_CHAINS = [
    {
        "name": "FourMeme/BSC",
        "emoji": "🟡",
        "rpc": "https://bsc-rpc.publicnode.com",
        "contract": "0x5c952063c7fc8610ffdb798152d69f0b9550762b",
        "dex_url": "https://dexscreener.com/bsc/{}",
        "launch_url": "https://four.meme",
        "known_tokens": set(),
        "last_block": None,
    },
    {
        "name": "Clanker/Base",
        "emoji": "🔵",
        "rpc": "https://base-rpc.publicnode.com",
        "contract": "0xe85a59c628f7d27878aceb4bf3b35733630083a9",
        "dex_url": "https://dexscreener.com/base/{}",
        "launch_url": "https://www.clanker.world",
        "known_tokens": set(),
        "last_block": None,
    },
]

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


def build_wallet_stats(wallets):
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


def format_wallet_output(wallet_data):
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
    return wallet_text, wallet_judge


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


def evm_rpc(rpc_url, method, params):
    try:
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        r = requests.post(rpc_url, json=payload, timeout=15)
        if r.status_code == 200:
            data = r.json()
            if data.get('error'):
                print(f"[EVM RPC Error] {method}: {data['error']}")
                return None
            return data.get('result')
        else:
            print(f"[EVM RPC HTTPエラー] {method}: {r.status_code}")
    except Exception as e:
        print(f"EVM RPCエラー ({method}): {e}")
    return None


def evm_wait_for_first_trade(rpc_url, token_address, chain_name, max_attempts=30):
    print(f"[{chain_name}] 初回取引待機中... {token_address[:20]}")
    for attempt in range(max_attempts):
        time.sleep(5)
        current_hex = evm_rpc(rpc_url, "eth_blockNumber", [])
        if not current_hex:
            continue
        current_int = int(current_hex, 16)
        from_b = current_int - 50

        logs = evm_rpc(rpc_url, "eth_getLogs", [{
            "fromBlock": hex(from_b),
            "toBlock": hex(current_int),
            "address": token_address,
            "topics": [TRANSFER_TOPIC]
        }])

        if logs:
            non_mint = [l for l in logs if l.get('topics', ['', ''])[1] != ZERO_TOPIC]
            if non_mint:
                print(f"[{chain_name}] 初回取引検知！ {token_address[:20]}")
                return True

        if attempt % 6 == 5:
            print(f"[{chain_name}] 待機中... {attempt+1}/30")

    print(f"[{chain_name}] タイムアウト: {token_address[:20]}")
    return False


def evm_analyze_wallets(rpc_url, token_address):
    try:
        current_hex = evm_rpc(rpc_url, "eth_blockNumber", [])
        if not current_hex:
            return None
        current_int = int(current_hex, 16)
        from_b = current_int - 500

        logs = evm_rpc(rpc_url, "eth_getLogs", [{
            "fromBlock": hex(from_b),
            "toBlock": hex(current_int),
            "address": token_address,
            "topics": [TRANSFER_TOPIC]
        }])

        if not logs:
            return None

        wallets = []
        for log in logs:
            topics = log.get('topics', [])
            if len(topics) < 3:
                continue
            if topics[1] == ZERO_TOPIC:
                continue
            to_addr = '0x' + topics[2][-40:]
            wallets.append(to_addr)

        if not wallets:
            return None
        return build_wallet_stats(wallets)
    except Exception as e:
        print(f"EVMウォレット分析エラー: {e}")
        return None


def check_evm_chain(chain):
    rpc_url  = chain["rpc"]
    contract = chain["contract"]
    name     = chain["name"]
    emoji    = chain["emoji"]

    latest_hex = evm_rpc(rpc_url, "eth_blockNumber", [])
    if not latest_hex:
        return

    latest_int = int(latest_hex, 16)

    if chain["last_block"] is None:
        chain["last_block"] = latest_int
        print(f"[{name}] 初期化完了 最新ブロック={latest_int}")
        return

    if latest_int <= chain["last_block"]:
        return

    from_block = max(chain["last_block"] + 1, latest_int - 20)

    logs = evm_rpc(rpc_url, "eth_getLogs", [{
        "fromBlock": hex(from_block),
        "toBlock": hex(latest_int),
        "address": contract
    }])

    chain["last_block"] = latest_int

    if not logs:
        return

    print(f"[{name}] {len(logs)}件のイベント")

    tx_hashes = list({log.get('transactionHash') for log in logs if log.get('transactionHash')})

    for tx_hash in tx_hashes[:10]:
        time.sleep(0.1)
        receipt = evm_rpc(rpc_url, "eth_getTransactionReceipt", [tx_hash])
        if not receipt:
            continue

        for rlog in receipt.get('logs', []):
            topics = rlog.get('topics', [])
            if len(topics) < 3:
                continue

            is_transfer = topics[0].lower() == TRANSFER_TOPIC
            is_from_zero = topics[1] == ZERO_TOPIC

            if is_transfer and is_from_zero:
                token_address = rlog.get('address', '')
                if not token_address:
                    continue
                token_lower = token_address.lower()
                if token_lower in chain["known_tokens"]:
                    continue

                chain["known_tokens"].add(token_lower)
                print(f"[{name}新規] token={token_address}")

                traded = evm_wait_for_first_trade(rpc_url, token_address, name)
                if not traded:
                    continue

                time.sleep(30)

                dex = analyze_dexscreener(token_address)
                wallet_data = evm_analyze_wallets(rpc_url, token_address)
                wallet_text, wallet_judge = format_wallet_output(wallet_data)

                if dex:
                    dex_text = (
                        f"💧 流動性: ${dex['liquidity']:,.0f}\n"
                        f"📈 価格変動: {dex['price_change_5m']:+.1f}%/5分\n"
                        f"🛒 買い{dex['buys_5m']}件 / 売り{dex['sells_5m']}件 (5分)\n"
                    )
                else:
                    dex_text = "📊 価格データ取得中...\n"

                msg = (
                    f"{emoji} <b>[{name}] 新規トークン 初回取引検知！</b>\n\n"
                    f"時刻: {datetime.now().strftime('%H:%M:%S')}\n"
                    f"Contract: <code>{token_address}</code>\n\n"
                    f"{dex_text}\n"
                    f"{wallet_text}\n"
                    f"{wallet_judge}\n\n"
                    f"📊 {chain['dex_url'].format(token_address)}\n"
                    f"🔗 {chain['launch_url']}"
                )
                send_telegram(msg)
                print(f"[{name}通知] token={token_address[:20]}")


def solana_rpc(method, params):
    try:
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
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
                {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0, "commitment": "confirmed"}
            ])
            if result:
                break
            time.sleep(0.5)
        if not result:
            return None
        post_balances = result.get('meta', {}).get('postTokenBalances', [])
        pre_balances  = result.get('meta', {}).get('preTokenBalances', [])
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
    print(f"[Pump.fun] 初回取引待機中... {mint[:20]}")
    for attempt in range(max_attempts):
        time.sleep(5)
        sigs = solana_rpc("getSignaturesForAddress", [mint, {"limit": 3, "commitment": "confirmed"}])
        if sigs and len(sigs) >= 2:
            print(f"[Pump.fun] 初回取引検知！ {mint[:20]}")
            return True
        if attempt % 6 == 5:
            print(f"[Pump.fun] 待機中... {attempt+1}/30 {mint[:20]}")
    print(f"[Pump.fun] タイムアウト: {mint[:20]}")
    return False


def analyze_wallets(token_address):
    try:
        sigs_result = solana_rpc("getSignaturesForAddress", [token_address, {"limit": 50}])
        if not sigs_result:
            return None
        wallets = []
        for sig_info in sigs_result[:30]:
            sig = sig_info.get('signature', '')
            if not sig:
                continue
            time.sleep(0.15)
            tx = solana_rpc("getTransaction", [sig, {"encoding": "json", "maxSupportedTransactionVersion": 0}])
            if not tx:
                continue
            account_keys = tx.get('transaction', {}).get('message', {}).get('accountKeys', [])
            if account_keys:
                wallets.append(account_keys[0])
        if not wallets:
            return None
        return build_wallet_stats(wallets)
    except Exception as e:
        print(f"ウォレット分析エラー: {e}")
        return None


def check_pumpfun_onchain():
    global known_token_mints

    txns = get_new_pumpfun_transactions()
    if not txns:
        return

    for tx_info in txns:
        sig = tx_info.get('signature', '')
        if not sig or tx_info.get('err'):
            continue

        time.sleep(0.2)
        mint = parse_new_token(sig)
        if not mint or mint in known_token_mints:
            continue

        known_token_mints.add(mint)
        print(f"[Pump.fun新規] mint={mint[:20]}")

        traded = wait_for_first_trade(mint)
        if not traded:
            continue

        time.sleep(30)

        dex = analyze_dexscreener(mint)
        wallet_data = analyze_wallets(mint)
        wallet_text, wallet_judge = format_wallet_output(wallet_data)

        if dex:
            dex_text = (
                f"💧 流動性: ${dex['liquidity']:,.0f}\n"
                f"📈 価格変動: {dex['price_change_5m']:+.1f}%/5分\n"
                f"🛒 買い{dex['buys_5m']}件 / 売り{dex['sells_5m']}件 (5分)\n"
            )
        else:
            dex_text = "📊 価格データ取得中...\n"

        msg = (
            f"🚀 <b>[Pump.fun/Solana] 新規トークン 初回取引検知！</b>\n\n"
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


def get_cex_symbols():
    try:
        response = requests.get(BITGET_SYMBOLS_URL, headers=HEADERS, timeout=10)
        data = response.json()
        if data.get('code') == '00000':
            return {item['symbol'] for item in data['data'] if item.get('status') == 'online'}
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
            base_token = symbol.replace('USDT', '')
            msg = (
                f"🏦 <b>[CEX] Bitget新規上場！</b>\n\n"
                f"トークン: <b>${base_token}</b>\n"
                f"時刻: {datetime.now().strftime('%H:%M:%S')}\n\n"
                f"✅ Bitget審査済み（比較的安全）\n"
                f"🔗 https://www.bitget.com/spot/{base_token}USDT_SPBL"
            )
            send_telegram(msg)
            print(f"[CEX新規] {symbol}")
    known_cex_symbols = current


def main():
    print("通知ボットくん 起動中...")
    send_telegram(
        "✅ <b>通知ボットくん 起動しました！</b>\n\n"
        "📊 監視対象：\n"
        "🏦 CEX: Bitget取引所（新規上場）\n"
        "🚀 Solana: Pump.fun 新規トークン\n"
        "🟡 BSC: FourMeme 新規トークン\n"
        "🔵 Base: Clanker 新規トークン\n\n"
        "🔍 通知タイミング：初回取引が来た瞬間"
    )

    # Solana初期化
    print("[Pump.fun] 初期化中...")
    init_sigs = solana_rpc("getSignaturesForAddress", [PUMPFUN_PROGRAM, {"limit": 5}])
    if init_sigs:
        global last_signature
        last_signature = init_sigs[0].get('signature', '')
        print(f"[Pump.fun] 初期化完了 最新sig={last_signature[:20]}")
    else:
        print("[Pump.fun] 初期化失敗")

    # EVMチェーン初期化
    for chain in EVM_CHAINS:
        init_block = evm_rpc(chain["rpc"], "eth_blockNumber", [])
        if init_block:
            chain["last_block"] = int(init_block, 16)
            print(f"[{chain['name']}] 初期化完了 最新ブロック={chain['last_block']}")
        else:
            print(f"[{chain['name']}] 初期化失敗")

    loop = 0
    while True:
        check_cex_listings()
        check_pumpfun_onchain()
        for chain in EVM_CHAINS:
            check_evm_chain(chain)
        time.sleep(60)
        loop += 1
        if loop % 10 == 0:
            bsc_count  = len(EVM_CHAINS[0]["known_tokens"])
            base_count = len(EVM_CHAINS[1]["known_tokens"])
            print(f"[{datetime.now().strftime('%H:%M')}] 稼働中 CEX={len(known_cex_symbols)} Solana={len(known_token_mints)} BSC={bsc_count} Base={base_count}")


if __name__ == "__main__":
    main()
