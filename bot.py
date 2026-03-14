import requests
import time
import os
from datetime import datetime
from collections import Counter

# ── 環境変数 ──────────────────────────────────────────────────────────────────
BOT_TOKEN  = os.environ.get('BOT_TOKEN')
CHAT_ID    = os.environ.get('CHAT_ID')
HELIUS_KEY = os.environ.get('HELIUS_API_KEY', '')

# ── エンドポイント ────────────────────────────────────────────────────────────
BITGET_SYMBOLS_URL = "https://api.bitget.com/api/v2/spot/public/symbols"
SOLANA_RPC = (
    f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}"
    if HELIUS_KEY
    else "https://api.mainnet-beta.solana.com"
)

# ── EVM定数 ──────────────────────────────────────────────────────────────────
TRANSFER_TOPIC  = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO_TOPIC      = "0x0000000000000000000000000000000000000000000000000000000000000000"
PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

# ── 監視チェーン（チェーン追加はここにdictを足すだけ）────────────────────────
EVM_CHAINS = [
    {
        "name": "FourMeme/BSC", "emoji": "🟡",
        "rpc": "https://bsc-rpc.publicnode.com",
        "contract": "0x5c952063c7fc8610ffdb798152d69f0b9550762b",
        "dex_url": "https://dexscreener.com/bsc/{}",
        "launch_url": "https://four.meme",
        "known_tokens": set(), "last_block": None,
    },
    {
        "name": "Clanker/Base", "emoji": "🔵",
        "rpc": "https://base-rpc.publicnode.com",
        "contract": "0xe85a59c628f7d27878aceb4bf3b35733630083a9",
        "dex_url": "https://dexscreener.com/base/{}",
        "launch_url": "https://www.clanker.world",
        "known_tokens": set(), "last_block": None,
    },
]

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

# ── グローバル状態 ────────────────────────────────────────────────────────────
known_cex_symbols = set()
known_token_mints = set()
last_signature    = None


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
            "disable_web_page_preview": True,
        }, timeout=10)
        if r.status_code != 200:
            print(f"Telegram送信エラー: {r.status_code} - {r.text}")
        else:
            print("Telegram送信成功！")
    except Exception as e:
        print(f"Telegram接続エラー: {e}")


def _build_wallet_stats(wallets):
    total  = len(wallets)
    unique = len(set(wallets))
    counter = Counter(wallets)
    top3 = counter.most_common(3)
    top3_count = sum(c for _, c in top3)
    top3_ratio = top3_count / total * 100 if total > 0 else 0
    max_single = top3[0][1] if top3 else 0
    max_single_ratio = max_single / total * 100 if total > 0 else 0
    top5 = counter.most_common(5)
    top5_detail = []
    top5_total  = 0
    for i, (addr, cnt) in enumerate(top5):
        r     = cnt / total * 100
        short = addr[:6] + "..." + addr[-4:]
        top5_detail.append(f"  {'ABCDE'[i]}. {short}: {r:.1f}% ({cnt}件)")
        top5_total += cnt
    others_cnt    = total - top5_total
    others_unique = max(0, unique - len(top5))
    if others_cnt > 0:
        top5_detail.append(
            f"  その他: {others_cnt / total * 100:.1f}%"
            f" ({others_cnt}件 / {others_unique}人)"
        )
    return {
        "total_txns":       total,
        "unique_wallets":   unique,
        "top3_ratio":       top3_ratio,
        "max_single_ratio": max_single_ratio,
        "top5_detail":      top5_detail,
    }


def format_wallet_output(wallet_data):
    if not wallet_data:
        return "👛 ウォレットデータ取得中...\n", ""
    top5_lines = "\n".join(wallet_data["top5_detail"])
    text = (
        f"👛 <b>ウォレット分析</b> (直近{wallet_data['total_txns']}取引)\n"
        f"ユニーク: {wallet_data['unique_wallets']}人 / "
        f"上位3人合計: {wallet_data['top3_ratio']:.0f}%\n"
        f"{top5_lines}\n"
    )
    if wallet_data["max_single_ratio"] >= 50:
        judge = "🚨 自作自演の疑い強い"
    elif wallet_data["top3_ratio"] < 30 and wallet_data["unique_wallets"] >= 15:
        judge = "✅ 多様なウォレット"
    else:
        judge = "🟡 やや集中気味"
    return text, judge


def analyze_dexscreener(token_address):
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code != 200:
            return None
        pairs = r.json().get("pairs", [])
        if not pairs:
            return None
        pair = max(pairs, key=lambda p: (p.get("liquidity") or {}).get("usd", 0))
        return {
            "liquidity":       (pair.get("liquidity") or {}).get("usd", 0) or 0,
            "buys_5m":         (pair.get("txns") or {}).get("m5", {}).get("buys", 0) or 0,
            "sells_5m":        (pair.get("txns") or {}).get("m5", {}).get("sells", 0) or 0,
            "price_change_5m": (pair.get("priceChange") or {}).get("m5", 0) or 0,
        }
    except Exception as e:
        print(f"DexScreenerエラー: {e}")
        return None


def evm_rpc(chain, method, params):
    try:
        r = requests.post(chain["rpc"], json={
            "jsonrpc": "2.0", "id": 1,
            "method": method, "params": params,
        }, timeout=15)
        if r.status_code == 200:
            data = r.json()
            if "error" in data:
                print(f"[{chain['name']}] RPC Error: {data['error']}")
                return None
            return data.get("result")
        print(f"[{chain['name']}] RPC HTTPエラー ({method}): {r.status_code}")
    except Exception as e:
        print(f"[{chain['name']}] RPC 接続エラー ({method}): {e}")
    return None


def evm_wait_for_first_trade(token_address, chain, timeout=300):
    print(f"[{chain['name']}] 初取引待機中: {token_address[:16]}...")
    deadline = time.time() + timeout
    latest_hex = evm_rpc(chain, "eth_blockNumber", [])
    scan_from  = int(latest_hex, 16) - 5 if latest_hex else 0
    while time.time() < deadline:
        latest_hex = evm_rpc(chain, "eth_blockNumber", [])
        if not latest_hex:
            time.sleep(5)
            continue
        latest = int(latest_hex, 16)
        logs = evm_rpc(chain, "eth_getLogs", [{
            "fromBlock": hex(scan_from),
            "toBlock":   hex(latest),
            "address":   token_address,
            "topics":    [TRANSFER_TOPIC],
        }])
        if logs:
            first_block = int(logs[0]["blockNumber"], 16)
            print(f"[{chain['name']}] 初取引検知！ block={first_block}")
            return first_block, time.time()
        scan_from = latest + 1
        time.sleep(5)
    print(f"[{chain['name']}] 初取引タイムアウト: {token_address[:16]}")
    return None, None


def evm_count_trades(token_address, from_block, chain):
    try:
        latest_hex = evm_rpc(chain, "eth_blockNumber", [])
        if not latest_hex:
            return 0
        to_block = int(latest_hex, 16)
        logs = evm_rpc(chain, "eth_getLogs", [{
            "fromBlock": hex(from_block),
            "toBlock":   hex(to_block),
            "address":   token_address,
            "topics":    [TRANSFER_TOPIC],
        }])
        return len(logs) if logs else 0
    except Exception as e:
        print(f"[{chain['name']}] トレード数カウントエラー: {e}")
        return 0


def evm_analyze_wallets(token_address, chain):
    try:
        latest_hex = evm_rpc(chain, "eth_blockNumber", [])
        if not latest_hex:
            return None
        latest     = int(latest_hex, 16)
        from_block = max(0, latest - 500)
        logs = evm_rpc(chain, "eth_getLogs", [{
            "fromBlock": hex(from_block),
            "toBlock":   hex(latest),
            "address":   token_address,
            "topics":    [TRANSFER_TOPIC],
        }])
        if not logs:
            return None
        senders = []
        for log in logs:
            topics = log.get("topics", [])
            if len(topics) >= 2:
                sender = "0x" + topics[1][-40:]
                if sender.lower() != ("0x" + "0" * 40):
                    senders.append(sender.lower())
        return _build_wallet_stats(senders) if senders else None
    except Exception as e:
        print(f"[{chain['name']}] ウォレット分析エラー: {e}")
        return None


def check_evm_chain(chain):
    try:
        latest_hex = evm_rpc(chain, "eth_blockNumber", [])
        if not latest_hex:
            return
        latest_int = int(latest_hex, 16)

        if chain["last_block"] is None:
            chain["last_block"] = latest_int
            print(f"[{chain['name']}] 初期化完了: block={latest_int}")
            return

        # ── FIX: max(last+1, latest-20) → last_block+1（ブロック漏れ防止）──
        from_block = chain["last_block"] + 1
        if latest_int - from_block > 500:
            print(f"[{chain['name']}] ブロック差={latest_int - from_block} → 制限適用")
            from_block = latest_int - 500
        if from_block > latest_int:
            return

        logs = evm_rpc(chain, "eth_getLogs", [{
            "fromBlock": hex(from_block),
            "toBlock":   hex(latest_int),
            "address":   chain["contract"],
            "topics":    [TRANSFER_TOPIC, ZERO_TOPIC],
        }])
        chain["last_block"] = latest_int

        if not logs:
            return

        print(f"[{chain['name']}] {len(logs)}件のイベント検知")

        for log in logs:
            topics = log.get("topics", [])
            if len(topics) < 3:
                continue
            token_address = ("0x" + topics[2][-40:]).lower()
            if token_address in chain["known_tokens"]:
                continue
            chain["known_tokens"].add(token_address)
            print(f"[{chain['name']}] 新規トークン: {token_address}")

            # STEP 1: 初取引待機
            first_block, first_time = evm_wait_for_first_trade(token_address, chain)
            if not first_block:
                print(f"[{chain['name']}] 初取引なし → スキップ")
                continue

            # STEP 2: 3分待機
            wait_remaining = max(0, 180 - (time.time() - first_time))
            if wait_remaining > 0:
                print(f"[{chain['name']}] 3分フィルター待機中 ({wait_remaining:.0f}秒)...")
                time.sleep(wait_remaining)

            # STEP 3: 70件フィルター
            trade_count = evm_count_trades(token_address, first_block, chain)
            print(f"[{chain['name']}] 3分間取引数: {trade_count}件")
            if trade_count < 70:
                print(f"[{chain['name']}] フィルター不合格 ({trade_count} < 70件) → スキップ")
                continue

            print(f"[{chain['name']}] ✅ フィルター合格！分析中...")

            # STEP 4: 通知
            dex         = analyze_dexscreener(token_address)
            wallet_data = evm_analyze_wallets(token_address, chain)
            wallet_text, wallet_judge = format_wallet_output(wallet_data)
            dex_text = (
                f"💧 流動性: ${dex['liquidity']:,.0f}\n"
                f"📈 価格変動: {dex['price_change_5m']:+.1f}%/5分\n"
                f"🛒 買い{dex['buys_5m']}件 / 売り{dex['sells_5m']}件 (5分)\n"
            ) if dex else "📊 価格データ取得中...\n"
            msg = (
                f"{chain['emoji']} <b>[{chain['name']}] 新規トークン検知！</b>\n\n"
                f"時刻: {datetime.now().strftime('%H:%M:%S')}\n"
                f"アドレス: <code>{token_address}</code>\n"
                f"🔥 3分間取引数: <b>{trade_count}件</b>\n\n"
                f"{dex_text}\n"
                f"{wallet_text}\n"
                f"{wallet_judge}\n\n"
                f"📊 {chain['dex_url'].format(token_address)}\n"
                f"🔗 {chain['launch_url']}"
            )
            send_telegram(msg)
            print(f"[{chain['name']}] 通知送信完了: {token_address}")

    except Exception as e:
        print(f"[{chain['name']}] チェックエラー: {e}")


def solana_rpc(method, params):
    for attempt in range(3):
        try:
            r = requests.post(SOLANA_RPC, json={
                "jsonrpc": "2.0", "id": 1,
                "method": method, "params": params,
            }, timeout=15)
            if r.status_code == 200:
                return r.json().get("result")
            print(f"[Solana RPC] HTTPエラー {r.status_code}: {r.text[:100]}")
        except Exception as e:
            print(f"[Solana RPC] 接続エラー ({method}): {e}")
        if attempt < 2:
            time.sleep(0.5)
    return None


def get_new_pumpfun_transactions():
    global last_signature
    params = [PUMPFUN_PROGRAM, {"limit": 20, "commitment": "confirmed"}]
    if last_signature:
        params[1]["until"] = last_signature
    result = solana_rpc("getSignaturesForAddress", params)
    if not result:
        return []
    if result:
        last_signature = result[0].get("signature", "")
    return result


def parse_new_token(signature):
    result = None
    for attempt in range(3):
        result = solana_rpc("getTransaction", [
            signature,
            {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0,
             "commitment": "confirmed"},
        ])
        if result:
            break
        print(f"[Solana] getTransaction返答なし (attempt {attempt+1}/3): {signature[:20]}")
        time.sleep(0.5)
    if not result:
        return None
    post_balances = result.get("meta", {}).get("postTokenBalances", [])
    pre_balances  = result.get("meta", {}).get("preTokenBalances", [])
    pre_mints = {b.get("mint") for b in pre_balances}
    for balance in post_balances:
        mint = balance.get("mint", "")
        if mint and mint not in pre_mints:
            print(f"[新規mint発見] {mint[:20]}")
            return mint
    return None


def wait_for_first_trade(token_address, timeout=300):
    print(f"[Pump.fun] 初取引待機中: {token_address[:20]}...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        sigs = solana_rpc("getSignaturesForAddress", [
            token_address, {"limit": 5, "commitment": "confirmed"},
        ])
        if sigs:
            oldest     = sigs[-1]  # 降順なので[-1]が最古 = 初取引
            block_time = oldest.get("blockTime") or time.time()
            print(f"[Pump.fun] 初取引検知！ blockTime={block_time}")
            return float(block_time), len(sigs)
        time.sleep(5)
    print(f"[Pump.fun] 初取引タイムアウト: {token_address[:20]}")
    return None, 0


def solana_count_trades(token_address, first_trade_time):
    sigs = solana_rpc("getSignaturesForAddress", [
        token_address, {"limit": 200, "commitment": "confirmed"},
    ])
    if not sigs:
        return 0
    cutoff = first_trade_time + 180
    count  = 0
    for sig_info in sigs:  # 降順（新→旧）
        bt = sig_info.get("blockTime", 0)
        if not bt:
            continue
        if bt > cutoff:
            continue       # ウィンドウより新しい → スキップ
        if bt < first_trade_time:
            break          # ウィンドウより古い → 終了
        count += 1
    return count


def analyze_wallets(token_address):
    sigs_result = solana_rpc("getSignaturesForAddress", [
        token_address, {"limit": 50},
    ])
    if not sigs_result:
        return None
    wallets = []
    for sig_info in sigs_result[:30]:
        sig = sig_info.get("signature", "")
        if not sig:
            continue
        time.sleep(0.2)
        tx = solana_rpc("getTransaction", [
            sig, {"encoding": "json", "maxSupportedTransactionVersion": 0},
        ])
        if not tx:
            continue
        keys = tx.get("transaction", {}).get("message", {}).get("accountKeys", [])
        if keys:
            wallets.append(keys[0])
    return _build_wallet_stats(wallets) if wallets else None


def check_pumpfun_onchain():
    global known_token_mints
    txns = get_new_pumpfun_transactions()
    if not txns:
        return
    for tx_info in txns:
        sig = tx_info.get("signature", "")
        if not sig or tx_info.get("err"):
            continue
        time.sleep(0.2)
        mint = parse_new_token(sig)
        if not mint or mint in known_token_mints:
            continue
        known_token_mints.add(mint)
        print(f"[Pump.fun] 新規mint: {mint[:20]} 監視開始...")

        # STEP 1: 初取引待機
        first_trade_time, _ = wait_for_first_trade(mint)
        if not first_trade_time:
            print(f"[Pump.fun] 初取引なし → スキップ")
            continue

        # STEP 2: 3分待機
        wait_remaining = max(0, 180 - (time.time() - first_trade_time))
        if wait_remaining > 0:
            print(f"[Pump.fun] 3分フィルター待機中 ({wait_remaining:.0f}秒)...")
            time.sleep(wait_remaining)

        # STEP 3: 70件フィルター
        trade_count = solana_count_trades(mint, first_trade_time)
        print(f"[Pump.fun] 3分間取引数: {trade_count}件")
        if trade_count < 70:
            print(f"[Pump.fun] フィルター不合格 ({trade_count} < 70件) → スキップ")
            continue

        print(f"[Pump.fun] ✅ フィルター合格！分析中...")

        # STEP 4: 通知
        dex         = analyze_dexscreener(mint)
        wallet_data = analyze_wallets(mint)
        wallet_text, wallet_judge = format_wallet_output(wallet_data)
        dex_text = (
            f"💧 流動性: ${dex['liquidity']:,.0f}\n"
            f"📈 価格変動: {dex['price_change_5m']:+.1f}%/5分\n"
            f"🛒 買い{dex['buys_5m']}件 / 売り{dex['sells_5m']}件 (5分)\n"
        ) if dex else "📊 価格データ取得中...\n"
        msg = (
            f"🚀 <b>[Pump.fun] 新規トークン検知！</b>\n\n"
            f"時刻: {datetime.now().strftime('%H:%M:%S')}\n"
            f"Mint: <code>{mint}</code>\n"
            f"🔥 3分間取引数: <b>{trade_count}件</b>\n\n"
            f"{dex_text}\n"
            f"{wallet_text}\n"
            f"{wallet_judge}\n\n"
            f"📊 https://dexscreener.com/solana/{mint}\n"
            f"🔗 https://pump.fun/{mint}"
        )
        send_telegram(msg)
        print(f"[Pump.fun] 通知送信完了: {mint[:20]}")


def get_cex_symbols():
    try:
        r = requests.get(BITGET_SYMBOLS_URL, headers=HEADERS, timeout=10)
        data = r.json()
        if data.get("code") == "00000":
            return {
                item["symbol"]
                for item in data["data"]
                if item.get("status") == "online"
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
        if symbol.endswith("USDT"):
            base = symbol.replace("USDT", "")
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


def main():
    print("通知ボットくん 起動中...")
    send_telegram(
        "✅ <b>通知ボットくん 起動しました！</b>\n\n"
        "📊 監視対象：\n"
        "🏦 CEX: Bitget取引所（新規上場）\n"
        "🚀 DEX: Pump.fun / Solana\n"
        "🟡 DEX: FourMeme / BSC\n"
        "🔵 DEX: Clanker / Base\n\n"
        "🔍 フィルター条件：\n"
        "・初取引検知後、3分間の取引数が70件以上のみ通知\n"
        "・ウォレット多様性（自作自演チェック）付き"
    )

    print("[Pump.fun] 初期化中...")
    init_sigs = solana_rpc("getSignaturesForAddress", [PUMPFUN_PROGRAM, {"limit": 5}])
    if init_sigs:
        global last_signature
        last_signature = init_sigs[0].get("signature", "")
        print(f"[Pump.fun] 初期化完了 sig={last_signature[:20]}")

    loop = 0
    while True:
        check_cex_listings()
        check_pumpfun_onchain()
        for chain in EVM_CHAINS:
            check_evm_chain(chain)
        time.sleep(20)
        loop += 1
        if loop % 30 == 0:
            evm_status = " ".join(
                f"{c['name']}={len(c['known_tokens'])}" for c in EVM_CHAINS
            )
            print(
                f"[{datetime.now().strftime('%H:%M')}] 稼働中 "
                f"CEX={len(known_cex_symbols)} "
                f"Solana={len(known_token_mints)} {evm_status}"
            )


if __name__ == "__main__":
    main()
