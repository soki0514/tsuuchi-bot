import requests
import time
import os
import threading
from datetime import datetime
from collections import Counter

# ── 環境変数 ──────────────────────────────────────────────────────────────────
BOT_TOKEN  = os.environ.get('BOT_TOKEN')
CHAT_ID    = os.environ.get('CHAT_ID')
HELIUS_KEY = os.environ.get('HELIUS_API_KEY', '')

# ── エンドポイント ────────────────────────────────────────────────────────────
BITGET_SYMBOLS_URL  = "https://api.bitget.com/api/v2/spot/public/symbols"
SOLANA_RPC = (
    f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}"
    if HELIUS_KEY
    else "https://api.mainnet-beta.solana.com"
)

# ── EVM定数 ──────────────────────────────────────────────────────────────────
TRANSFER_TOPIC  = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO_TOPIC      = "0x0000000000000000000000000000000000000000000000000000000000000000"
PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

# ── Solana全般監視: SPL Token Metadata Program (全launchpad対応) ───────────────
SPL_METADATA_PROGRAM = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"

# ── EVM全般監視定数 ────────────────────────────────────────────────────────────
POOL_CREATED_TOPIC      = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"
PANCAKE_V3_FACTORY_BSC  = "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"
UNISWAP_V3_FACTORY_BASE = "0x33128a8fC17869897dcE68Ed026d694621f6FDfD"
PAIR_CREATED_TOPIC      = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
PANCAKE_V2_FACTORY_BSC  = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"

BSC_BASE_TOKENS = {
    "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
    "0x55d398326f99059ff775485246999027b3197955",
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
    "0xe9e7cea3dedca5984780bafc599bd69add087d56",
    "0x2170ed0880ac9a755fd29b2688956bd959f933f8",
    "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3",
}
BASE_BASE_TOKENS = {
    "0x4200000000000000000000000000000000000006",
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
    "0x50c5725949a6f0c72e6c4a641f24049a917db0cb",
    "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca",
    "0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22",
}

_BSC_KNOWN  = set()
_BASE_KNOWN = set()

EVM_CHAINS = [
    {
        "name": "FourMeme/BSC", "emoji": "🟡",
        "rpc_list": [
            "https://bsc-mainnet.public.blastapi.io",
            "https://1rpc.io/bnb",
            "https://bsc-rpc.publicnode.com",
        ],
        "contract": "0x5c952063c7fc8610ffdb798152d69f0b9550762b",
        "dex_url": "https://dexscreener.com/bsc/{}",
        "launch_url": "https://four.meme",
        "known_tokens": _BSC_KNOWN, "last_block": None,
    },
    {
        "name": "Clanker/Base", "emoji": "🔵",
        "rpc_list": [
            "https://base-mainnet.public.blastapi.io",
            "https://1rpc.io/base",
            "https://base.llamarpc.com",
        ],
        "contract": "0xe85a59c628f7d27878aceb4bf3b35733630083a9",
        "dex_url": "https://dexscreener.com/base/{}",
        "launch_url": "https://www.clanker.world",
        "known_tokens": _BASE_KNOWN, "last_block": None,
    },
]

EVM_ALL_CHAINS = [
    {
        "name": "BNB Chain全般(V2)", "emoji": "🟡",
        "rpc_list": [
            "https://bsc-mainnet.public.blastapi.io",
            "https://1rpc.io/bnb",
            "https://bsc-rpc.publicnode.com",
        ],
        "factory":      PANCAKE_V2_FACTORY_BSC,
        "topic":        PAIR_CREATED_TOPIC,
        "base_tokens":  BSC_BASE_TOKENS,
        "dex_url":      "https://dexscreener.com/bsc/{}",
        "known_tokens": _BSC_KNOWN,
        "last_block":   None,
    },
    {
        "name": "BNB Chain全般(V3)", "emoji": "🟡",
        "rpc_list": [
            "https://bsc-mainnet.public.blastapi.io",
            "https://1rpc.io/bnb",
            "https://bsc-rpc.publicnode.com",
        ],
        "factory":      PANCAKE_V3_FACTORY_BSC,
        "topic":        POOL_CREATED_TOPIC,
        "base_tokens":  BSC_BASE_TOKENS,
        "dex_url":      "https://dexscreener.com/bsc/{}",
        "known_tokens": _BSC_KNOWN,
        "last_block":   None,
    },
    {
        "name": "Base全般", "emoji": "🔵",
        "rpc_list": [
            "https://base-mainnet.public.blastapi.io",
            "https://1rpc.io/base",
            "https://base.llamarpc.com",
        ],
        "factory":      UNISWAP_V3_FACTORY_BASE,
        "topic":        POOL_CREATED_TOPIC,
        "base_tokens":  BASE_BASE_TOKENS,
        "dex_url":      "https://dexscreener.com/base/{}",
        "known_tokens": _BASE_KNOWN,
        "last_block":   None,
    },
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

# ── グローバル状態 ────────────────────────────────────────────────────────────
known_cex_symbols = set()
known_token_mints = set()
last_signature    = None
all_solana_last_signature = None

WALLET_SEMAPHORE = threading.Semaphore(2)
KNOWN_MINTS_LOCK = threading.Lock()

RETRY_SIG_QUEUE = []
RETRY_SIG_LOCK  = threading.Lock()
RETRY_EXPIRY    = 300


# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ══════════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════════
# 共通ウォレット統計
# ══════════════════════════════════════════════════════════════════════════════

def _build_wallet_stats(wallets):
    total   = len(wallets)
    unique  = len(set(wallets))
    counter = Counter(wallets)
    top3    = counter.most_common(3)
    top3_count       = sum(c for _, c in top3)
    top3_ratio       = top3_count / total * 100 if total > 0 else 0
    max_single       = top3[0][1] if top3 else 0
    max_single_ratio = max_single / total * 100 if total > 0 else 0

    top5        = counter.most_common(5)
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


# ══════════════════════════════════════════════════════════════════════════════
# DEXSCREENER
# ══════════════════════════════════════════════════════════════════════════════

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
            "dex_id":          pair.get("dexId", ""),
        }
    except Exception as e:
        print(f"DexScreenerエラー: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# EVM RPC
# ══════════════════════════════════════════════════════════════════════════════

def evm_rpc(chain, method, params):
    rpc_list = chain.get("rpc_list") or [chain.get("rpc", "")]
    for rpc_url in rpc_list:
        for attempt in range(2):
            try:
                r = requests.post(rpc_url, json={
                    "jsonrpc": "2.0", "id": 1,
                    "method": method, "params": params,
                }, timeout=15)
                if r.status_code == 200:
                    data = r.json()
                    if "error" in data:
                        err = data["error"]
                        print(f"[{chain['name']}] RPC Error ({rpc_url.split('/')[2]}): {err}")
                        break
                    return data.get("result")
                print(f"[{chain['name']}] HTTP {r.status_code} ({rpc_url.split('/')[2]}) → 次のRPCへ")
                break
            except Exception as e:
                print(f"[{chain['name']}] 接続エラー ({rpc_url.split('/')[2]}) attempt{attempt+1}: {e}")
                if attempt < 1:
                    time.sleep(1)
    return None


def evm_wait_for_first_trade(token_address, chain, timeout=300):
    print(f"[{chain['name']}] 初取引待機中: {token_address[:16]}...")
    deadline  = time.time() + timeout
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


# ══════════════════════════════════════════════════════════════════════════════
# EVM トークン処理スレッド
# ══════════════════════════════════════════════════════════════════════════════

def _process_evm_token(token_address, chain):
    try:
        first_block, first_time = evm_wait_for_first_trade(token_address, chain)
        if not first_block:
            print(f"[{chain['name']}] 初取引なし → スキップ: {token_address[:16]}")
            return

        wait_remaining = max(0, 180 - (time.time() - first_time))
        if wait_remaining > 0:
            print(f"[{chain['name']}] 3分フィルター待機中 ({wait_remaining:.0f}秒)... ※メインループは継続中")
            time.sleep(wait_remaining)

        wallet_data  = evm_analyze_wallets(token_address, chain)
        unique_count = wallet_data["unique_wallets"] if wallet_data else 0
        print(f"[{chain['name']}] 3分間ユニークアドレス: {unique_count}人")
        if unique_count < 30:
            print(f"[{chain['name']}] フィルター不合格 ({unique_count} < 30人) → スキップ")
            return

        print(f"[{chain['name']}] ✅ フィルター合格！通知送信中...")

        dex = analyze_dexscreener(token_address)
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
            f"👥 3分間ユニーク: <b>{unique_count}人</b>\n\n"
            f"{dex_text}\n"
            f"{wallet_text}\n"
            f"{wallet_judge}\n\n"
            f"📊 {chain['dex_url'].format(token_address)}\n"
            f"🔗 {chain['launch_url']}"
        )
        send_telegram(msg)
        print(f"[{chain['name']}] 通知送信完了: {token_address}")

    except Exception as e:
        print(f"[{chain['name']}] スレッドエラー ({token_address[:16]}): {e}")


# ══════════════════════════════════════════════════════════════════════════════
# EVM チェーン監視
# ══════════════════════════════════════════════════════════════════════════════

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
            print(f"[{chain['name']}] 新規トークン → スレッド起動: {token_address}")

            t = threading.Thread(
                target=_process_evm_token,
                args=(token_address, chain),
                daemon=True,
            )
            t.start()

    except Exception as e:
        print(f"[{chain['name']}] チェックエラー: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# EVM 全般監視
# ══════════════════════════════════════════════════════════════════════════════

def _process_evm_all_token(token_address, chain):
    try:
        first_block, first_time = evm_wait_for_first_trade(token_address, chain)
        if not first_block:
            print(f"[{chain['name']}] 初取引なし → スキップ: {token_address[:16]}")
            return

        wait_remaining = max(0, 180 - (time.time() - first_time))
        if wait_remaining > 0:
            print(f"[{chain['name']}] 3分フィルター待機中 ({wait_remaining:.0f}秒)... ※メインループは継続中")
            time.sleep(wait_remaining)

        wallet_data  = evm_analyze_wallets(token_address, chain)
        unique_count = wallet_data["unique_wallets"] if wallet_data else 0
        print(f"[{chain['name']}] 3分間ユニークアドレス: {unique_count}人")
        if unique_count < 30:
            print(f"[{chain['name']}] フィルター不合格 ({unique_count} < 30人) → スキップ")
            return

        print(f"[{chain['name']}] ✅ フィルター合格！通知送信中...")

        dex = analyze_dexscreener(token_address)
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
            f"👥 3分間ユニーク: <b>{unique_count}人</b>\n\n"
            f"{dex_text}\n"
            f"{wallet_text}\n"
            f"{wallet_judge}\n\n"
            f"📊 {chain['dex_url'].format(token_address)}"
        )
        send_telegram(msg)
        print(f"[{chain['name']}] 通知送信完了: {token_address}")

    except Exception as e:
        print(f"[{chain['name']}] スレッドエラー ({token_address[:16]}): {e}")


def check_evm_all_chain(chain):
    try:
        latest_hex = evm_rpc(chain, "eth_blockNumber", [])
        if not latest_hex:
            return
        latest_int = int(latest_hex, 16)

        if chain["last_block"] is None:
            chain["last_block"] = latest_int
            print(f"[{chain['name']}] 初期化完了: block={latest_int}")
            return

        from_block = chain["last_block"] + 1
        if latest_int - from_block > 500:
            print(f"[{chain['name']}] ブロック差={latest_int - from_block} → 制限適用")
            from_block = latest_int - 500
        if from_block > latest_int:
            return

        event_topic = chain.get("topic", POOL_CREATED_TOPIC)
        logs = evm_rpc(chain, "eth_getLogs", [{
            "fromBlock": hex(from_block),
            "toBlock":   hex(latest_int),
            "address":   chain["factory"],
            "topics":    [event_topic],
        }])
        chain["last_block"] = latest_int

        if not logs:
            return

        print(f"[{chain['name']}] {len(logs)}件のPair/PoolCreatedイベント検知")

        base_tokens = chain["base_tokens"]
        for log in logs:
            topics = log.get("topics", [])
            if len(topics) < 3:
                continue
            token0 = ("0x" + topics[1][-40:]).lower()
            token1 = ("0x" + topics[2][-40:]).lower()

            t0_is_base = token0 in base_tokens
            t1_is_base = token1 in base_tokens
            if not t0_is_base and t1_is_base:
                new_token = token0
            elif t0_is_base and not t1_is_base:
                new_token = token1
            else:
                continue

            if new_token in chain["known_tokens"]:
                continue

            chain["known_tokens"].add(new_token)
            print(f"[{chain['name']}] 新規トークン → スレッド起動: {new_token}")

            t = threading.Thread(
                target=_process_evm_all_token,
                args=(new_token, chain),
                daemon=True,
            )
            t.start()

    except Exception as e:
        print(f"[{chain['name']}] チェックエラー: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# SOLANA RPC
# ══════════════════════════════════════════════════════════════════════════════

def solana_rpc(method, params):
    for attempt in range(4):
        try:
            r = requests.post(SOLANA_RPC, json={
                "jsonrpc": "2.0", "id": 1,
                "method": method, "params": params,
            }, timeout=15)
            if r.status_code == 200:
                return r.json().get("result")
            if r.status_code == 429:
                wait = 2 ** attempt
                print(f"[Solana RPC] 429 レート制限 → {wait}秒待機 (attempt {attempt+1}/4)")
                time.sleep(wait)
                continue
            print(f"[Solana RPC] HTTPエラー {r.status_code}: {r.text[:100]}")
        except Exception as e:
            print(f"[Solana RPC] 接続エラー ({method}): {e}")
        if attempt < 3:
            time.sleep(0.5)
    return None


def get_new_pumpfun_transactions():
    global last_signature
    all_txns  = []
    before    = None
    is_catchup = (last_signature is None)

    while True:
        opts = {"limit": 50, "commitment": "confirmed"}
        if last_signature:
            opts["until"] = last_signature
        if before:
            opts["before"] = before

        result = solana_rpc("getSignaturesForAddress", [PUMPFUN_PROGRAM, opts])
        if not result:
            break

        all_txns.extend(result)

        if len(result) < 50:
            break

        if is_catchup:
            print(f"[Pump.fun] 初回起動: 最新{len(all_txns)}件のみ処理（遡り制限）")
            break

        if len(all_txns) >= 200:
            print(f"[Pump.fun] ページネーション上限200件 → 打ち切り")
            break

        before = result[-1].get("signature")
        time.sleep(0.1)

    if all_txns:
        last_signature = all_txns[0].get("signature", "")
        if len(all_txns) > 1:
            print(f"[Pump.fun] {len(all_txns)}件の新規TX検出")

    return all_txns


def parse_new_token(signature):
    IGNORED_MINTS = {
        "So11111111111111111111111111111111111111112",
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
        "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",
        "7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj",
    }

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
        return False
    post_balances = result.get("meta", {}).get("postTokenBalances", [])
    pre_balances  = result.get("meta", {}).get("preTokenBalances", [])
    pre_mints = {b.get("mint") for b in pre_balances}
    for balance in post_balances:
        mint = balance.get("mint", "")
        if mint and mint not in pre_mints and mint not in IGNORED_MINTS:
            print(f"[新規mint発見] {mint[:20]}")
            return mint
    return None


def parse_new_fungible_mint(signature):
    IGNORED_MINTS = {
        "So11111111111111111111111111111111111111112",
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
        "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",
        "7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj",
    }

    result = None
    for attempt in range(3):
        result = solana_rpc("getTransaction", [
            signature,
            {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0,
             "commitment": "confirmed"},
        ])
        if result:
            break
        time.sleep(0.5)
    if not result:
        return False

    post_balances = result.get("meta", {}).get("postTokenBalances", [])
    pre_balances  = result.get("meta", {}).get("preTokenBalances", [])
    pre_mints = {b.get("mint") for b in pre_balances}

    for balance in post_balances:
        mint     = balance.get("mint", "")
        decimals = balance.get("uiTokenAmount", {}).get("decimals", 0)
        if not mint or mint in pre_mints or mint in IGNORED_MINTS:
            continue
        if decimals == 0:
            continue
        print(f"[Solana全般] 新規ファンジブルmint: {mint[:20]} (decimals={decimals})")
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
            oldest     = sigs[-1]
            block_time = oldest.get("blockTime") or time.time()
            print(f"[Pump.fun] 初取引検知！ blockTime={block_time}")
            return float(block_time), len(sigs)
        time.sleep(10)
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
    for sig_info in sigs:
        bt = sig_info.get("blockTime", 0)
        if not bt:
            continue
        if bt > cutoff:
            continue
        if bt < first_trade_time:
            break
        count += 1
    return count


def get_holder_count(mint_address):
    """
    Solanaトークンの保有者数（残高>0のアカウント数）を取得。
    Helius DAS API getTokenAccounts を使用。
    保有者数が1001を超えた時点で打ち切り（フィルター対象外のため）。
    失敗時はNoneを返す（= フィルターはスキップ）。
    """
    if not HELIUS_KEY:
        return None

    try:
        url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}"
        total = 0
        page  = 1
        while True:
            payload = {
                "jsonrpc": "2.0",
                "id":      f"holders-p{page}",
                "method":  "getTokenAccounts",
                "params": {
                    "page":    page,
                    "limit":   1000,
                    "mint":    mint_address,
                    "options": {"showZeroBalance": False},
                },
            }
            r = requests.post(url, json=payload, timeout=15)
            if r.status_code != 200:
                print(f"[保有者数] HTTPエラー {r.status_code}")
                break
            accounts = r.json().get("result", {}).get("token_accounts", [])
            total += len(accounts)

            # 1000超 = フィルター対象外なので即打ち切り
            if total > 1000 or len(accounts) < 1000:
                break
            page += 1
            time.sleep(0.3)

        print(f"[保有者数] {mint_address[:20]} → {total}人")
        return total

    except Exception as e:
        print(f"[保有者数] 取得エラー: {e}")
        return None


def is_holder_ratio_suspicious(mint, unique_traders, label=""):
    """
    保有者分布が不自然かチェック。
    条件: 保有者数<=1000 かつ 保有者数/取引アドレス数>=4 → True（通知スキップ）
    取得失敗時はFalse（= 安全側に倒してスキップしない）。
    """
    if unique_traders == 0:
        return False

    holder_count = get_holder_count(mint)
    if holder_count is None:
        return False

    ratio = holder_count / unique_traders
    print(f"[{label}] 保有者={holder_count} / 取引={unique_traders} → 比率={ratio:.1f}倍")

    if holder_count <= 1000 and ratio >= 4:
        print(f"[{label}] ❌ 保有者比率異常（{ratio:.1f}倍 >= 4倍）→ 通知スキップ")
        return True
    return False


def analyze_wallets(token_address):
    with WALLET_SEMAPHORE:
        sigs_result = solana_rpc("getSignaturesForAddress", [
            token_address, {"limit": 100},
        ])
        if not sigs_result:
            return None
        wallets = []
        for sig_info in sigs_result[:100]:
            sig = sig_info.get("signature", "")
            if not sig:
                continue
            time.sleep(0.4)
            tx = solana_rpc("getTransaction", [
                sig, {"encoding": "json", "maxSupportedTransactionVersion": 0},
            ])
            if not tx:
                continue
            keys = tx.get("transaction", {}).get("message", {}).get("accountKeys", [])
            if keys:
                wallets.append(keys[0])
        return _build_wallet_stats(wallets) if wallets else None


# ══════════════════════════════════════════════════════════════════════════════
# Solana (pump.fun) トークン処理スレッド
# ══════════════════════════════════════════════════════════════════════════════

def _process_solana_token(mint):
    try:
        first_trade_time, _ = wait_for_first_trade(mint)
        if not first_trade_time:
            print(f"[Pump.fun] 初取引なし → スキップ: {mint[:20]}")
            return

        # ── STEP A: 初取引から60秒後 → 早期チェック ──────────────────────────
        wait_early = max(0, 60 - (time.time() - first_trade_time))
        if wait_early > 0:
            time.sleep(wait_early)

        early_data   = analyze_wallets(mint)
        early_unique = early_data["unique_wallets"] if early_data else 0
        print(f"[Pump.fun] 早期チェック(60秒): {early_unique}人")

        if early_unique >= 30:
            # 保有者比率フィルター（保有者<=1000 かつ 保有者/取引>=4倍 → スキップ）
            if is_holder_ratio_suspicious(mint, early_unique, "Pump.fun早期"):
                return
            dex = analyze_dexscreener(mint)
            dex_text = (
                f"💧 流動性: ${dex['liquidity']:,.0f}\n"
                f"📈 価格変動: {dex['price_change_5m']:+.1f}%/5分\n"
                f"🛒 買い{dex['buys_5m']}件 / 売り{dex['sells_5m']}件 (5分)\n"
            ) if dex else "📊 価格データ取得中...\n"
            msg = (
                f"🟣 <b>[Pump.fun] ボンカーブ早期検知！</b>\n\n"
                f"時刻: {datetime.now().strftime('%H:%M:%S')}\n"
                f"📋 Mintアドレス（タップでコピー）\n"
                f"<code>{mint}</code>\n\n"
                f"👥 取引アドレス: <b>{early_unique}人</b>\n\n"
                f"{dex_text}\n"
                f"⚡ ボンディングカーブ中（Raydium未移行）\n\n"
                f"📊 https://dexscreener.com/solana/{mint}\n"
                f"📱 <a href=\"https://pump.fun/{mint}\">pump.fun（Bitget Walletで開く）</a>"
            )
            send_telegram(msg)
            print(f"[Pump.fun] 🟣 早期通知送信完了: {mint[:20]}")

        # ── STEP B: 初取引から180秒後 → 確定チェック ─────────────────────────
        wait_final = max(0, 180 - (time.time() - first_trade_time))
        if wait_final > 0:
            print(f"[Pump.fun] 確定チェック待機中 ({wait_final:.0f}秒)...")
            time.sleep(wait_final)

        wallet_data  = analyze_wallets(mint)
        unique_count = wallet_data["unique_wallets"] if wallet_data else 0
        print(f"[Pump.fun] 確定チェック(180秒): {unique_count}人")
        if unique_count < 20:
            print(f"[Pump.fun] フィルター不合格 ({unique_count} < 20人) → スキップ")
            return

        # 保有者比率フィルター（保有者<=1000 かつ 保有者/取引>=4倍 → スキップ）
        if is_holder_ratio_suspicious(mint, unique_count, "Pump.fun確定"):
            return

        print(f"[Pump.fun] ✅ フィルター合格！🚀通知送信中...")

        dex = analyze_dexscreener(mint)
        wallet_text, wallet_judge = format_wallet_output(wallet_data)
        dex_text = (
            f"💧 流動性: ${dex['liquidity']:,.0f}\n"
            f"📈 価格変動: {dex['price_change_5m']:+.1f}%/5分\n"
            f"🛒 買い{dex['buys_5m']}件 / 売り{dex['sells_5m']}件 (5分)\n"
        ) if dex else "📊 価格データ取得中...\n"
        msg = (
            f"🚀 <b>[Pump.fun] 新規トークン検知！</b>\n\n"
            f"時刻: {datetime.now().strftime('%H:%M:%S')}\n"
            f"📋 Mintアドレス（タップでコピー）\n"
            f"<code>{mint}</code>\n\n"
            f"👥 3分間ユニーク: <b>{unique_count}人</b>\n\n"
            f"{dex_text}\n"
            f"{wallet_text}\n"
            f"{wallet_judge}\n\n"
            f"📊 https://dexscreener.com/solana/{mint}\n"
            f"📱 <a href=\"https://pump.fun/{mint}\">pump.fun（Bitget Walletで開く）</a>"
        )
        send_telegram(msg)
        print(f"[Pump.fun] 🚀 確定通知送信完了: {mint[:20]}")

    except Exception as e:
        print(f"[Pump.fun] スレッドエラー ({mint[:20]}): {e}")


# ══════════════════════════════════════════════════════════════════════════════
# Pump.fun 監視
# ══════════════════════════════════════════════════════════════════════════════

def check_pumpfun_onchain():
    global known_token_mints
    txns = get_new_pumpfun_transactions()
    if not txns:
        return

    now = time.time()
    before_filter = len(txns)
    txns = [tx for tx in txns
            if not tx.get("blockTime") or (now - tx["blockTime"]) <= 300]
    if len(txns) < before_filter:
        print(f"[Pump.fun] 古いTX除外: {before_filter - len(txns)}件スキップ"
              f"（残り{len(txns)}件）")

    for tx_info in txns:
        sig = tx_info.get("signature", "")
        if not sig or tx_info.get("err"):
            continue
        time.sleep(0.5)
        mint = parse_new_token(sig)
        if mint is False:
            with RETRY_SIG_LOCK:
                RETRY_SIG_QUEUE.append((sig, time.time()))
            print(f"[Pump.fun] リトライ予約: {sig[:20]}")
            continue
        if not mint:
            continue
        with KNOWN_MINTS_LOCK:
            if mint in known_token_mints:
                continue
            known_token_mints.add(mint)
        print(f"[Pump.fun] 新規mint → スレッド起動: {mint[:20]}")

        t = threading.Thread(
            target=_process_solana_token,
            args=(mint,),
            daemon=True,
        )
        t.start()


# ══════════════════════════════════════════════════════════════════════════════
# Pump.fun リトライキュー処理
# ══════════════════════════════════════════════════════════════════════════════

def process_retry_queue():
    global known_token_mints
    now = time.time()

    with RETRY_SIG_LOCK:
        if not RETRY_SIG_QUEUE:
            return
        valid   = [(s, t) for s, t in RETRY_SIG_QUEUE if now - t <= RETRY_EXPIRY]
        expired = len(RETRY_SIG_QUEUE) - len(valid)
        RETRY_SIG_QUEUE.clear()

    if expired > 0:
        print(f"[Pump.fun] リトライ期限切れ: {expired}件 破棄")
    if not valid:
        return

    print(f"[Pump.fun] リトライ処理: {len(valid)}件")
    still_failed = []
    for sig, enqueued_at in valid:
        time.sleep(1.0)
        mint = parse_new_token(sig)

        if mint is False:
            if time.time() - enqueued_at <= RETRY_EXPIRY:
                still_failed.append((sig, enqueued_at))
            continue

        if not mint:
            continue
        with KNOWN_MINTS_LOCK:
            if mint in known_token_mints:
                continue
            known_token_mints.add(mint)
        print(f"[Pump.fun] ✅ リトライ成功！mint → スレッド起動: {mint[:20]}")
        t = threading.Thread(
            target=_process_solana_token,
            args=(mint,),
            daemon=True,
        )
        t.start()

    if still_failed:
        with RETRY_SIG_LOCK:
            RETRY_SIG_QUEUE.extend(still_failed)
        print(f"[Pump.fun] リトライ再キュー: {len(still_failed)}件")


# ══════════════════════════════════════════════════════════════════════════════
# Solana 全般監視
# ══════════════════════════════════════════════════════════════════════════════

def get_new_metadata_transactions():
    global all_solana_last_signature
    all_txns = []
    before   = None
    is_first = (all_solana_last_signature is None)

    while True:
        opts = {"limit": 50, "commitment": "confirmed"}
        if all_solana_last_signature:
            opts["until"] = all_solana_last_signature
        if before:
            opts["before"] = before

        result = solana_rpc("getSignaturesForAddress", [SPL_METADATA_PROGRAM, opts])
        if not result:
            break

        all_txns.extend(result)

        if len(result) < 50:
            break

        if is_first:
            print(f"[Solana全般] 初回起動: 最新{len(all_txns)}件のみ処理（遡り制限）")
            break

        if len(all_txns) >= 100:
            print("[Solana全般] ページネーション上限100件 → 打ち切り")
            break

        before = result[-1].get("signature")
        time.sleep(0.1)

    if all_txns:
        all_solana_last_signature = all_txns[0].get("signature", "")
        if len(all_txns) > 1:
            print(f"[Solana全般] {len(all_txns)}件の新規TX検出")

    return all_txns


def _process_solana_any_token(mint):
    try:
        print(f"[Solana全般] 初取引待機中: {mint[:20]}...")
        deadline = time.time() + 300
        first_trade_time = None
        while time.time() < deadline:
            sigs = solana_rpc("getSignaturesForAddress", [
                mint, {"limit": 5, "commitment": "confirmed"},
            ])
            if sigs:
                oldest = sigs[-1]
                first_trade_time = float(oldest.get("blockTime") or time.time())
                print(f"[Solana全般] 初取引検知: {mint[:20]}")
                break
            time.sleep(10)

        if not first_trade_time:
            print(f"[Solana全般] 初取引タイムアウト → スキップ: {mint[:20]}")
            return

        # ── 60秒チェック ──────────────────────────────────────────────────────
        wait_secs = max(0, 60 - (time.time() - first_trade_time))
        if wait_secs > 0:
            time.sleep(wait_secs)

        early_data   = analyze_wallets(mint)
        early_unique = early_data["unique_wallets"] if early_data else 0
        print(f"[Solana全般] 60秒チェック: {early_unique}人 ({mint[:16]})")

        if early_unique >= 50:
            # 保有者比率フィルター（保有者<=1000 かつ 保有者/取引>=4倍 → スキップ）
            if is_holder_ratio_suspicious(mint, early_unique, "Solana全般早期"):
                return
            dex = analyze_dexscreener(mint)
            platform = _get_platform_name(dex)
            dex_text = _build_dex_text(dex)
            msg = (
                f"🟡 <b>[Solana/{platform}] 新規トークン早期検知！</b>\n\n"
                f"時刻: {datetime.now().strftime('%H:%M:%S')}\n"
                f"📋 Mintアドレス（タップでコピー）\n"
                f"<code>{mint}</code>\n\n"
                f"👥 取引アドレス: <b>{early_unique}人</b>\n\n"
                f"{dex_text}"
                f"📊 https://dexscreener.com/solana/{mint}\n"
                f"🔍 https://solscan.io/token/{mint}"
            )
            send_telegram(msg)
            print(f"[Solana全般] 🟡 早期通知送信完了: {mint[:20]}")

        # ── 180秒チェック ─────────────────────────────────────────────────────
        wait_secs = max(0, 180 - (time.time() - first_trade_time))
        if wait_secs > 0:
            print(f"[Solana全般] 確定チェック待機中 ({wait_secs:.0f}秒)...")
            time.sleep(wait_secs)

        wallet_data  = analyze_wallets(mint)
        unique_count = wallet_data["unique_wallets"] if wallet_data else 0
        print(f"[Solana全般] 180秒チェック: {unique_count}人 ({mint[:16]})")

        if unique_count < 50:
            print(f"[Solana全般] フィルター不合格 ({unique_count} < 50人) → スキップ")
            return

        # 保有者比率フィルター（保有者<=1000 かつ 保有者/取引>=4倍 → スキップ）
        if is_holder_ratio_suspicious(mint, unique_count, "Solana全般確定"):
            return

        dex = analyze_dexscreener(mint)
        platform = _get_platform_name(dex)
        dex_text = _build_dex_text(dex)
        wallet_text, wallet_judge = format_wallet_output(wallet_data)
        msg = (
            f"🟢 <b>[Solana/{platform}] 新規トークン確定通知！</b>\n\n"
            f"時刻: {datetime.now().strftime('%H:%M:%S')}\n"
            f"📋 Mintアドレス（タップでコピー）\n"
            f"<code>{mint}</code>\n\n"
            f"👥 3分間ユニーク: <b>{unique_count}人</b>\n\n"
            f"{dex_text}"
            f"{wallet_text}\n"
            f"{wallet_judge}\n\n"
            f"📊 https://dexscreener.com/solana/{mint}\n"
            f"🔍 https://solscan.io/token/{mint}"
        )
        send_telegram(msg)
        print(f"[Solana全般] 🟢 確定通知送信完了: {mint[:20]}")

    except Exception as e:
        print(f"[Solana全般] スレッドエラー ({mint[:20]}): {e}")


def _get_platform_name(dex):
    if not dex:
        return "Unknown"
    dex_id = dex.get("dex_id", "").lower()
    name_map = {
        "raydium":     "Raydium",
        "pump-fun":    "pump.fun",
        "pumpfun":     "pump.fun",
        "orca":        "Orca",
        "meteora":     "Meteora",
        "jupiter":     "Jupiter",
        "rapidlaunch": "rapidlaunch.io",
        "moonshot":    "Moonshot",
        "letsbonk":    "LetsBonk",
    }
    for key, label in name_map.items():
        if key in dex_id:
            return label
    return dex_id.capitalize() if dex_id else "Unknown"


def _build_dex_text(dex):
    if not dex:
        return "📊 価格データ取得中...\n\n"
    return (
        f"💧 流動性: ${dex['liquidity']:,.0f}\n"
        f"📈 価格変動: {dex['price_change_5m']:+.1f}%/5分\n"
        f"🛒 買い{dex['buys_5m']}件 / 売り{dex['sells_5m']}件 (5分)\n\n"
    )


def check_all_solana_onchain():
    global known_token_mints
    txns = get_new_metadata_transactions()
    if not txns:
        return

    now = time.time()
    before_filter = len(txns)
    txns = [tx for tx in txns
            if not tx.get("blockTime") or (now - tx["blockTime"]) <= 300]
    if len(txns) < before_filter:
        print(f"[Solana全般] 古いTX除外: {before_filter - len(txns)}件スキップ"
              f"（残り{len(txns)}件）")

    # TX上限撤廃（全件処理・独立スレッドなのでEVM監視に影響なし）

    new_count = 0
    for tx_info in txns:
        sig = tx_info.get("signature", "")
        if not sig or tx_info.get("err"):
            continue
        time.sleep(0.6)
        mint = parse_new_fungible_mint(sig)
        if mint is False or not mint:
            continue
        with KNOWN_MINTS_LOCK:
            if mint in known_token_mints:
                continue
            known_token_mints.add(mint)
        new_count += 1
        print(f"[Solana全般] 新規ファンジブルmint → スレッド起動: {mint[:20]}")

        t = threading.Thread(
            target=_process_solana_any_token,
            args=(mint,),
            daemon=True,
        )
        t.start()

    if new_count > 0:
        print(f"[Solana全般] {new_count}件の新規トークンスレッド起動")


def pumpfun_monitor_loop():
    global last_signature
    print("[Pump.fun] 監視ループ開始中...")

    init_sigs = solana_rpc("getSignaturesForAddress", [PUMPFUN_PROGRAM, {"limit": 5}])
    if init_sigs:
        last_signature = init_sigs[0].get("signature", "")
        print(f"[Pump.fun] 初期化完了 sig={last_signature[:20]}")
    else:
        print("[Pump.fun] 初期化失敗（RPC応答なし）→ 次回から取得")

    while True:
        try:
            check_pumpfun_onchain()
            process_retry_queue()
        except Exception as e:
            print(f"[Pump.fun] ループエラー: {e}")
        time.sleep(10)


def solana_all_monitor_loop():
    global all_solana_last_signature
    print("[Solana全般] 監視ループ開始中...")

    init_sigs = solana_rpc("getSignaturesForAddress",
                           [SPL_METADATA_PROGRAM, {"limit": 5}])
    if init_sigs:
        all_solana_last_signature = init_sigs[0].get("signature", "")
        print(f"[Solana全般] 初期化完了 sig={all_solana_last_signature[:20]}")
    else:
        print("[Solana全般] 初期化失敗（RPC応答なし）→ 次回から取得")

    while True:
        try:
            check_all_solana_onchain()
        except Exception as e:
            print(f"[Solana全般] ループエラー: {e}")
        time.sleep(60)


# ══════════════════════════════════════════════════════════════════════════════
# CEX (Bitget) 監視
# ══════════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════════
# メインループ
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print("通知ボットくん 起動中...")
    send_telegram(
        "✅ <b>通知ボットくん 起動しました！</b>\n\n"
        "📊 監視対象：\n"
        "🏦 CEX: Bitget取引所（新規上場）\n"
        "🟣 Pump.fun 早期検知（60秒・30人）\n"
        "🚀 Pump.fun 確定通知（3分・20人）\n"
        "🟡 Solana全般 早期検知（60秒・50人）\n"
        "🟢 Solana全般 確定通知（3分・50人）\n"
        "🟡 DEX: FourMeme / BSC\n"
        "🟡 DEX: BNB Chain全般（PancakeSwap V2）← 主流\n"
        "🟡 DEX: BNB Chain全般（PancakeSwap V3）\n"
        "🔵 DEX: Clanker / Base\n"
        "🔵 DEX: Base全般（Uniswap V3）\n\n"
        "🔍 Solana全般監視対象：\n"
        "pump.fun / rapidlaunch.io / moonshot\n"
        "letsbonk / その他全Solana launchpad\n\n"
        "🔍 フィルター条件：\n"
        "・Pump.fun: 60秒後30人 / 3分後20人\n"
        "・Solana全般: 60秒後50人 / 3分後50人\n"
        "・BNB/Base全般: 3分後30人\n"
        "・保有者比率: 保有者<=1000 かつ 比率>=4倍 → スキップ\n"
        "・並列処理で待機中も他チェーンを継続監視"
    )

    # Pump.fun監視を独立バックグラウンドスレッドで起動
    t_pumpfun = threading.Thread(target=pumpfun_monitor_loop, daemon=True)
    t_pumpfun.start()
    print("[Pump.fun] バックグラウンドスレッド起動完了")

    # Solana全般監視を独立バックグラウンドスレッドで起動
    t_all_solana = threading.Thread(target=solana_all_monitor_loop, daemon=True)
    t_all_solana.start()
    print("[Solana全般] バックグラウンドスレッド起動完了")

    loop = 0
    while True:
        check_cex_listings()
        for chain in EVM_CHAINS:
            check_evm_chain(chain)
        for chain in EVM_ALL_CHAINS:
            check_evm_all_chain(chain)

        time.sleep(30)
        loop += 1
        if loop % 30 == 0:
            evm_status = " ".join(
                f"{c['name']}={len(c['known_tokens'])}" for c in EVM_CHAINS
            )
            all_status = " ".join(
                f"{c['name']}={len(c['known_tokens'])}" for c in EVM_ALL_CHAINS
            )
            active_threads = threading.active_count() - 1
            print(
                f"[{datetime.now().strftime('%H:%M')}] 稼働中 "
                f"CEX={len(known_cex_symbols)} "
                f"Solana={len(known_token_mints)} {evm_status} {all_status} "
                f"監視スレッド={active_threads}"
            )


if __name__ == "__main__":
    main()
