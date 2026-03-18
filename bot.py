import requests
import time
import os
import threading
from datetime import datetime

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

SPL_METADATA_PROGRAM = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"

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

CHAINLINK_BNB_USD  = "0x0567F2323251f0Aab15c8dFb1967E4e8A7D42aeE"
CHAINLINK_ETH_USD  = "0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70"

STABLE_ADDRS = {
    "0x55d398326f99059ff775485246999027b3197955",
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
    "0xe9e7cea3dedca5984780bafc599bd69add087d56",
    "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3",
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
    "0x50c5725949a6f0c72e6c4a641f24049a917db0cb",
    "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca",
}
STABLE_DECIMALS = {
    "0x55d398326f99059ff775485246999027b3197955": 18,
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d": 18,
    "0xe9e7cea3dedca5984780bafc599bd69add087d56": 18,
    "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3": 18,
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": 6,
    "0x50c5725949a6f0c72e6c4a641f24049a917db0cb": 18,
    "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca": 6,
}
NATIVE_ADDRS = {
    "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
    "0x4200000000000000000000000000000000000006",
    "0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22",
    "0x2170ed0880ac9a755fd29b2688956bd959f933f8",
}

_native_price_cache: dict = {}
PRICE_CACHE_SEC = 300

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

known_cex_symbols = set()
known_token_mints = set()
last_signature    = None
all_solana_last_signature = None

LIQUIDITY_MIN       = 25_000
TOP10_MAX_PCT       = 60.0
POLL_INTERVAL_SEC   = 10
MONITOR_TIMEOUT_SEC = 600

KNOWN_MINTS_LOCK = threading.Lock()

RETRY_SIG_QUEUE = []
RETRY_SIG_LOCK  = threading.Lock()
RETRY_EXPIRY    = 300

_SOLANA_RPS_LIMIT  = 8
_solana_rpc_times  = []
_solana_rpc_lock   = threading.Lock()


def _wait_for_rpc_slot():
    global _solana_rpc_times
    while True:
        with _solana_rpc_lock:
            now = time.time()
            _solana_rpc_times = [t for t in _solana_rpc_times if now - t < 1.0]
            if len(_solana_rpc_times) < _SOLANA_RPS_LIMIT:
                _solana_rpc_times.append(now)
                return
        time.sleep(0.05)


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
                        print(f"[{chain['name']}] RPC Error ({rpc_url.split('/')[2]}): {data['error']}")
                        break
                    return data.get("result")
                print(f"[{chain['name']}] HTTP {r.status_code} ({rpc_url.split('/')[2]}) → 次のRPCへ")
                break
            except Exception as e:
                print(f"[{chain['name']}] 接続エラー ({rpc_url.split('/')[2]}) attempt{attempt+1}: {e}")
                if attempt < 1:
                    time.sleep(1)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# オンチェーン流動性チェック
# ══════════════════════════════════════════════════════════════════════════════

def _get_native_price_usd(chain):
    global _native_price_cache
    chain_name = chain["name"]
    now = time.time()
    cached = _native_price_cache.get(chain_name)
    if cached and now - cached[1] < PRICE_CACHE_SEC:
        return cached[0]
    feed = CHAINLINK_BNB_USD if "BSC" in chain_name or "BNB" in chain_name else CHAINLINK_ETH_USD
    result = evm_rpc(chain, "eth_call", [{"to": feed, "data": "0xfeaf968c"}, "latest"])
    if result and len(result) >= 130:
        answer = int(result[66:130], 16)
        price  = answer / 1e8
        _native_price_cache[chain_name] = (price, now)
        print(f"[{chain_name}] ネイティブ価格更新: ${price:,.0f}")
        return price
    return cached[0] if cached else None


def _get_v2_pair_liquidity_usd(pair_addr, token0, token1, chain):
    try:
        r = evm_rpc(chain, "eth_call", [{"to": pair_addr, "data": "0x0902f1ac"}, "latest"])
        if not r or len(r) < 130:
            return None
        d        = r[2:]
        reserve0 = int(d[0:64],  16)
        reserve1 = int(d[64:128], 16)
        if reserve0 == 0 and reserve1 == 0:
            return None
        t0, t1 = token0.lower(), token1.lower()
        if t1 in STABLE_ADDRS:
            return reserve1 / (10 ** STABLE_DECIMALS[t1])
        if t0 in STABLE_ADDRS:
            return reserve0 / (10 ** STABLE_DECIMALS[t0])
        if t1 in NATIVE_ADDRS:
            p = _get_native_price_usd(chain)
            return (reserve1 / 1e18) * p if p else None
        if t0 in NATIVE_ADDRS:
            p = _get_native_price_usd(chain)
            return (reserve0 / 1e18) * p if p else None
        return None
    except Exception as e:
        print(f"[{chain['name']}] getReserves エラー: {e}")
        return None


def _get_v3_pool_liquidity_usd(pool_addr, token0, token1, chain):
    def _balance_of(token_addr):
        data = "0x70a08231" + "000000000000000000000000" + pool_addr[2:].lower().zfill(40)
        res  = evm_rpc(chain, "eth_call", [{"to": token_addr, "data": data}, "latest"])
        return int(res[2:66], 16) if res and len(res) >= 66 else 0
    try:
        t0, t1 = token0.lower(), token1.lower()
        if t1 in STABLE_ADDRS:
            return _balance_of(t1) / (10 ** STABLE_DECIMALS[t1])
        if t0 in STABLE_ADDRS:
            return _balance_of(t0) / (10 ** STABLE_DECIMALS[t0])
        if t1 in NATIVE_ADDRS:
            p = _get_native_price_usd(chain)
            return (_balance_of(t1) / 1e18) * p if p else None
        if t0 in NATIVE_ADDRS:
            p = _get_native_price_usd(chain)
            return (_balance_of(t0) / 1e18) * p if p else None
        return None
    except Exception as e:
        print(f"[{chain['name']}] V3 balanceOf エラー: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# EVM トークン処理スレッド
# ══════════════════════════════════════════════════════════════════════════════

def _process_evm_token(token_address, chain, from_block,
                        pair_addr=None, token0=None, token1=None, is_v2=False):
    try:
        if pair_addr and token0 and token1:
            liq = None
            for attempt in range(3):
                if is_v2:
                    liq = _get_v2_pair_liquidity_usd(pair_addr, token0, token1, chain)
                else:
                    liq = _get_v3_pool_liquidity_usd(pair_addr, token0, token1, chain)
                if liq is not None and liq > 0:
                    break
                if attempt < 2:
                    time.sleep(3)
                    print(f"[{chain['name']}] 流動性リトライ {attempt+2}/3: {pair_addr[:12]}")

            liq_str = f"${liq:,.0f}" if liq else "$0"
            print(f"[{chain['name']}] オンチェーン流動性: {liq_str} ({token_address[:12]})")

            if liq is None or liq < LIQUIDITY_MIN:
                print(f"[{chain['name']}] 流動性不足 → スキップ: {token_address[:12]}")
                return

            holder_data = get_evm_holder_stats(token_address, chain, from_block)
            if holder_data is None:
                print(f"[{chain['name']}] 保有データ取得失敗 → スキップ")
                return

            top10 = holder_data["top10_ratio"]
            print(f"[{chain['name']}] トップ10保有率: {top10:.1f}%")
            if top10 > TOP10_MAX_PCT:
                print(f"[{chain['name']}] ❌ 保有集中高すぎ → スキップ")
                return

            dex         = analyze_dexscreener(token_address)
            liq_display = dex["liquidity"] if dex else liq
            dex_extra   = (
                f"📈 価格変動: {dex['price_change_5m']:+.1f}%/5分\n"
                f"🛒 買い{dex['buys_5m']}件 / 売り{dex['sells_5m']}件 (5分)\n"
            ) if dex else ""
            holder_text, holder_judge = format_holder_output(holder_data)
            launch_line = f"🔗 {chain['launch_url']}\n" if chain.get("launch_url") else ""
            msg = (
                f"{chain['emoji']} <b>[{chain['name']}] 新規トークン検知！</b>\n\n"
                f"時刻: {datetime.now().strftime('%H:%M:%S')}\n"
                f"アドレス: <code>{token_address}</code>\n\n"
                f"💧 流動性: <b>${liq_display:,.0f}</b>\n"
                f"{dex_extra}\n"
                f"{holder_text}\n"
                f"{holder_judge}\n\n"
                f"📊 {chain['dex_url'].format(token_address)}\n"
                f"{launch_line}"
            )
            send_telegram(msg)
            print(f"[{chain['name']}] ✅ 通知送信完了: {token_address[:16]}")
            return

        deadline = time.time() + MONITOR_TIMEOUT_SEC
        print(f"[{chain['name']}] DexScreener監視開始: {token_address[:16]}")

        while time.time() < deadline:
            dex = analyze_dexscreener(token_address)
            if not dex:
                time.sleep(POLL_INTERVAL_SEC)
                continue
            liq = dex["liquidity"]
            print(f"[{chain['name']}] 流動性: ${liq:,.0f} ({token_address[:12]})")
            if liq >= LIQUIDITY_MIN:
                holder_data = get_evm_holder_stats(token_address, chain, from_block)
                if holder_data is None:
                    time.sleep(POLL_INTERVAL_SEC)
                    continue
                top10 = holder_data["top10_ratio"]
                if top10 > TOP10_MAX_PCT:
                    print(f"[{chain['name']}] ❌ 保有集中高すぎ → スキップ")
                    return
                holder_text, holder_judge = format_holder_output(holder_data)
                launch_line = f"🔗 {chain['launch_url']}\n" if chain.get("launch_url") else ""
                msg = (
                    f"{chain['emoji']} <b>[{chain['name']}] 新規トークン検知！</b>\n\n"
                    f"時刻: {datetime.now().strftime('%H:%M:%S')}\n"
                    f"アドレス: <code>{token_address}</code>\n\n"
                    f"{_build_dex_text(dex)}"
                    f"{holder_text}\n"
                    f"{holder_judge}\n\n"
                    f"📊 {chain['dex_url'].format(token_address)}\n"
                    f"{launch_line}"
                )
                send_telegram(msg)
                print(f"[{chain['name']}] ✅ 通知送信完了: {token_address[:16]}")
                return
            time.sleep(POLL_INTERVAL_SEC)

        print(f"[{chain['name']}] タイムアウト → スキップ: {token_address[:16]}")

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
                args=(token_address, chain, latest_int),
                daemon=True,
            )
            t.start()
    except Exception as e:
        print(f"[{chain['name']}] チェックエラー: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# EVM 全般監視
# ══════════════════════════════════════════════════════════════════════════════

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
        is_v2       = (event_topic == PAIR_CREATED_TOPIC)
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
            raw_data  = log.get("data", "0x")
            pair_addr = None
            try:
                if is_v2 and len(raw_data) >= 66:
                    pair_addr = "0x" + raw_data[26:66]
                elif not is_v2 and len(raw_data) >= 130:
                    pair_addr = "0x" + raw_data[90:130]
            except Exception:
                pair_addr = None
            chain["known_tokens"].add(new_token)
            print(f"[{chain['name']}] 新規トークン → スレッド起動: {new_token}"
                  f" pair={pair_addr[:12] if pair_addr else 'None'}")
            t = threading.Thread(
                target=_process_evm_token,
                args=(new_token, chain, latest_int),
                kwargs={"pair_addr": pair_addr, "token0": token0,
                        "token1": token1, "is_v2": is_v2},
                daemon=True,
            )
            t.start()
    except Exception as e:
        print(f"[{chain['name']}] チェックエラー: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# SOLANA RPC
# ══════════════════════════════════════════════════════════════════════════════

def solana_rpc(method, params):
    _wait_for_rpc_slot()
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
                _wait_for_rpc_slot()
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


def get_solana_holder_stats(mint):
    try:
        supply_result = solana_rpc("getTokenSupply", [mint])
        if not supply_result:
            return None
        total_supply = float(supply_result["value"]["amount"])
        if total_supply == 0:
            return None
        accounts_result = solana_rpc("getTokenLargestAccounts", [mint])
        if not accounts_result:
            return None
        accounts = accounts_result["value"][:10]
        top10_detail = []
        top10_total  = 0
        for i, acc in enumerate(accounts):
            amount = float(acc["amount"])
            ratio  = amount / total_supply * 100
            short  = acc["address"][:6] + "..." + acc["address"][-4:]
            top10_detail.append(f"  {'ABCDEFGHIJ'[i]}. {short}: {ratio:.1f}%")
            top10_total += amount
        top10_ratio = top10_total / total_supply * 100
        return {"top10_ratio": top10_ratio, "top10_detail": top10_detail}
    except Exception as e:
        print(f"[保有量取得エラー] {e}")
        return None


def format_holder_output(holder_data):
    if not holder_data:
        return "👛 保有者データ取得中...\n", ""
    top10_lines = "\n".join(holder_data["top10_detail"])
    text = (
        f"👛 <b>保有者分析</b> (実際の保有量)\n"
        f"上位10保有者合計: {holder_data['top10_ratio']:.0f}%\n"
        f"{top10_lines}\n"
    )
    top10_ratio = holder_data["top10_ratio"]
    if top10_ratio >= 50:
        judge = "🚨 保有集中度高め"
    elif top10_ratio < 30:
        judge = "✅ 分散した保有"
    else:
        judge = "🟡 やや集中気味"
    return text, judge


def get_evm_holder_stats(token_address, chain, from_block):
    try:
        latest_hex = evm_rpc(chain, "eth_blockNumber", [])
        if not latest_hex:
            return None
        latest = int(latest_hex, 16)
        logs = evm_rpc(chain, "eth_getLogs", [{
            "fromBlock": hex(from_block),
            "toBlock":   hex(latest),
            "address":   token_address,
            "topics":    [TRANSFER_TOPIC],
        }])
        if not logs:
            return None
        balances  = {}
        ZERO_ADDR = "0x" + "0" * 40
        for log in logs:
            topics = log.get("topics", [])
            data   = log.get("data", "0x")
            if len(topics) < 3:
                continue
            from_addr = "0x" + topics[1][-40:].lower()
            to_addr   = "0x" + topics[2][-40:].lower()
            try:
                amount = int(data, 16) if data and data != "0x" else 0
            except Exception:
                amount = 0
            if amount == 0:
                continue
            if from_addr != ZERO_ADDR:
                balances[from_addr] = balances.get(from_addr, 0) - amount
            if to_addr != ZERO_ADDR:
                balances[to_addr]   = balances.get(to_addr, 0)   + amount
        positive = {addr: bal for addr, bal in balances.items() if bal > 0}
        if not positive:
            return None
        total = sum(positive.values())
        if total == 0:
            return None
        sorted_holders = sorted(positive.items(), key=lambda x: x[1], reverse=True)
        top10          = sorted_holders[:10]
        top10_total    = sum(bal for _, bal in top10)
        top10_ratio    = top10_total / total * 100
        top10_detail = []
        for i, (addr, bal) in enumerate(top10):
            ratio = bal / total * 100
            short = addr[:6] + "..." + addr[-4:]
            top10_detail.append(f"  {'ABCDEFGHIJ'[i]}. {short}: {ratio:.1f}%")
        return {"top10_ratio": top10_ratio, "top10_detail": top10_detail}
    except Exception as e:
        print(f"[{chain['name']}] EVM保有量トップ10取得エラー: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# Solana トークン処理スレッド
# ══════════════════════════════════════════════════════════════════════════════

def _process_solana_token(mint, label="Pump.fun", pump_link=True):
    try:
        deadline = time.time() + MONITOR_TIMEOUT_SEC
        print(f"[{label}] 閾値監視開始: {mint[:20]}"
              f" (流動性${LIQUIDITY_MIN:,}+, トップ10≤{TOP10_MAX_PCT}%)")
        while time.time() < deadline:
            dex = analyze_dexscreener(mint)
            if not dex:
                time.sleep(POLL_INTERVAL_SEC)
                continue
            liq = dex['liquidity']
            print(f"[{label}] 流動性: ${liq:,.0f} / 閾値${LIQUIDITY_MIN:,} ({mint[:16]})")
            if liq >= LIQUIDITY_MIN:
                holder_data = get_solana_holder_stats(mint)
                if holder_data is None:
                    time.sleep(POLL_INTERVAL_SEC)
                    continue
                top10 = holder_data["top10_ratio"]
                print(f"[{label}] トップ10保有率: {top10:.1f}%")
                if top10 > TOP10_MAX_PCT:
                    print(f"[{label}] ❌ 保有集中高すぎ ({top10:.1f}% > {TOP10_MAX_PCT}%) → スキップ")
                    return
                platform     = _get_platform_name(dex)
                dex_text     = _build_dex_text(dex)
                holder_text, holder_judge = format_holder_output(holder_data)
                pump_line = (
                    f"📱 <a href=\"https://pump.fun/{mint}\">pump.fun（Bitget Walletで開く）</a>"
                    if pump_link else
                    f"🔍 https://solscan.io/token/{mint}"
                )
                msg = (
                    f"🚀 <b>[Solana/{platform}] 新規トークン検知！</b>\n\n"
                    f"時刻: {datetime.now().strftime('%H:%M:%S')}\n"
                    f"📋 Mintアドレス（タップでコピー）\n"
                    f"<code>{mint}</code>\n\n"
                    f"{dex_text}"
                    f"{holder_text}\n"
                    f"{holder_judge}\n\n"
                    f"📊 https://dexscreener.com/solana/{mint}\n"
                    f"{pump_line}"
                )
                send_telegram(msg)
                print(f"[{label}] ✅ 通知送信完了: {mint[:20]}")
                return
            time.sleep(POLL_INTERVAL_SEC)
        print(f"[{label}] タイムアウト(10分) → スキップ: {mint[:20]}")
    except Exception as e:
        print(f"[{label}] スレッドエラー ({mint[:20]}): {e}")


# ══════════════════════════════════════════════════════════════════════════════
# Pump.fun 並列監視
# ══════════════════════════════════════════════════════════════════════════════

def _handle_pumpfun_sig(sig):
    """Pump.fun シグネチャを並列処理するワーカー関数"""
    mint = parse_new_token(sig)
    if mint is False:
        with RETRY_SIG_LOCK:
            RETRY_SIG_QUEUE.append((sig, time.time()))
        print(f"[Pump.fun] リトライ予約: {sig[:20]}")
        return
    if not mint:
        return
    with KNOWN_MINTS_LOCK:
        if mint in known_token_mints:
            return
        known_token_mints.add(mint)
    print(f"[Pump.fun] 新規mint → スレッド起動: {mint[:20]}")
    t = threading.Thread(target=_process_solana_token, args=(mint,), daemon=True)
    t.start()


def check_pumpfun_onchain():
    """新規mintを並列で一気に処理。_wait_for_rpc_slot()でRPS制限管理。"""
    global known_token_mints
    txns = get_new_pumpfun_transactions()
    if not txns:
        return
    now = time.time()
    before_filter = len(txns)
    txns = [tx for tx in txns
            if not tx.get("blockTime") or (now - tx["blockTime"]) <= 300]
    if len(txns) < before_filter:
        print(f"[Pump.fun] 古いTX除外: {before_filter - len(txns)}件スキップ（残り{len(txns)}件）")
    sigs = [tx.get("signature", "") for tx in txns
            if tx.get("signature") and not tx.get("err")]
    if not sigs:
        return
    print(f"[Pump.fun] {len(sigs)}件を並列処理開始")
    workers = [threading.Thread(target=_handle_pumpfun_sig, args=(sig,), daemon=True)
               for sig in sigs]
    for w in workers:
        w.start()


# ══════════════════════════════════════════════════════════════════════════════
# Pump.fun リトライキュー
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
        t = threading.Thread(target=_process_solana_token, args=(mint,), daemon=True)
        t.start()
    if still_failed:
        with RETRY_SIG_LOCK:
            RETRY_SIG_QUEUE.extend(still_failed)
        print(f"[Pump.fun] リトライ再キュー: {len(still_failed)}件")


# ══════════════════════════════════════════════════════════════════════════════
# Solana 全般並列監視
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
        if len(all_txns) >= 200:
            print("[Solana全般] ページネーション上限200件 → 打ち切り")
            break
        before = result[-1].get("signature")
        time.sleep(0.1)
    if all_txns:
        all_solana_last_signature = all_txns[0].get("signature", "")
        if len(all_txns) > 1:
            print(f"[Solana全般] {len(all_txns)}件の新規TX検出")
    return all_txns


def _get_platform_name(dex):
    if not dex:
        return "Unknown"
    dex_id = dex.get("dex_id", "").lower()
    name_map = {
        "raydium": "Raydium", "pump-fun": "pump.fun", "pumpfun": "pump.fun",
        "orca": "Orca", "meteora": "Meteora", "jupiter": "Jupiter",
        "rapidlaunch": "rapidlaunch.io", "moonshot": "Moonshot", "letsbonk": "LetsBonk",
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


def _handle_metadata_sig(sig):
    """Solana全般 シグネチャを並列処理するワーカー関数"""
    mint = parse_new_fungible_mint(sig)
    if mint is False or not mint:
        return
    with KNOWN_MINTS_LOCK:
        if mint in known_token_mints:
            return
        known_token_mints.add(mint)
    print(f"[Solana全般] 新規ファンジブルmint → スレッド起動: {mint[:20]}")
    t = threading.Thread(target=_process_solana_token, args=(mint, "Solana全般", False), daemon=True)
    t.start()


def check_all_solana_onchain():
    """Token Metadata Program の新規TXを並列で一気に処理。"""
    global known_token_mints
    txns = get_new_metadata_transactions()
    if not txns:
        return
    now = time.time()
    before_filter = len(txns)
    txns = [tx for tx in txns
            if not tx.get("blockTime") or (now - tx["blockTime"]) <= 300]
    if len(txns) < before_filter:
        print(f"[Solana全般] 古いTX除外: {before_filter - len(txns)}件スキップ（残り{len(txns)}件）")
    sigs = [tx.get("signature", "") for tx in txns
            if tx.get("signature") and not tx.get("err")]
    if not sigs:
        return
    print(f"[Solana全般] {len(sigs)}件を並列処理開始")
    workers = [threading.Thread(target=_handle_metadata_sig, args=(sig,), daemon=True)
               for sig in sigs]
    for w in workers:
        w.start()


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
            return {item["symbol"] for item in data["data"] if item.get("status") == "online"}
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
        "🚀 Pump.fun / Solana全般（全launchpad）\n"
        "🟡 DEX: FourMeme / BSC\n"
        "🟡 DEX: BNB Chain全般（PancakeSwap V2/V3）\n"
        "🔵 DEX: Clanker / Base\n"
        "🔵 DEX: Base全般（Uniswap V3）\n\n"
        f"⚡ 新戦略: プール作成時オンチェーン即時チェック\n"
        f"💧 初期流動性 ${LIQUIDITY_MIN:,}+ AND\n"
        f"👛 トップ10保有率 ≤{TOP10_MAX_PCT:.0f}%\n"
        f"🔄 EVM検知5秒毎 / 通知まで平均10〜15秒\n\n"
        "🔍 Solana監視対象：\n"
        "pump.fun / rapidlaunch.io / moonshot\n"
        "letsbonk / その他全Solana launchpad"
    )

    t_pumpfun = threading.Thread(target=pumpfun_monitor_loop, daemon=True)
    t_pumpfun.start()
    print("[Pump.fun] バックグラウンドスレッド起動完了")

    t_all_solana = threading.Thread(target=solana_all_monitor_loop, daemon=True)
    t_all_solana.start()
    print("[Solana全般] バックグラウンドスレッド起動完了")

    loop = 0
    while True:
        if loop % 6 == 0:
            check_cex_listings()
        for chain in EVM_CHAINS:
            check_evm_chain(chain)
        for chain in EVM_ALL_CHAINS:
            check_evm_all_chain(chain)
        time.sleep(5)
        loop += 1
        if loop % 360 == 0:
            evm_status = " ".join(f"{c['name']}={len(c['known_tokens'])}" for c in EVM_CHAINS)
            all_status = " ".join(f"{c['name']}={len(c['known_tokens'])}" for c in EVM_ALL_CHAINS)
            active_threads = threading.active_count() - 1
            print(
                f"[{datetime.now().strftime('%H:%M')}] 稼働中 "
                f"CEX={len(known_cex_symbols)} "
                f"Solana={len(known_token_mints)} {evm_status} {all_status} "
                f"監視スレッド={active_threads}"
            )


if __name__ == "__main__":
    main()
