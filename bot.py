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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}

known_cex_symbols = set()
known_token_mints = set()
last_signature    = None
all_solana_last_signature = None

LIQUIDITY_MIN       = 50_000
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
                        print(f"[{chain['name']}] RPC Error: {data['error']}")
                        break
                    return data.get("result")
                print(f"[{chain['name']}] HTTP {r.status_code} → 次のRPCへ")
                break
            except Exception as e:
                print(f"[{chain['name']}] 接続エラー attempt{attempt+1}: {e}")
                if attempt < 1:
                    time.sleep(1)
    return None


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
        reserve0 = int(d[0:64],   16)
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


def _process_evm_token(token_address, chain, from_block,
                        pair_addr=None, token0=None, token1=None, is_v2=False):
    try:
        # ── オンチェーン直接チェック ──────────────────────────────────────────
        if pair_addr and token0 and token1:
            liq = None
            for attempt in range(3):
                liq = (_get_v2_pair_liquidity_usd(pair_addr, token0, token1, chain)
                       if is_v2 else
                       _get_v3_pool_liquidity_usd(pair_addr, token0, token1, chain))
                if liq is not None and liq > 0:
                    break
                if attempt < 2:
                    time.sleep(3)
                    print(f"[{chain['name']}] 流動性リトライ {attempt+2}/3")

            liq_str = f"${liq:,.0f}" if liq else "$0"
            print(f"[{chain['name']}] オンチェーン流動性: {liq_str} ({token_address[:12]})")

            if liq is None or liq < LIQUIDITY_MIN:
                print(f"[{chain['name']}] 流動性不足 → スキップ")
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

        # ── フォールバック: DexScreenerポーリング ────────────────────────────
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
            from_block = latest_int - 500
        if from_block > latest_int:
            return

        event_topic = chain.get("topic", POOL_CREATED_TOPIC)
        is_v2       = (event_topic == PAIR_CREATED_TOPIC)
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

            # pair/poolアドレス抽出
            # V2: data = pair(32B)+uint(32B) → pair = data[26:66]
            # V3: data = tickSpacing(32B)+pool(32B) → pool = data[90:130]
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
