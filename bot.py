import requests
import time
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# ── 環境変数 ──────────────────────────────────────────────────────────────────
BOT_TOKEN  = os.environ.get('BOT_TOKEN')
CHAT_ID    = os.environ.get('CHAT_ID')
HELIUS_KEY = os.environ.get('HELIUS_API_KEY', '')

# ── SNS監視（Telegram）──────────────────────────────────────────────────────
TELEGRAM_MONITOR_CHATS = [
    int(x.strip()) for x in os.environ.get('TELEGRAM_MONITOR_CHATS', '').split(',')
    if x.strip().lstrip('-').isdigit()
]

# ── エンドポイント ────────────────────────────────────────────────────────────
BITGET_SYMBOLS_URL  = "https://api.bitget.com/api/v2/spot/public/symbols"

# Solana RPC ラウンドロビンリスト
# ※除外済み: rpc.ankr.com/solana(403), go.getblock.io(404),
#            solana.drpc.org(521 Cloudflareダウン), endpoints.omniatech.io(400 freetier不可)
_SOLANA_RPC_LIST = [r for r in [
    f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}" if HELIUS_KEY else None,
    "https://solana.publicnode.com",
    "https://solana-rpc.publicnode.com",
    "https://api.mainnet-beta.solana.com",
] if r]
SOLANA_RPC = _SOLANA_RPC_LIST[0]
_solana_rpc_idx      = 0
_solana_rpc_idx_lock = threading.Lock()

# ── EVM定数 ──────────────────────────────────────────────────────────────────
TRANSFER_TOPIC  = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO_TOPIC      = "0x0000000000000000000000000000000000000000000000000000000000000000"
PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

SPL_METADATA_PROGRAM = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"

RAYDIUM_AMM_V4    = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
RAYDIUM_CPMM      = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"
WSOL_MINT         = "So11111111111111111111111111111111111111112"
USDC_MINT         = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT         = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
SOLANA_BASE_TOKENS = {
    WSOL_MINT,
    USDC_MINT,
    USDT_MINT,
    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",
    "7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj",
}

POOL_CREATED_TOPIC      = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"
PANCAKE_V3_FACTORY_BSC  = "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"
UNISWAP_V3_FACTORY_BASE = "0x33128a8fC17869897dcE68Ed026d694621f6FDfD"
PAIR_CREATED_TOPIC      = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
PANCAKE_V2_FACTORY_BSC  = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"

V2_MINT_TOPIC = "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f"
V3_MINT_TOPIC = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"

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
            "https://mainnet.base.org",
            "https://base.drpc.org",
            "https://base-rpc.publicnode.com",
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
        "top10_max_pct": 80.0,
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
            "https://mainnet.base.org",
            "https://base.drpc.org",
            "https://base-rpc.publicnode.com",
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

_ca_watch      = {}
_ca_watch_lock = threading.Lock()

_pending_tokens      = {}
_pending_lock        = threading.Lock()

_notified_tokens     = set()
_notified_lock       = threading.Lock()

_tg_update_offset = None
_tg_update_lock   = threading.Lock()

_EVM_ADDR_RE = re.compile(r'0x[a-fA-F0-9]{40}')
_SOL_ADDR_RE = re.compile(
    r'(?:CA|ca|contract|mint|address|token|アドレス|Contract Address)'
    r'\s*[:：]\s*([1-9A-HJ-NP-Za-km-z]{32,44})'
)
last_signature    = None
all_solana_last_signature = None

raydium_last_sigs = {RAYDIUM_AMM_V4: None, RAYDIUM_CPMM: None}

_sol_price_cache  = [None, 0.0]

LIQUIDITY_MIN       = 10_000
TOP10_MAX_PCT       = 60.0
POLL_INTERVAL_SEC   = 3
MONITOR_TIMEOUT_SEC = 300

KNOWN_MINTS_LOCK = threading.Lock()

_SOLANA_SEMAPHORE  = threading.Semaphore(6)
_MONITOR_SEMAPHORE = threading.Semaphore(50)

_PUMPFUN_POOL    = ThreadPoolExecutor(max_workers=8, thread_name_prefix="pumpfun")
_SOLANA_ALL_POOL = ThreadPoolExecutor(max_workers=8, thread_name_prefix="solana_all")
_RAYDIUM_POOL    = ThreadPoolExecutor(max_workers=8, thread_name_prefix="raydium")

RETRY_SIG_QUEUE = []
RETRY_SIG_LOCK  = threading.Lock()
RETRY_EXPIRY    = 300

_SOLANA_RPS_LIMIT  = 24
_solana_rpc_times  = []
_solana_rpc_lock   = threading.Lock()


def _wrapped_process_solana(mint, label="Pump.fun", is_pumpfun=True):
    with _MONITOR_SEMAPHORE:
        _process_solana_token(mint, label, is_pumpfun)


def _wrapped_process_raydium(mint, liq_usd):
    with _MONITOR_SEMAPHORE:
        _process_raydium_token(mint, liq_usd)


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


def _get_sol_price_usd():
    global _sol_price_cache
    now = time.time()
    if _sol_price_cache[0] and now - _sol_price_cache[1] < PRICE_CACHE_SEC:
        return _sol_price_cache[0]
    try:
        r = requests.get("https://price.jup.ag/v6/price?ids=SOL", headers=HEADERS, timeout=8)
        if r.status_code == 200:
            data = r.json()
            price = float(data.get("data", {}).get("SOL", {}).get("price", 0))
            if price > 0:
                _sol_price_cache[0] = price
                _sol_price_cache[1] = now
                print(f"[SOL価格] ${price:,.2f}")
                return price
    except Exception as e:
        print(f"[SOL価格] 取得エラー: {e}")
    return _sol_price_cache[0] if _sol_price_cache[0] else 150.0


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
            "image_url":       (pair.get("info") or {}).get("imageUrl", ""),
        }
    except Exception as e:
        print(f"DexScreenerエラー: {e}")
        return None


def analyze_pumpfun_api(mint):
    _pf_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://pump.fun/",
        "Origin": "https://pump.fun",
        "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
    }
    try:
        r = requests.get(f"https://frontend-api.pump.fun/coins/{mint}",
                         headers=_pf_headers, timeout=10)
        if r.status_code != 200:
            print(f"[Pump.fun API] HTTP {r.status_code} ({mint[:16]})")
            return None
        data = r.json()
        if not data:
            return None
        return {
            "liquidity": float(data.get("usd_market_cap") or 0),
            "complete":  bool(data.get("complete", False)),
            "name":      data.get("name", ""),
            "symbol":    data.get("symbol", ""),
        }
    except Exception as e:
        print(f"[Pump.fun API] エラー: {e}")
        return None


def _has_token_icon(key: str, chain: str, dex: dict = None) -> bool:
    """
    【EVM（BSC/Base）】: アイコンチェックをスキップ → 常にTrue
      理由: DexScreener info.imageUrl はプロジェクトが手動登録した場合のみ
            セットされる。新規EVM トークンの99%は未登録 → 全スキップになる。
            EVMは流動性・保有率フィルターで品質担保する。

    【Solana】:
      1. 渡されたdexデータの image_url → 即判定
      2. pump.fun API の image_uri     → フォールバック
      3. DexScreener の info.imageUrl  → フォールバック
    """
    # EVM チェーンはアイコン不要（流動性・保有率で担保）
    if chain != "sol":
        return True

    if dex and dex.get("image_url"):
        return True

    if chain == "sol":
        try:
            _pf_headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                              " (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Referer": "https://pump.fun/",
                "Origin": "https://pump.fun",
            }
            r = requests.get(f"https://frontend-api.pump.fun/coins/{key}",
                             headers=_pf_headers, timeout=8)
            if r.status_code == 200:
                data = r.json()
                if data and data.get("image_uri"):
                    print(f"[アイコン] pump.fun image_uri あり: {key[:20]}")
                    return True
        except Exception:
            pass

    try:
        r = requests.get(f"https://api.dexscreener.com/latest/dex/tokens/{key}",
                         headers=HEADERS, timeout=8)
        if r.status_code == 200:
            for pair in (r.json().get("pairs") or []):
                if (pair.get("info") or {}).get("imageUrl"):
                    print(f"[アイコン] DexScreener imageUrl あり: {key[:20]}")
                    return True
    except Exception:
        pass

    print(f"[アイコン] なし → スキップ: {key[:20]}")
    return False


def evm_rpc(chain, method, params):
    rpc_list = chain.get("rpc_list") or [chain.get("rpc", "")]
    for rpc_url in rpc_list:
        for attempt in range(2):
            try:
                r = requests.post(rpc_url, json={
                    "jsonrpc": "2.0", "id": 1,
                    "method": method, "params": params,
                }, timeout=8)
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


def _get_v2_pair(token_a, token_b, factory, chain):
    ta   = token_a[2:].lower().zfill(40)
    tb   = token_b[2:].lower().zfill(40)
    data = "0xe6a43905" + "000000000000000000000000" + ta + "000000000000000000000000" + tb
    res  = evm_rpc(chain, "eth_call", [{"to": factory, "data": data}, "latest"])
    if res and len(res) >= 66:
        addr = "0x" + res[-40:]
        if addr.lower() != "0x" + "0" * 40:
            return addr
    return None


def _get_v3_pool_addr(token_a, token_b, factory, fee, chain):
    ta      = token_a[2:].lower().zfill(40)
    tb      = token_b[2:].lower().zfill(40)
    fee_hex = hex(fee)[2:].zfill(64)
    data    = "0x1698ee82" + "000000000000000000000000" + ta + "000000000000000000000000" + tb + fee_hex
    res     = evm_rpc(chain, "eth_call", [{"to": factory, "data": data}, "latest"])
    if res and len(res) >= 66:
        addr = "0x" + res[-40:]
        if addr.lower() != "0x" + "0" * 40:
            return addr
    return None


def _find_pair_address(token_addr, chain):
    ta  = token_addr.lower()
    is_bsc = "BSC" in chain["name"] or "BNB" in chain["name"] or "FourMeme" in chain["name"]
    if is_bsc:
        native     = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
        factory_v2 = PANCAKE_V2_FACTORY_BSC
        factory_v3 = PANCAKE_V3_FACTORY_BSC
        v3_fees    = (500, 2500, 10000, 100)
    else:
        native     = "0x4200000000000000000000000000000000000006"
        factory_v2 = None
        factory_v3 = UNISWAP_V3_FACTORY_BASE
        v3_fees    = (500, 3000, 10000, 100)
    nb = native.lower()
    t0, t1 = (ta, nb) if ta < nb else (nb, ta)
    if factory_v2:
        pair = _get_v2_pair(ta, native, factory_v2, chain)
        if pair:
            return pair, t0, t1, True
    for fee in v3_fees:
        pool = _get_v3_pool_addr(ta, native, factory_v3, fee, chain)
        if pool:
            return pool, t0, t1, False
    return None


def _wait_for_liquidity_mint(pair_addr, token_address, chain, from_block,
                              token0, token1, is_v2):
    mint_topic         = V2_MINT_TOPIC if is_v2 else V3_MINT_TOPIC
    deadline           = time.time() + MONITOR_TIMEOUT_SEC
    last_checked_block = from_block
    print(f"[{chain['name']}] Mint監視開始: {pair_addr[:12]} ({token_address[:12]})")
    while time.time() < deadline:
        time.sleep(3)
        latest_hex = evm_rpc(chain, "eth_blockNumber", [])
        if not latest_hex:
            continue
        latest_int = int(latest_hex, 16)
        if latest_int <= last_checked_block:
            continue
        logs = evm_rpc(chain, "eth_getLogs", [{
            "fromBlock": hex(last_checked_block + 1),
            "toBlock":   hex(latest_int),
            "address":   pair_addr,
            "topics":    [mint_topic],
        }])
        last_checked_block = latest_int
        if logs:
            print(f"[{chain['name']}] ⚡ Mint検知！即getReserves: {pair_addr[:12]}")
        if is_v2:
            liq = _get_v2_pair_liquidity_usd(pair_addr, token0, token1, chain)
        else:
            liq = _get_v3_pool_liquidity_usd(pair_addr, token0, token1, chain)
        if liq is None or liq < LIQUIDITY_MIN:
            continue
        print(f"[{chain['name']}] 流動性OK: ${liq:,.0f} → 保有率チェックへ")
        holder_result = [None]
        dex_result    = [None]
        def _fetch_holder():
            holder_result[0] = get_evm_holder_stats(token_address, chain, from_block)
        def _fetch_dex():
            dex_result[0] = analyze_dexscreener(token_address)
        t_holder = threading.Thread(target=_fetch_holder, daemon=True)
        t_dex    = threading.Thread(target=_fetch_dex,    daemon=True)
        t_holder.start(); t_dex.start()
        t_holder.join();  t_dex.join()
        holder_data = holder_result[0]
        dex         = dex_result[0]
        if holder_data is None:
            print(f"[{chain['name']}] 保有データ取得失敗 → 監視継続")
            continue
        top10       = holder_data["top10_ratio"]
        top10_limit = chain.get("top10_max_pct", TOP10_MAX_PCT)
        print(f"[{chain['name']}] トップ10保有率: {top10:.1f}% (上限{top10_limit:.0f}%)")
        if top10 > top10_limit:
            print(f"[{chain['name']}] ❌ 保有集中高すぎ → スキップ")
            return
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
        with _notified_lock:
            _notified_tokens.add(token_address.lower())
        print(f"[{chain['name']}] ✅ Mint検知→即通知: {token_address[:16]}")
        return
    print(f"[{chain['name']}] Mint監視タイムアウト → スキップ: {pair_addr[:12]}")


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
                    time.sleep(1)
                    print(f"[{chain['name']}] 流動性リトライ {attempt+2}/3: {pair_addr[:12]}")
            liq_str = f"${liq:,.0f}" if liq else "$0"
            print(f"[{chain['name']}] オンチェーン流動性: {liq_str} ({token_address[:12]})")
            if liq is None or liq < LIQUIDITY_MIN:
                print(f"[{chain['name']}] オンチェーン流動性不足(${liq or 0:,.0f}) → Mint監視へ: {token_address[:12]}")
                _wait_for_liquidity_mint(pair_addr, token_address, chain, from_block, token0, token1, is_v2)
                return
            else:
                holder_result = [None]
                dex_result    = [None]
                def _fetch_holder():
                    holder_result[0] = get_evm_holder_stats(token_address, chain, from_block)
                def _fetch_dex():
                    dex_result[0] = analyze_dexscreener(token_address)
                t_holder = threading.Thread(target=_fetch_holder, daemon=True)
                t_dex    = threading.Thread(target=_fetch_dex,    daemon=True)
                t_holder.start(); t_dex.start()
                t_holder.join();  t_dex.join()
                holder_data = holder_result[0]
                dex         = dex_result[0]
                if holder_data is None:
                    print(f"[{chain['name']}] 保有データ取得失敗 → 流動性のみで通知")
                    liq_display = dex["liquidity"] if dex else liq
                    dex_extra   = (
                        f"📈 価格変動: {dex['price_change_5m']:+.1f}%/5分\n"
                        f"🛒 買い{dex['buys_5m']}件 / 売り{dex['sells_5m']}件 (5分)\n"
                    ) if dex else ""
                    launch_line = f"🔗 {chain['launch_url']}\n" if chain.get("launch_url") else ""
                    msg = (
                        f"{chain['emoji']} <b>[{chain['name']}] 新規トークン検知！</b>\n\n"
                        f"時刻: {datetime.now().strftime('%H:%M:%S')}\n"
                        f"アドレス: <code>{token_address}</code>\n\n"
                        f"💧 流動性: <b>${liq_display:,.0f}</b>\n"
                        f"{dex_extra}\n"
                        f"⚠️ 保有率データ取得不可（要確認）\n\n"
                        f"📊 {chain['dex_url'].format(token_address)}\n"
                        f"{launch_line}"
                    )
                    send_telegram(msg)
                    with _notified_lock:
                        _notified_tokens.add(token_address.lower())
                    print(f"[{chain['name']}] ✅ 通知送信完了（保有率なし）: {token_address[:16]}")
                    return
                top10       = holder_data["top10_ratio"]
                top10_limit = chain.get("top10_max_pct", TOP10_MAX_PCT)
                print(f"[{chain['name']}] トップ10保有率: {top10:.1f}% (上限{top10_limit:.0f}%)")
                if top10 > top10_limit:
                    print(f"[{chain['name']}] ❌ 保有集中高すぎ → スキップ")
                    return
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
                with _notified_lock:
                    _notified_tokens.add(token_address.lower())
                print(f"[{chain['name']}] ✅ 通知送信完了: {token_address[:16]}")
                return
        deadline = time.time() + MONITOR_TIMEOUT_SEC
        print(f"[{chain['name']}] Factoryペア検索開始: {token_address[:16]}")
        while time.time() < deadline:
            found = _find_pair_address(token_address, chain)
            if found:
                f_pair, f_t0, f_t1, f_is_v2 = found
                print(f"[{chain['name']}] ⚡ ペア発見！Mint監視へ: {f_pair[:12]}")
                _wait_for_liquidity_mint(f_pair, token_address, chain, from_block, f_t0, f_t1, f_is_v2)
                return
            time.sleep(3)
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
            _register_pending_token(token_address.lower(), "evm", chain["name"])
            print(f"[{chain['name']}] 新規トークン検知 → 遅延監視に登録: {token_address}")
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
            _register_pending_token(
                new_token.lower(), "evm", chain["name"],
                pair_addr=pair_addr, token0=token0, token1=token1,
                is_v2=is_v2, evm_chain=chain,
            )
            print(f"[{chain['name']}] 新規トークン検知 → 遅延監視に登録: {new_token}"
                  f" pair={pair_addr[:12] if pair_addr else 'None'}")
    except Exception as e:
        print(f"[{chain['name']}] チェックエラー: {e}")


def solana_rpc(method, params):
    global _solana_rpc_idx
    _wait_for_rpc_slot()
    for attempt in range(4):
        with _solana_rpc_idx_lock:
            url = _SOLANA_RPC_LIST[_solana_rpc_idx % len(_SOLANA_RPC_LIST)]
            _solana_rpc_idx += 1
        try:
            r = requests.post(url, json={
                "jsonrpc": "2.0", "id": 1,
                "method": method, "params": params,
            }, timeout=15)
            if r.status_code == 200:
                return r.json().get("result")
            if r.status_code == 429:
                wait = 2 ** attempt
                rpc_name = url.split("//")[1].split("/")[0][:20]
                print(f"[Solana RPC] 429 ({rpc_name}) → {wait}秒待機 (attempt {attempt+1}/4)")
                time.sleep(wait)
                _wait_for_rpc_slot()
                continue
            if r.status_code == 403:
                rpc_name = url.split("//")[1].split("/")[0][:20]
                print(f"[Solana RPC] 403 ({rpc_name}) APIキー必須 → 次のRPCへ")
                break
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
        if len(all_txns) >= 50:
            print(f"[Pump.fun] ページネーション上限50件 → 打ち切り")
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
        effective_from = max(from_block, latest - 5000)
        logs = None
        for attempt in range(3):
            logs = evm_rpc(chain, "eth_getLogs", [{
                "fromBlock": hex(effective_from),
                "toBlock":   hex(latest),
                "address":   token_address,
                "topics":    [TRANSFER_TOPIC],
            }])
            if logs:
                break
            if attempt < 2:
                time.sleep(3)
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


def _process_solana_token(mint, label="Pump.fun", pump_link=True):
    try:
        deadline = time.time() + MONITOR_TIMEOUT_SEC
        print(f"[{label}] 閾値監視開始: {mint[:20]}"
              f" (流動性${LIQUIDITY_MIN:,}+, トップ10≤{TOP10_MAX_PCT}%)")
        effective_pump = pump_link
        if not pump_link:
            pf_probe = analyze_pumpfun_api(mint)
            if pf_probe:
                effective_pump = True
                print(f"[{label}] pump.fun API使用可 → fast path切替")
        while time.time() < deadline:
            if effective_pump:
                pf = analyze_pumpfun_api(mint)
                if pf:
                    liq = pf["liquidity"]
                    print(f"[{label}] pump.fun API 時価総額: ${liq:,.0f} ({mint[:16]})")
                    if liq >= LIQUIDITY_MIN:
                        holder_data = get_solana_holder_stats(mint)
                        if holder_data is None:
                            time.sleep(POLL_INTERVAL_SEC)
                            continue
                        top10 = holder_data["top10_ratio"]
                        print(f"[{label}] トップ10保有率: {top10:.1f}%")
                        if top10 > TOP10_MAX_PCT:
                            print(f"[{label}] ❌ 保有集中高すぎ → スキップ")
                            return
                        dex = analyze_dexscreener(mint)
                        platform = _get_platform_name(dex) if dex else "pump.fun"
                        dex_text = _build_dex_text(dex) if dex else (
                            f"💧 時価総額: ${liq:,.0f}\n"
                            f"{'🎓 Raydium卒業済み' if pf['complete'] else '📈 bonding curve中'}\n\n"
                        )
                        holder_text, holder_judge = format_holder_output(holder_data)
                        token_name = f"{pf['name']} (${pf['symbol']})\n" if pf.get("name") else ""
                        msg = (
                            f"🚀 <b>[Solana/{platform}] 新規トークン検知！</b>\n\n"
                            f"時刻: {datetime.now().strftime('%H:%M:%S')}\n"
                            f"{token_name}"
                            f"📋 Mintアドレス（タップでコピー）\n"
                            f"<code>{mint}</code>\n\n"
                            f"{dex_text}"
                            f"{holder_text}\n"
                            f"{holder_judge}\n\n"
                            f"📊 https://dexscreener.com/solana/{mint}\n"
                            f"📱 <a href=\"https://pump.fun/{mint}\">pump.fun（Bitget Walletで開く）</a>"
                        )
                        send_telegram(msg)
                        with _notified_lock:
                            _notified_tokens.add(mint)
                        print(f"[{label}] ✅ pump.fun API経由で通知: {mint[:20]}")
                        return
                    time.sleep(POLL_INTERVAL_SEC)
                    continue
                effective_pump = False
            dex = analyze_dexscreener(mint)
            if not dex:
                time.sleep(POLL_INTERVAL_SEC)
                continue
            liq = dex['liquidity']
            print(f"[{label}] DexScreener 流動性: ${liq:,.0f} ({mint[:16]})")
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
                    if effective_pump else
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
                with _notified_lock:
                    _notified_tokens.add(mint)
                print(f"[{label}] ✅ 通知送信完了: {mint[:20]}")
                return
            time.sleep(POLL_INTERVAL_SEC)
        print(f"[{label}] タイムアウト(5分) → スキップ: {mint[:20]}")
    except Exception as e:
        print(f"[{label}] スレッドエラー ({mint[:20]}): {e}")


def _handle_pumpfun_sig(sig):
    with _SOLANA_SEMAPHORE:
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
    _register_pending_token(mint, "sol", "Pump.fun")
    print(f"[Pump.fun] 新規mint → 遅延監視に登録: {mint[:20]}")


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
        print(f"[Pump.fun] 古いTX除外: {before_filter - len(txns)}件スキップ（残り{len(txns)}件）")
    sigs = [tx.get("signature", "") for tx in txns
            if tx.get("signature") and not tx.get("err")]
    if not sigs:
        return
    print(f"[Pump.fun] {len(sigs)}件を並列処理開始")
    for sig in sigs:
        _PUMPFUN_POOL.submit(_handle_pumpfun_sig, sig)


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
        _register_pending_token(mint, "sol", "Pump.fun")
        print(f"[Pump.fun] ✅ リトライ成功！mint → 遅延監視に登録: {mint[:20]}")
    if still_failed:
        with RETRY_SIG_LOCK:
            RETRY_SIG_QUEUE.extend(still_failed)
        print(f"[Pump.fun] リトライ再キュー: {len(still_failed)}件")


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


def _handle_metadata_sig(sig):
    with _SOLANA_SEMAPHORE:
        mint = parse_new_fungible_mint(sig)
    if mint is False or not mint:
        return
    with KNOWN_MINTS_LOCK:
        if mint in known_token_mints:
            return
        known_token_mints.add(mint)
    _register_pending_token(mint, "sol", "Solana全般")
    print(f"[Solana全般] 新規ファンジブルmint → 遅延監視に登録: {mint[:20]}")


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
        print(f"[Solana全般] 古いTX除外: {before_filter - len(txns)}件スキップ（残り{len(txns)}件）")
    sigs = [tx.get("signature", "") for tx in txns
            if tx.get("signature") and not tx.get("err")]
    if not sigs:
        return
    print(f"[Solana全般] {len(sigs)}件を並列処理開始")
    for sig in sigs:
        _SOLANA_ALL_POOL.submit(_handle_metadata_sig, sig)


def pumpfun_monitor_loop():
    global last_signature
    print("[Pump.fun] 監視ループ開始中...")
    if last_signature is None:
        init_sigs = solana_rpc("getSignaturesForAddress", [PUMPFUN_PROGRAM, {"limit": 5}])
        if init_sigs:
            last_signature = init_sigs[0].get("signature", "")
            print(f"[Pump.fun] 初期化完了 sig={last_signature[:20]}")
        else:
            print("[Pump.fun] 初期化失敗（RPC応答なし）→ 次回から取得")
    else:
        print(f"[Pump.fun] 起動スキャン済み → 通常監視開始")
    while True:
        try:
            check_pumpfun_onchain()
            process_retry_queue()
        except Exception as e:
            print(f"[Pump.fun] ループエラー: {e}")
        time.sleep(3)


def solana_all_monitor_loop():
    global all_solana_last_signature
    print("[Solana全般] 監視ループ開始中...")
    if all_solana_last_signature is None:
        init_sigs = solana_rpc("getSignaturesForAddress",
                               [SPL_METADATA_PROGRAM, {"limit": 5}])
        if init_sigs:
            all_solana_last_signature = init_sigs[0].get("signature", "")
            print(f"[Solana全般] 初期化完了 sig={all_solana_last_signature[:20]}")
        else:
            print("[Solana全般] 初期化失敗（RPC応答なし）→ 次回から取得")
    else:
        print(f"[Solana全般] 起動スキャン済み → 通常監視開始")
    while True:
        try:
            check_all_solana_onchain()
        except Exception as e:
            print(f"[Solana全般] ループエラー: {e}")
        time.sleep(5)


def parse_raydium_new_pool(signature):
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
    meta          = result.get("meta", {}) or {}
    post_balances = meta.get("postTokenBalances", []) or []
    pre_balances  = meta.get("preTokenBalances",  []) or []
    pre_mints     = {b.get("mint") for b in pre_balances}
    new_mint = None
    for b in post_balances:
        mint = b.get("mint", "")
        if not mint or mint in pre_mints:
            continue
        if mint in SOLANA_BASE_TOKENS:
            continue
        new_mint = mint
        break
    if not new_mint:
        return None
    liq_usd = 0.0
    for b in post_balances:
        mint   = b.get("mint", "")
        ui_amt = float((b.get("uiTokenAmount") or {}).get("uiAmount") or 0)
        if mint == WSOL_MINT and ui_amt > 0:
            sol_price = _get_sol_price_usd()
            liq_usd   = ui_amt * sol_price
            break
        if mint in (USDC_MINT, USDT_MINT) and ui_amt > 0:
            liq_usd = ui_amt
            break
    print(f"[Raydium] 新規プール: {new_mint[:20]} 流動性=${liq_usd:,.0f}")
    return new_mint, liq_usd


def _process_raydium_token(mint, liq_usd):
    try:
        if liq_usd >= LIQUIDITY_MIN:
            print(f"[Raydium] 流動性OK ${liq_usd:,.0f} → 保有率チェック: {mint[:20]}")
            holder_data = get_solana_holder_stats(mint)
            if holder_data is None:
                print(f"[Raydium] 保有データ取得失敗 → DexScreenerフォールバック: {mint[:20]}")
                _process_solana_token(mint, "Raydium", False)
                return
            top10 = holder_data["top10_ratio"]
            print(f"[Raydium] トップ10保有率: {top10:.1f}%")
            if top10 > TOP10_MAX_PCT:
                print(f"[Raydium] ❌ 保有集中高すぎ → スキップ")
                return
            dex = analyze_dexscreener(mint)
            dex_text = _build_dex_text(dex) if dex else f"💧 流動性: ${liq_usd:,.0f}\n\n"
            holder_text, holder_judge = format_holder_output(holder_data)
            msg = (
                f"⚡ <b>[Solana/Raydium] 新規トークン検知！</b>\n\n"
                f"時刻: {datetime.now().strftime('%H:%M:%S')}\n"
                f"📋 Mintアドレス（タップでコピー）\n"
                f"<code>{mint}</code>\n\n"
                f"{dex_text}"
                f"{holder_text}\n"
                f"{holder_judge}\n\n"
                f"📊 https://dexscreener.com/solana/{mint}\n"
                f"🔍 https://solscan.io/token/{mint}"
            )
            send_telegram(msg)
            with _notified_lock:
                _notified_tokens.add(mint)
            print(f"[Raydium] ✅ 即通知完了: {mint[:20]}")
        else:
            print(f"[Raydium] 流動性不足 ${liq_usd:,.0f} → DexScreener監視: {mint[:20]}")
            _process_solana_token(mint, "Raydium", False)
    except Exception as e:
        print(f"[Raydium] スレッドエラー ({mint[:20]}): {e}")


def _handle_raydium_tx(sig):
    with _SOLANA_SEMAPHORE:
        parsed = parse_raydium_new_pool(sig)
    if parsed is False or parsed is None:
        return
    mint, liq_usd = parsed
    now = time.time()
    with _pending_lock:
        pending_info = _pending_tokens.get(mint)
    if pending_info is not None:
        age = now - pending_info["created_at"]
        if age >= 20 * 60:
            print(f"[Raydium] 遅延ローンチ検知！ {mint[:20]} age={age/60:.0f}分 liq=${liq_usd:,.0f}")
            _notify_delayed_launch(mint, "sol", liq_usd, age, "Raydium")
        else:
            with _pending_lock:
                _pending_tokens.pop(mint, None)
            print(f"[Raydium] 早期取引開始({age/60:.0f}分) → 除外: {mint[:20]}")
        return
    with KNOWN_MINTS_LOCK:
        if mint in known_token_mints:
            return
        known_token_mints.add(mint)
    _register_pending_token(mint, "sol", "Raydium")
    print(f"[Raydium] 新規mint → 遅延監視に登録: {mint[:20]} (${liq_usd:,.0f})")


def check_raydium_onchain():
    global raydium_last_sigs
    now = time.time()
    for program in (RAYDIUM_AMM_V4, RAYDIUM_CPMM):
        label = "Raydium_AMM_V4" if program == RAYDIUM_AMM_V4 else "Raydium_CPMM"
        opts  = {"limit": 50, "commitment": "confirmed"}
        if raydium_last_sigs[program]:
            opts["until"] = raydium_last_sigs[program]
        txns = solana_rpc("getSignaturesForAddress", [program, opts])
        if not txns:
            continue
        raydium_last_sigs[program] = txns[0].get("signature", "")
        txns = [tx for tx in txns
                if not tx.get("blockTime") or (now - tx["blockTime"]) <= 300]
        if not txns:
            continue
        sigs = [tx.get("signature", "") for tx in txns
                if tx.get("signature") and not tx.get("err")]
        if not sigs:
            continue
        print(f"[{label}] {len(sigs)}件を並列処理開始")
        for sig in sigs:
            _RAYDIUM_POOL.submit(_handle_raydium_tx, sig)


def raydium_monitor_loop():
    global raydium_last_sigs
    print("[Raydium] 監視ループ開始中...")
    for program in (RAYDIUM_AMM_V4, RAYDIUM_CPMM):
        label = "AMM_V4" if program == RAYDIUM_AMM_V4 else "CPMM"
        if raydium_last_sigs[program] is not None:
            print(f"[Raydium/{label}] 起動スキャン済み → 通常監視開始")
            continue
        init = solana_rpc("getSignaturesForAddress", [program, {"limit": 5}])
        if init:
            raydium_last_sigs[program] = init[0].get("signature", "")
            print(f"[Raydium/{label}] 初期化完了 sig={raydium_last_sigs[program][:20]}")
        else:
            print(f"[Raydium/{label}] 初期化失敗（RPC応答なし）→ 次回から取得")
    while True:
        try:
            check_raydium_onchain()
        except Exception as e:
            print(f"[Raydium] ループエラー: {e}")
        time.sleep(5)


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


def get_bitget_contract_addresses(coin):
    try:
        url = f"https://api.bitget.com/api/v2/spot/public/coins?coin={coin}"
        r = requests.get(url, headers=HEADERS, timeout=8)
        if r.status_code != 200:
            return {}
        data = r.json()
        if data.get("code") != "00000":
            return {}
        coins = data.get("data", [])
        if not coins:
            return {}
        addresses = {}
        for chain in coins[0].get("chains", []):
            chain_name = chain.get("chain", "")
            contract   = chain.get("contractAddress", "")
            if chain_name and contract:
                addresses[chain_name] = contract
        return addresses
    except Exception as e:
        print(f"[CEX] コントラクトアドレス取得エラー: {e}")
        return {}


def _register_ca_watch(address: str, chain: str, source: str):
    key = address.lower() if chain == "evm" else address
    with _ca_watch_lock:
        if key in _ca_watch:
            return
        _ca_watch[key] = {
            "chain":         chain,
            "address_orig":  address,
            "registered_at": time.time(),
            "source":        source,
            "notified":      False,
        }
    print(f"[CA監視] 登録: {address[:20]}... chain={chain} from={source}")
    send_telegram(
        f"📡 <b>CA監視登録</b>\n\n"
        f"📋 アドレス: <code>{address}</code>\n"
        f"⛓ チェーン: {chain.upper()}\n"
        f"📰 情報源: {source}\n"
        f"⏰ LP追加を24時間監視します"
    )


def _extract_and_register_ca(text: str, source: str):
    for addr in _EVM_ADDR_RE.findall(text):
        low = addr.lower()
        if low in BSC_BASE_TOKENS or low in BASE_BASE_TOKENS:
            continue
        _register_ca_watch(addr, "evm", source)
    for addr in _SOL_ADDR_RE.findall(text):
        if addr in SOLANA_BASE_TOKENS or len(addr) < 32:
            continue
        _register_ca_watch(addr, "sol", source)


def _notify_delayed_launch(key: str, chain: str, liq_usd: float, age: float, source: str, dex: dict = None):
    # アイコンフィルター（Solanaのみ・EVMはTrue）
    if not _has_token_icon(key, chain, dex):
        with _pending_lock:
            _pending_tokens.pop(key, None)
        return
    with _notified_lock:
        if key in _notified_tokens:
            return
        _notified_tokens.add(key)
    with _pending_lock:
        _pending_tokens.pop(key, None)
    age_h   = int(age // 3600)
    age_m   = int((age % 3600) // 60)
    age_str = f"{age_h}時間{age_m}分" if age_h > 0 else f"{age_m}分"
    dex_url = (
        f"https://dexscreener.com/solana/{key}"
        if chain == "sol"
        else f"https://dexscreener.com/{key}"
    )
    pc5 = (dex or {}).get("price_change_5m", 0) or 0
    b5  = (dex or {}).get("buys_5m", 0) or 0
    s5  = (dex or {}).get("sells_5m", 0) or 0
    dex_line = f"📈 5分変動: {pc5:+.1f}%\n🛒 買い{b5}件 / 売り{s5}件 (5分)\n\n" if dex else "\n"
    send_telegram(
        f"⏰ <b>[遅延ローンチ] 取引開始！</b>\n\n"
        f"📋 アドレス: <code>{key}</code>\n"
        f"⛓ チェーン: {chain.upper()}\n"
        f"⏱ 作成から <b>{age_str}後</b> に取引開始\n\n"
        f"💧 流動性: <b>${liq_usd:,.0f}</b>\n"
        f"{dex_line}"
        f"🔗 {dex_url}"
    )
    print(f"[遅延ローンチ] ✅ 通知: {key[:20]} age={age_str} liq=${liq_usd:,.0f}")


def _register_pending_token(key: str, chain: str, source: str, **kwargs):
    with _pending_lock:
        if key in _pending_tokens:
            return
        _pending_tokens[key] = {
            "chain":      chain,
            "created_at": time.time(),
            "source":     source,
            **kwargs,
        }
    print(f"[遅延監視] 登録: {key[:20]}... chain={chain} → 20分後から監視開始")


def evm_pending_onchain_loop():
    last_blocks = {}
    while True:
        try:
            now = time.time()
            with _pending_lock:
                evm_items = {
                    k: v for k, v in _pending_tokens.items()
                    if v.get("chain") == "evm" and v.get("pair_addr") and v.get("evm_chain")
                }
            by_chain = {}
            for key, info in evm_items.items():
                chain_name = info["source"]
                if chain_name not in by_chain:
                    by_chain[chain_name] = []
                by_chain[chain_name].append((key, info))
            for chain_name, items in by_chain.items():
                chain = items[0][1].get("evm_chain")
                if not chain:
                    continue
                latest_hex = evm_rpc(chain, "eth_blockNumber", [])
                if not latest_hex:
                    continue
                latest_int = int(latest_hex, 16)
                from_block = last_blocks.get(chain_name, latest_int - 3)
                last_blocks[chain_name] = latest_int
                if from_block > latest_int:
                    continue
                pair_addrs = [info.get("pair_addr") for _, info in items if info.get("pair_addr")]
                if not pair_addrs:
                    continue
                logs = evm_rpc(chain, "eth_getLogs", [{
                    "fromBlock": hex(from_block),
                    "toBlock":   hex(latest_int),
                    "address":   pair_addrs,
                    "topics":    [V2_MINT_TOPIC],
                }])
                if not logs:
                    continue
                minted_pairs = {log["address"].lower() for log in logs}
                for key, info in items:
                    pair_addr = (info.get("pair_addr") or "").lower()
                    if pair_addr not in minted_pairs:
                        continue
                    age = now - info["created_at"]
                    if age < 20 * 60:
                        with _pending_lock:
                            _pending_tokens.pop(key, None)
                        print(f"[EVM遅延監視] 早期取引({age/60:.0f}分) → 除外: {key[:16]}")
                        continue
                    token0 = info.get("token0")
                    token1 = info.get("token1")
                    is_v2  = info.get("is_v2", True)
                    if is_v2:
                        liq = _get_v2_pair_liquidity_usd(pair_addr, token0, token1, chain)
                    else:
                        liq = _get_v3_pool_liquidity_usd(pair_addr, token0, token1, chain)
                    if not liq or liq < LIQUIDITY_MIN:
                        continue
                    print(f"[EVM遅延監視] 遅延ローンチ検知！ {key[:16]} age={age/60:.0f}分 liq=${liq:,.0f}")
                    _notify_delayed_launch(key, "evm", liq, age, chain_name)
        except Exception as e:
            print(f"[EVM遅延監視] エラー: {e}")
        time.sleep(3)


def pending_watch_loop():
    WAIT_SEC         = 20 * 60
    MAX_SEC          = 8 * 3600
    BATCH_SIZE       = 30
    MAX_WATCH        = 2000
    EARLY_CHECK_INTV = 5 * 60
    last_early_check = 0.0
    while True:
        try:
            now = time.time()
            with _pending_lock:
                to_delete = []
                for key, info in list(_pending_tokens.items()):
                    age = now - info["created_at"]
                    if age > MAX_SEC:
                        to_delete.append(key)
                        continue
                    with _notified_lock:
                        if key in _notified_tokens:
                            to_delete.append(key)
                for k in to_delete:
                    _pending_tokens.pop(k, None)
                if len(_pending_tokens) > MAX_WATCH:
                    sorted_keys = sorted(
                        _pending_tokens,
                        key=lambda k: _pending_tokens[k]["created_at"]
                    )
                    for k in sorted_keys[:len(_pending_tokens) - MAX_WATCH]:
                        _pending_tokens.pop(k, None)
                all_tokens = dict(_pending_tokens)
            if now - last_early_check >= EARLY_CHECK_INTV:
                last_early_check = now
                early_targets = {
                    k: v for k, v in all_tokens.items()
                    if now - v["created_at"] < WAIT_SEC
                }
                early_keys = list(early_targets.keys())
                for i in range(0, len(early_keys), BATCH_SIZE):
                    batch = early_keys[i:i + BATCH_SIZE]
                    try:
                        r = requests.get(
                            f"https://api.dexscreener.com/latest/dex/tokens/{','.join(batch)}",
                            headers=HEADERS, timeout=10
                        )
                        if r.status_code != 200:
                            continue
                        pairs = r.json().get("pairs") or []
                    except Exception:
                        continue
                    has_liq = set()
                    for pair in pairs:
                        addr = (pair.get("baseToken") or {}).get("address", "")
                        liq  = (pair.get("liquidity") or {}).get("usd", 0) or 0
                        if addr and liq > 0:
                            has_liq.add(addr.lower())
                    for key in batch:
                        if key.lower() in has_liq:
                            with _pending_lock:
                                _pending_tokens.pop(key, None)
                            print(f"[遅延監視] 早期取引開始 → 除外: {key[:20]}...")
                    time.sleep(0.3)
            targets = {
                k: v for k, v in all_tokens.items()
                if now - v["created_at"] >= WAIT_SEC
            }
            if not targets:
                time.sleep(10)
                continue
            keys = list(targets.keys())
            for i in range(0, len(keys), BATCH_SIZE):
                batch = keys[i:i + BATCH_SIZE]
                try:
                    r = requests.get(
                        f"https://api.dexscreener.com/latest/dex/tokens/{','.join(batch)}",
                        headers=HEADERS, timeout=10
                    )
                    if r.status_code != 200:
                        continue
                    pairs = r.json().get("pairs") or []
                except Exception:
                    continue
                liq_map = {}
                dex_map = {}
                for pair in pairs:
                    base_addr = (pair.get("baseToken") or {}).get("address", "")
                    liq = (pair.get("liquidity") or {}).get("usd", 0) or 0
                    if not base_addr:
                        continue
                    key_lower = base_addr.lower()
                    if liq > liq_map.get(key_lower, 0):
                        liq_map[key_lower] = liq
                        dex_map[key_lower] = {
                            "liquidity":       liq,
                            "price_change_5m": (pair.get("priceChange") or {}).get("m5", 0) or 0,
                            "buys_5m":         (pair.get("txns") or {}).get("m5", {}).get("buys", 0) or 0,
                            "sells_5m":        (pair.get("txns") or {}).get("m5", {}).get("sells", 0) or 0,
                            "pair_created_at": (pair.get("pairCreatedAt") or 0) / 1000,
                            "image_url":       (pair.get("info") or {}).get("imageUrl", ""),
                        }
                for key in batch:
                    liq = liq_map.get(key.lower(), 0)
                    if liq < LIQUIDITY_MIN:
                        continue
                    info = targets.get(key)
                    if not info:
                        continue
                    dex_data     = dex_map.get(key.lower(), {})
                    pair_created = dex_data.get("pair_created_at", 0)
                    if pair_created > 0:
                        launch_delay = pair_created - info["created_at"]
                        if launch_delay < WAIT_SEC:
                            print(f"[遅延監視] 早期取引開始({launch_delay/60:.0f}分)→除外: {key[:20]}...")
                            with _pending_lock:
                                _pending_tokens.pop(key, None)
                            continue
                    age  = now - info["created_at"]
                    dex  = dex_map.get(key.lower(), {})
                    _notify_delayed_launch(key, info["chain"], liq, age, info["source"], dex)
                time.sleep(0.3)
        except Exception as e:
            print(f"[遅延監視] エラー: {e}")
        time.sleep(10)


def ca_watch_loop():
    while True:
        try:
            now = time.time()
            with _ca_watch_lock:
                items = list(_ca_watch.items())
            for key, info in items:
                if info["notified"]:
                    continue
                elapsed = now - info["registered_at"]
                if elapsed > 86400:
                    with _ca_watch_lock:
                        _ca_watch.pop(key, None)
                    print(f"[CA監視] タイムアウト削除: {key[:20]}...")
                    continue
                addr  = info["address_orig"]
                chain = info["chain"]
                dex   = analyze_dexscreener(addr)
                if not dex:
                    continue
                liq = dex.get("liquidity", 0) or 0
                if liq < LIQUIDITY_MIN:
                    continue
                info["notified"] = True
                elapsed_h = int(elapsed // 3600)
                elapsed_m = int((elapsed % 3600) // 60)
                elapsed_str = f"{elapsed_h}時間{elapsed_m}分" if elapsed_h > 0 else f"{elapsed_m}分"
                dex_url = (
                    f"https://dexscreener.com/solana/{addr}" if chain == "sol"
                    else f"https://dexscreener.com/{addr}"
                )
                send_telegram(
                    f"🎯 <b>CA監視トークン 取引開始！</b>\n\n"
                    f"📋 CA: <code>{addr}</code>\n"
                    f"⛓ チェーン: {chain.upper()}\n"
                    f"💧 流動性: <b>${liq:,.0f}</b>\n"
                    f"📈 5分変動: {dex.get('price_change_5m', 0):+.1f}%\n"
                    f"📰 情報源: {info['source']}\n"
                    f"⏰ 登録から: {elapsed_str}後に取引開始\n\n"
                    f"🔗 {dex_url}"
                )
                print(f"[CA監視] 取引開始通知: {addr[:20]}... liq=${liq:,.0f}")
        except Exception as e:
            print(f"[CA監視] エラー: {e}")
        time.sleep(5)


def telegram_sns_monitor_loop():
    global _tg_update_offset
    print("[SNS監視] Telegram監視スレッド起動")
    if TELEGRAM_MONITOR_CHATS:
        print(f"[SNS監視] 監視グループ: {TELEGRAM_MONITOR_CHATS}")
    else:
        print("[SNS監視] 監視グループ未設定 → /watch コマンドのみ有効")
    while True:
        try:
            params = {"timeout": 30, "limit": 100}
            with _tg_update_lock:
                if _tg_update_offset is not None:
                    params["offset"] = _tg_update_offset
            r = requests.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                params=params, timeout=35
            )
            if r.status_code != 200:
                time.sleep(5)
                continue
            updates = r.json().get("result", [])
            for upd in updates:
                with _tg_update_lock:
                    _tg_update_offset = upd["update_id"] + 1
                msg  = upd.get("message") or upd.get("channel_post") or {}
                text = (msg.get("text") or msg.get("caption") or "").strip()
                if not text:
                    continue
                chat_id   = msg.get("chat", {}).get("id")
                chat_type = msg.get("chat", {}).get("type", "")
                if text.startswith("/watch ") and chat_type == "private":
                    raw = text.split(" ", 1)[1].strip()
                    if _EVM_ADDR_RE.match(raw):
                        _register_ca_watch(raw, "evm", "手動登録")
                    elif re.match(r'^[1-9A-HJ-NP-Za-km-z]{32,44}$', raw):
                        _register_ca_watch(raw, "sol", "手動登録")
                    else:
                        send_telegram(
                            "❌ アドレスが無効です\n\n"
                            "使い方:\n"
                            "EVM: /watch 0x1234...abcd\n"
                            "Solana: /watch AbCd...XyZ1"
                        )
                    continue
                if TELEGRAM_MONITOR_CHATS and chat_id in TELEGRAM_MONITOR_CHATS:
                    chat_name = msg.get("chat", {}).get("title", str(chat_id))
                    _extract_and_register_ca(text, source=f"Telegram:{chat_name}")
        except requests.exceptions.Timeout:
            pass
        except Exception as e:
            print(f"[SNS監視] エラー: {e}")
            time.sleep(5)


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
            addresses = get_bitget_contract_addresses(base)
            addr_text = ""
            if addresses:
                lines = []
                for chain_name, addr in list(addresses.items())[:5]:
                    lines.append(f"  <b>{chain_name}:</b> <code>{addr}</code>")
                addr_text = "📋 コントラクトアドレス:\n" + "\n".join(lines) + "\n\n"
            msg = (
                f"🏦 <b>[CEX] Bitget新規上場！</b>\n\n"
                f"トークン: <b>${base}</b>\n"
                f"時刻: {datetime.now().strftime('%H:%M:%S')}\n\n"
                f"{addr_text}"
                f"✅ Bitget審査済み（比較的安全）\n"
                f"🔗 https://www.bitget.com/spot/{base}USDT_SPBL"
            )
            send_telegram(msg)
            print(f"[CEX新規] {symbol} アドレス={len(addresses)}チェーン")
    known_cex_symbols = current


def _startup_scan_one_evm(chain, is_all, now, min_age, max_age):
    try:
        avg_block_sec = 3 if ("BSC" in chain["name"] or "BNB" in chain["name"]
                              or "FourMeme" in chain["name"]) else 2
        blocks_8h  = int(max_age / avg_block_sec)
        blocks_20m = int(min_age / avg_block_sec)
        latest_hex = evm_rpc(chain, "eth_blockNumber", [])
        if not latest_hex:
            chain["last_block"] = None
            return 0
        latest_int = int(latest_hex, 16)
        from_block = max(0, latest_int - blocks_8h)
        to_block   = latest_int - blocks_20m
        if to_block <= from_block:
            chain["last_block"] = latest_int
            return 0
        count = 0
        CHUNK = 500
        for start in range(from_block, to_block + 1, CHUNK):
            end = min(start + CHUNK - 1, to_block)
            if is_all:
                event_topic = chain.get("topic", POOL_CREATED_TOPIC)
                logs = evm_rpc(chain, "eth_getLogs", [{
                    "fromBlock": hex(start), "toBlock": hex(end),
                    "address":  chain["factory"], "topics": [event_topic],
                }])
            else:
                logs = evm_rpc(chain, "eth_getLogs", [{
                    "fromBlock": hex(start), "toBlock": hex(end),
                    "address":  chain["contract"],
                    "topics":   [TRANSFER_TOPIC, ZERO_TOPIC],
                }])
            if not logs:
                continue
            for log in logs:
                block_num  = int(log.get("blockNumber", "0x0"), 16)
                created_at = now - (latest_int - block_num) * avg_block_sec
                if is_all:
                    topics = log.get("topics", [])
                    if len(topics) < 3:
                        continue
                    is_v2       = (chain.get("topic") == PAIR_CREATED_TOPIC)
                    base_tokens = chain["base_tokens"]
                    t0 = ("0x" + topics[1][-40:]).lower()
                    t1 = ("0x" + topics[2][-40:]).lower()
                    if t0 not in base_tokens and t1 in base_tokens:
                        new_token = t0
                    elif t0 in base_tokens and t1 not in base_tokens:
                        new_token = t1
                    else:
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
                    if new_token in chain["known_tokens"]:
                        continue
                    chain["known_tokens"].add(new_token)
                    with _pending_lock:
                        if new_token not in _pending_tokens:
                            _pending_tokens[new_token] = {
                                "chain": "evm", "created_at": created_at,
                                "source": chain["name"], "pair_addr": pair_addr,
                                "token0": t0, "token1": t1, "is_v2": is_v2,
                                "evm_chain": chain,
                            }
                    count += 1
                else:
                    topics = log.get("topics", [])
                    if len(topics) < 3:
                        continue
                    token_addr = ("0x" + topics[2][-40:]).lower()
                    if token_addr in chain["known_tokens"]:
                        continue
                    chain["known_tokens"].add(token_addr)
                    with _pending_lock:
                        if token_addr not in _pending_tokens:
                            _pending_tokens[token_addr] = {
                                "chain": "evm", "created_at": created_at,
                                "source": chain["name"],
                            }
                    count += 1
        chain["last_block"] = latest_int
        if count > 0:
            print(f"[起動スキャン/{chain['name']}] {count}件を遅延監視に登録")
        return count
    except Exception as e:
        print(f"[起動スキャン/{chain['name']}] エラー: {e}")
        return 0


def _startup_scan_solana(now, min_age, max_age):
    global raydium_last_sigs
    for program, label in [
        (RAYDIUM_AMM_V4, "Raydium_AMM_V4"),
        (RAYDIUM_CPMM,   "Raydium_CPMM"),
    ]:
        first_sig  = None
        before     = None
        to_process = []
        while True:
            opts = {"limit": 50, "commitment": "confirmed"}
            if before:
                opts["before"] = before
            txns = solana_rpc("getSignaturesForAddress", [program, opts])
            if not txns:
                break
            if first_sig is None:
                first_sig = txns[0].get("signature")
            stop = False
            for tx in txns:
                bt = tx.get("blockTime")
                if not bt:
                    continue
                age = now - bt
                if age > max_age:
                    stop = True
                    break
                if age >= min_age and not tx.get("err") and tx.get("signature"):
                    to_process.append((tx["signature"], float(bt)))
            if stop or len(txns) < 50:
                break
            before = txns[-1].get("signature")
            time.sleep(0.3)
        if first_sig:
            raydium_last_sigs[program] = first_sig
        if not to_process:
            continue
        print(f"[起動スキャン/{label}] {len(to_process)}件を処理開始（バックグラウンド）")
        count = 0
        for sig, block_time in to_process:
            time.sleep(0.5)
            with _SOLANA_SEMAPHORE:
                parsed = parse_raydium_new_pool(sig)
            if not parsed or parsed is False:
                continue
            mint = parsed[0]
            with KNOWN_MINTS_LOCK:
                if mint in known_token_mints:
                    continue
                known_token_mints.add(mint)
            with _pending_lock:
                if mint not in _pending_tokens:
                    _pending_tokens[mint] = {
                        "chain":      "sol",
                        "created_at": block_time,
                        "source":     label,
                    }
            count += 1
        if count > 0:
            print(f"[起動スキャン/{label}] {count}件を遅延監視に登録完了")


def _startup_scan():
    now     = time.time()
    MIN_AGE = 20 * 60
    MAX_AGE = 8 * 3600
    print(f"[起動スキャン] 開始: 過去20分〜8時間のトークンを遅延監視に登録中...")
    total = [0]
    lock  = threading.Lock()
    def _run(chain, is_all):
        n = _startup_scan_one_evm(chain, is_all, now, MIN_AGE, MAX_AGE)
        with lock:
            total[0] += n
    threads = []
    for chain in EVM_CHAINS:
        t = threading.Thread(target=_run, args=(chain, False), daemon=True)
        t.start(); threads.append(t)
    for chain in EVM_ALL_CHAINS:
        t = threading.Thread(target=_run, args=(chain, True), daemon=True)
        t.start(); threads.append(t)
    for t in threads:
        t.join()
    print(f"[起動スキャン] EVM完了: {total[0]}件を遅延監視に登録")
    threading.Thread(
        target=_startup_scan_solana, args=(now, MIN_AGE, MAX_AGE), daemon=True
    ).start()
    print("[起動スキャン] Solana(Raydium)はバックグラウンドでスキャン中...")


def _startup_notify_scan():
    BATCH_SIZE = 30
    WAIT_SEC   = 20 * 60
    time.sleep(10)
    with _pending_lock:
        targets = {
            k: v for k, v in _pending_tokens.items()
            if time.time() - v["created_at"] >= WAIT_SEC
        }
    total_checked = len(targets)
    if not targets:
        print("[起動通知スキャン] 対象トークンなし")
        send_telegram(
            "🔍 <b>起動通知スキャン完了</b>\n\n"
            "対象トークン: 0件\n"
            "（20分〜8時間前のトークンが見つかりませんでした）"
        )
        return
    print(f"[起動通知スキャン] {total_checked}件をDexScreenerで一括チェック開始")
    send_telegram(
        f"🔍 <b>起動通知スキャン開始</b>\n\n"
        f"📋 チェック対象: {total_checked}件\n"
        f"💧 既に取引中のトークンを検索中...\n"
        f"（Solanaのみアイコンフィルター適用）"
    )
    keys        = list(targets.keys())
    notified    = 0
    skipped_liq = 0
    skipped_age = 0
    for i in range(0, len(keys), BATCH_SIZE):
        batch = keys[i:i + BATCH_SIZE]
        try:
            r = requests.get(
                f"https://api.dexscreener.com/latest/dex/tokens/{','.join(batch)}",
                headers=HEADERS, timeout=12,
            )
            if r.status_code != 200:
                time.sleep(1)
                continue
            pairs = r.json().get("pairs") or []
        except Exception as e:
            print(f"[起動通知スキャン] DexScreenerエラー: {e}")
            continue
        liq_map = {}
        dex_map = {}
        for pair in pairs:
            base_addr = (pair.get("baseToken") or {}).get("address", "")
            liq       = (pair.get("liquidity") or {}).get("usd", 0) or 0
            if not base_addr:
                continue
            key_lower = base_addr.lower()
            if liq > liq_map.get(key_lower, 0):
                liq_map[key_lower] = liq
                dex_map[key_lower] = {
                    "liquidity":       liq,
                    "price_change_5m": (pair.get("priceChange") or {}).get("m5", 0) or 0,
                    "buys_5m":         (pair.get("txns") or {}).get("m5", {}).get("buys", 0) or 0,
                    "sells_5m":        (pair.get("txns") or {}).get("m5", {}).get("sells", 0) or 0,
                    "pair_created_at": (pair.get("pairCreatedAt") or 0) / 1000,
                    "image_url":       (pair.get("info") or {}).get("imageUrl", ""),
                }
        now = time.time()
        for key in batch:
            liq = liq_map.get(key.lower(), 0)
            if liq < LIQUIDITY_MIN:
                skipped_liq += 1
                continue
            info = targets.get(key)
            if not info:
                continue
            dex_data     = dex_map.get(key.lower(), {})
            pair_created = dex_data.get("pair_created_at", 0)
            if pair_created > 0:
                launch_delay = pair_created - info["created_at"]
                if launch_delay < WAIT_SEC:
                    skipped_age += 1
                    with _pending_lock:
                        _pending_tokens.pop(key, None)
                    print(f"[起動通知スキャン] 早期取引({launch_delay/60:.0f}分) → 除外: {key[:20]}")
                    continue
            age = now - info["created_at"]
            _notify_delayed_launch(key, info["chain"],
                             liq, age, info["source"], dex_data)
            notified += 1
        time.sleep(0.3)
    print(f"[起動通知スキャン] 完了: チェック{total_checked}件 → 通知{notified}件")
    send_telegram(
        f"✅ <b>起動通知スキャン完了</b>\n\n"
        f"📋 チェック: {total_checked}件\n"
        f"💧 流動性不足でスキップ: {skipped_liq}件\n"
        f"⏱ 早期取引（20分以内）でスキップ: {skipped_age}件\n"
        f"🖼 アイコンなしでスキップ(Solanaのみ): {total_checked - skipped_liq - skipped_age - notified}件\n"
        f"✅ 通知送信: <b>{notified}件</b>"
    )


def main():
    print("通知ボットくん 起動中...")
    send_telegram(
        "✅ <b>通知ボットくん 起動しました！</b>\n\n"
        "📊 監視対象：\n"
        "🏦 CEX: Bitget取引所（新規上場）\n"
        "🚀 Pump.fun / Solana全般（全launchpad）\n"
        "⚡ Raydium直接監視（AMM V4 / CPMM）\n"
        "🟡 DEX: FourMeme / BSC\n"
        "🟡 DEX: BNB Chain全般（PancakeSwap V2/V3）\n"
        "🔵 DEX: Clanker / Base\n"
        "🔵 DEX: Base全般（Uniswap V3）\n\n"
        f"⚡ 新戦略: プール作成時オンチェーン即時チェック\n"
        f"💧 初期流動性 ${LIQUIDITY_MIN:,}+ AND\n"
        f"👛 トップ10保有率 ≤{TOP10_MAX_PCT:.0f}%\n"
        f"🔄 EVM検知5秒毎 / 通知まで平均10〜15秒\n"
        f"⚡ Raydium: プール作成TX直接検知 5〜15秒\n\n"
        "🔍 Solana監視対象：\n"
        "pump.fun / Raydium / rapidlaunch.io\n"
        "moonshot / letsbonk / その他全Solana launchpad"
    )

    send_telegram(
        "🔍 <b>起動スキャン中...</b>\n"
        "過去20分〜8時間に作成されたトークンを遅延監視に登録しています\n"
        "（EVM: 30〜60秒 / Solana: バックグラウンドで処理）"
    )
    _startup_scan()
    with _pending_lock:
        pending_count = len(_pending_tokens)
    send_telegram(
        f"✅ <b>起動スキャン完了</b>\n\n"
        f"📋 EVM登録済み: {pending_count}件\n"
        f"🔄 Solana(Raydium): バックグラウンドで追加登録中\n\n"
        f"⏰ 20分〜8時間前に作成されたトークンが取引開始したら即通知します"
    )

    threading.Thread(target=_startup_notify_scan, daemon=True).start()
    print("[起動通知スキャン] バックグラウンドスレッド起動完了")

    t_pumpfun = threading.Thread(target=pumpfun_monitor_loop, daemon=True)
    t_pumpfun.start()
    print("[Pump.fun] バックグラウンドスレッド起動完了")

    t_all_solana = threading.Thread(target=solana_all_monitor_loop, daemon=True)
    t_all_solana.start()
    print("[Solana全般] バックグラウンドスレッド起動完了")

    t_raydium = threading.Thread(target=raydium_monitor_loop, daemon=True)
    t_raydium.start()
    print("[Raydium] バックグラウンドスレッド起動完了")

    t_tg_sns = threading.Thread(target=telegram_sns_monitor_loop, daemon=True)
    t_tg_sns.start()
    print("[SNS監視] Telegramモニタースレッド起動完了")

    t_ca_watch = threading.Thread(target=ca_watch_loop, daemon=True)
    t_ca_watch.start()
    print("[CA監視] CA監視ループスレッド起動完了")

    t_pending = threading.Thread(target=pending_watch_loop, daemon=True)
    t_pending.start()
    print("[遅延監視] 遅延ローンチ監視スレッド起動完了")

    t_evm_onchain = threading.Thread(target=evm_pending_onchain_loop, daemon=True)
    t_evm_onchain.start()
    print("[EVM遅延監視] EVMオンチェーン監視スレッド起動完了")

    loop = 0
    while True:
        if loop % 6 == 0:
            check_cex_listings()
        for chain in EVM_CHAINS:
            check_evm_chain(chain)
        for chain in EVM_ALL_CHAINS:
            check_evm_all_chain(chain)
        time.sleep(2)
        loop += 1
        if loop % 360 == 0:
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
