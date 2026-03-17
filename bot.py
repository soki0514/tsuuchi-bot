aimport requests
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

# ── Solana全般監視: SPL Token Metadata Program (全launchpad対応) ───────────────
# pump.fun / rapidlaunch.io / moonshot など、Solanaの全launchpadはこのプログラムに
# トークンのメタデータ(名前・シンボル)を登録するため、ここを見れば全て拾える
SPL_METADATA_PROGRAM = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"

# ── EVM全般監視定数 ────────────────────────────────────────────────────────────
# Uniswap V3 / PancakeSwap V3 共通の PoolCreated イベントトピック
POOL_CREATED_TOPIC      = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"
PANCAKE_V3_FACTORY_BSC  = "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"  # PancakeSwap V3
UNISWAP_V3_FACTORY_BASE = "0x33128a8fC17869897dcE68Ed026d694621f6FDfD"  # Uniswap V3 on Base
# PancakeSwap V2 の PairCreated イベントトピック（BSCミームトークンの主流）
PAIR_CREATED_TOPIC      = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
PANCAKE_V2_FACTORY_BSC  = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"  # PancakeSwap V2

# BSC の「ベーストークン」= 新規トークンとして扱わないアドレス（小文字で統一）
BSC_BASE_TOKENS = {
    "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",  # WBNB
    "0x55d398326f99059ff775485246999027b3197955",  # USDT (BSC)
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",  # USDC (BSC)
    "0xe9e7cea3dedca5984780bafc599bd69add087d56",  # BUSD
    "0x2170ed0880ac9a755fd29b2688956bd959f933f8",  # ETH (BSC)
    "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3",  # DAI (BSC)
}
# Base の「ベーストークン」
BASE_BASE_TOKENS = {
    "0x4200000000000000000000000000000000000006",  # WETH
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",  # USDC (Base)
    "0x50c5725949a6f0c72e6c4a641f24049a917db0cb",  # DAI (Base)
    "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca",  # USDbC
    "0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22",  # cbETH
}

# ── オンチェーン流動性チェック用定数 ─────────────────────────────────────────
# Chainlink価格フィード（BNB/USD・ETH/USD）
CHAINLINK_BNB_USD  = "0x0567F2323251f0Aab15c8dFb1967E4e8A7D42aeE"  # BSC
CHAINLINK_ETH_USD  = "0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70"  # Base

# ステーブルコインアドレス → そのままUSD換算可能（小文字）
STABLE_ADDRS = {
    "0x55d398326f99059ff775485246999027b3197955",  # USDT BSC  (18 dec)
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",  # USDC BSC  (18 dec)
    "0xe9e7cea3dedca5984780bafc599bd69add087d56",  # BUSD BSC  (18 dec)
    "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3",  # DAI  BSC  (18 dec)
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",  # USDC Base  (6 dec)
    "0x50c5725949a6f0c72e6c4a641f24049a917db0cb",  # DAI  Base (18 dec)
    "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca",  # USDbC Base  (6 dec)
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
# ネイティブトークン（Chainlinkで価格取得が必要・18 dec）
NATIVE_ADDRS = {
    "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",  # WBNB
    "0x4200000000000000000000000000000000000006",  # WETH (Base)
    "0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22",  # cbETH (Base)
    "0x2170ed0880ac9a755fd29b2688956bd959f933f8",  # ETH on BSC
}

# ネイティブ価格キャッシュ: chain_name → (price_usd, timestamp)
_native_price_cache: dict = {}
PRICE_CACHE_SEC = 300  # 5分

# launchpad固有監視と全般監視でknown_tokensを共有し二重通知を防ぐ
_BSC_KNOWN  = set()
_BASE_KNOWN = set()

# ── 監視チェーン ──────────────────────────────────────────────────────────────
EVM_CHAINS = [
    {
        "name": "FourMeme/BSC", "emoji": "🟡",
        # eth_getLogs対応のRPCのみ使用（dataseedはgetLogs非対応）
        "rpc_list": [
            "https://bsc-mainnet.public.blastapi.io",
            "https://1rpc.io/bnb",
            "https://bsc-rpc.publicnode.com",
        ],
        "contract": "0x5c952063c7fc8610ffdb798152d69f0b9550762b",
        "dex_url": "https://dexscreener.com/bsc/{}",
        "launch_url": "https://four.meme",
        "known_tokens": _BSC_KNOWN, "last_block": None,  # BNB Chain全般と共有
    },
    {
        "name": "Clanker/Base", "emoji": "🔵",
        # BlastAPI → 1RPC → llamarpc の順でフォールバック
        "rpc_list": [
            "https://base-mainnet.public.blastapi.io",
            "https://1rpc.io/base",
            "https://base.llamarpc.com",
        ],
        "contract": "0xe85a59c628f7d27878aceb4bf3b35733630083a9",
        "dex_url": "https://dexscreener.com/base/{}",
        "launch_url": "https://www.clanker.world",
        "known_tokens": _BASE_KNOWN, "last_block": None,  # Base全般と共有
    },
]

# ── EVM全般監視チェーン（PoolCreated経由で全launchpad対応）─────────────────────
EVM_ALL_CHAINS = [
    {
        "name": "BNB Chain全般(V2)", "emoji": "🟡",
        # PancakeSwap V2 Factory監視（BSCミームトークンの主流はV2）
        "rpc_list": [
            "https://bsc-mainnet.public.blastapi.io",
            "https://1rpc.io/bnb",
            "https://bsc-rpc.publicnode.com",
        ],
        "factory":      PANCAKE_V2_FACTORY_BSC,
        "topic":        PAIR_CREATED_TOPIC,   # V2: PairCreated
        "base_tokens":  BSC_BASE_TOKENS,
        "dex_url":      "https://dexscreener.com/bsc/{}",
        "known_tokens": _BSC_KNOWN,  # FourMeme/V3と共有（二重通知防止）
        "last_block":   None,
    },
    {
        "name": "BNB Chain全般(V3)", "emoji": "🟡",
        # PancakeSwap V3 Factory監視（V3プールを使う一部トークン対応）
        "rpc_list": [
            "https://bsc-mainnet.public.blastapi.io",
            "https://1rpc.io/bnb",
            "https://bsc-rpc.publicnode.com",
        ],
        "factory":      PANCAKE_V3_FACTORY_BSC,
        "topic":        POOL_CREATED_TOPIC,   # V3: PoolCreated
        "base_tokens":  BSC_BASE_TOKENS,
        "dex_url":      "https://dexscreener.com/bsc/{}",
        "known_tokens": _BSC_KNOWN,  # FourMeme/V2と共有（二重通知防止）
        "last_block":   None,
    },
    {
        "name": "Base全般", "emoji": "🔵",
        # Uniswap V3 Factory監視（Clanker以外のBase全launchpad対応）
        "rpc_list": [
            "https://base-mainnet.public.blastapi.io",
            "https://1rpc.io/base",
            "https://base.llamarpc.com",
        ],
        "factory":      UNISWAP_V3_FACTORY_BASE,
        "topic":        POOL_CREATED_TOPIC,   # V3: PoolCreated
        "base_tokens":  BASE_BASE_TOKENS,
        "dex_url":      "https://dexscreener.com/base/{}",
        "known_tokens": _BASE_KNOWN,  # Clankerと共有（二重通知防止）
        "last_block":   None,
    },
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

# ── グローバル状態 ────────────────────────────────────────────────────────────
known_cex_symbols = set()
known_token_mints = set()   # pump.fun + Solana全般共通（重複通知防止）
last_signature    = None    # pump.fun 用
all_solana_last_signature = None  # Solana全般（Metadata Program）用

# ── 閾値ベース通知フィルター ──────────────────────────────────────────────────
LIQUIDITY_MIN       = 25_000   # 流動性閾値（USD）: これ以上で通知候補
TOP10_MAX_PCT       = 60.0     # トップ10保有率上限（%）: これ超えでスキップ
POLL_INTERVAL_SEC   = 10       # DexScreenerポーリング間隔（秒）
MONITOR_TIMEOUT_SEC = 600      # 監視タイムアウト（10分）: 未達成はスキップ

# ── スレッドセーフ: known_token_mintsの競合書き込み防止 ─────────────────────
# pump.fun（バックグラウンド）とSolana全般（バックグラウンド）が同時に
# 同一mintを検知して二重通知するのを防ぐ
KNOWN_MINTS_LOCK = threading.Lock()

# ── 検知漏れ防止: getTransaction失敗シグネチャのリトライキュー ──────────────
# parse_new_tokenでgetTransactionが全試行失敗した場合にここへ保存し、
# 次のメインループで再試行する（5分以内に成功しなければ破棄）
RETRY_SIG_QUEUE = []               # [(signature, enqueued_time), ...]
RETRY_SIG_LOCK  = threading.Lock()
RETRY_EXPIRY    = 300              # 秒: 5分

# ── Solana RPCグローバルレート制限（8 RPS）────────────────────────────────────
# 全スレッド合計で1秒間に8回までしかSolana RPCを呼べないよう制限する。
# Helius無料枠(10 RPS)に対して2つのマージンを確保し429エラーを防ぐ。
_SOLANA_RPS_LIMIT  = 8
_solana_rpc_times  = []            # 直近1秒間のRPC呼び出しタイムスタンプ
_solana_rpc_lock   = threading.Lock()


def _wait_for_rpc_slot():
    """
    スライディングウィンドウ方式で8 RPSを超えないよう待機する。
    全スレッドで共有されるグローバルレート制限。
    """
    global _solana_rpc_times
    while True:
        with _solana_rpc_lock:
            now = time.time()
            # 1秒以上前のタイムスタンプを削除
            _solana_rpc_times = [t for t in _solana_rpc_times if now - t < 1.0]
            if len(_solana_rpc_times) < _SOLANA_RPS_LIMIT:
                _solana_rpc_times.append(now)
                return
        time.sleep(0.05)  # 50ms待ってリトライ


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
            "dex_id":          pair.get("dexId", ""),   # launchpad特定用
        }
    except Exception as e:
        print(f"DexScreenerエラー: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# EVM RPC
# ══════════════════════════════════════════════════════════════════════════════

def evm_rpc(chain, method, params):
    """
    rpc_list内のRPCを順番に試す。全て失敗した場合はNoneを返す。
    503/no responseなどでフェイルしたRPCはスキップし次のURLへ。
    """
    rpc_list = chain.get("rpc_list") or [chain.get("rpc", "")]
    for rpc_url in rpc_list:
        for attempt in range(2):  # 各RPCは2回まで試す
            try:
                r = requests.post(rpc_url, json={
                    "jsonrpc": "2.0", "id": 1,
                    "method": method, "params": params,
                }, timeout=15)
                if r.status_code == 200:
                    data = r.json()
                    if "error" in data:
                        err = data["error"]
                        # "no response"などは次のRPCへ
                        print(f"[{chain['name']}] RPC Error ({rpc_url.split('/')[2]}): {err}")
                        break  # このRPCを諦め次のURLへ
                    return data.get("result")
                print(f"[{chain['name']}] HTTP {r.status_code} ({rpc_url.split('/')[2]}) → 次のRPCへ")
                break  # 4xx/5xxは即次のRPCへ
            except Exception as e:
                print(f"[{chain['name']}] 接続エラー ({rpc_url.split('/')[2]}) attempt{attempt+1}: {e}")
                if attempt < 1:
                    time.sleep(1)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# オンチェーン流動性チェック（getReserves / balanceOf / Chainlink）
# ══════════════════════════════════════════════════════════════════════════════

def _get_native_price_usd(chain):
    """
    ChainlinkからBNB(BSC)またはETH(Base)のUSD価格を取得。
    5分キャッシュ。失敗時はキャッシュ値を返す（なければNone）。
    """
    global _native_price_cache
    chain_name = chain["name"]
    now = time.time()
    cached = _native_price_cache.get(chain_name)
    if cached and now - cached[1] < PRICE_CACHE_SEC:
        return cached[0]

    feed = CHAINLINK_BNB_USD if "BSC" in chain_name or "BNB" in chain_name else CHAINLINK_ETH_USD
    # latestRoundData() selector: 0xfeaf968c
    # returns: (roundId, answer, startedAt, updatedAt, answeredInRound)
    # answer は index=1 (2番目の32バイト), 8 decimal
    result = evm_rpc(chain, "eth_call", [{"to": feed, "data": "0xfeaf968c"}, "latest"])
    if result and len(result) >= 130:
        answer = int(result[66:130], 16)   # 2番目の32バイト
        price  = answer / 1e8
        _native_price_cache[chain_name] = (price, now)
        print(f"[{chain_name}] ネイティブ価格更新: ${price:,.0f}")
        return price

    # Chainlink失敗 → キャッシュが古くても使う
    return cached[0] if cached else None


def _get_v2_pair_liquidity_usd(pair_addr, token0, token1, chain):
    """
    PancakeSwap V2ペアの getReserves() を呼びUSD換算で流動性を返す。
    失敗・不明ペア → None。
    """
    try:
        # getReserves() selector: 0x0902f1ac
        # returns: uint112 reserve0, uint112 reserve1, uint32 timestamp
        r = evm_rpc(chain, "eth_call", [{"to": pair_addr, "data": "0x0902f1ac"}, "latest"])
        if not r or len(r) < 130:
            return None
        d        = r[2:]                     # "0x" を除去
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
        return None  # ベーストークン不明

    except Exception as e:
        print(f"[{chain['name']}] getReserves エラー: {e}")
        return None


def _get_v3_pool_liquidity_usd(pool_addr, token0, token1, chain):
    """
    Uniswap/PancakeSwap V3プールの balanceOf() でUSD換算流動性を返す。
    失敗・不明ペア → None。
    """
    def _balance_of(token_addr):
        # balanceOf(address) selector: 0x70a08231
        data = "0x70a08231" + "000000000000000000000000" + pool_addr[2:].lower().zfill(40)
        res  = evm_rpc(chain, "eth_call", [{"to": token_addr, "data": data}, "latest"])
        return int(res[2:66], 16) if res and len(res) >= 66 else 0

    try:
        t0, t1 = token0.lower(), token1.lower()

        if t1 in STABLE_ADDRS:
            bal = _balance_of(t1)
            return bal / (10 ** STABLE_DECIMALS[t1])
        if t0 in STABLE_ADDRS:
            bal = _balance_of(t0)
            return bal / (10 ** STABLE_DECIMALS[t0])
        if t1 in NATIVE_ADDRS:
            bal = _balance_of(t1)
            p   = _get_native_price_usd(chain)
            return (bal / 1e18) * p if p else None
        if t0 in NATIVE_ADDRS:
            bal = _balance_of(t0)
            p   = _get_native_price_usd(chain)
            return (bal / 1e18) * p if p else None
        return None

    except Exception as e:
        print(f"[{chain['name']}] V3 balanceOf エラー: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# EVM トークン処理スレッド（閾値ベース: 流動性$50k+ かつ トップ10≤60%）
# ══════════════════════════════════════════════════════════════════════════════

def _process_evm_token(token_address, chain, from_block,
                        pair_addr=None, token0=None, token1=None, is_v2=False):
    """
    新規EVMトークンの流動性・保有率チェックと通知。

    pair_addr あり（check_evm_all_chain から）:
        オンチェーン getReserves()/balanceOf() で即時チェック。
        万が一 reserve=0 なら 3秒 × 3回 リトライ後スキップ。
        → 平均通知まで 5〜15秒。

    pair_addr なし（check_evm_chain / FourMeme・Clanker から）:
        DexScreener を 10秒毎にポーリング（最大10分）。
        → 平均通知まで 30〜60秒（フォールバック）。
    """
    try:
        # ── オンチェーン直接チェック（pair_addr あり）──────────────────────────
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

            # 流動性OK → 保有率チェック
            holder_data = get_evm_holder_stats(token_address, chain, from_block)
            if holder_data is None:
                print(f"[{chain['name']}] 保有データ取得失敗 → スキップ")
                return

            top10 = holder_data["top10_ratio"]
            print(f"[{chain['name']}] トップ10保有率: {top10:.1f}%")
            if top10 > TOP10_MAX_PCT:
                print(f"[{chain['name']}] ❌ 保有集中高すぎ → スキップ")
                return

            # 通知（DexScreenerで追加情報を取得、なくても送信）
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

        # ── フォールバック: DexScreenerポーリング（pair_addr なし）────────────
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
# EVM チェーン監視（メインループから呼ばれる・即リターン）
# ══════════════════════════════════════════════════════════════════════════════

def check_evm_chain(chain):
    """
    新規トークンを検知したらすぐ別スレッドへ渡してリターン。
    待機処理は一切ここではやらない。
    """
    try:
        latest_hex = evm_rpc(chain, "eth_blockNumber", [])
        if not latest_hex:
            return
        latest_int = int(latest_hex, 16)

        # 初回: 現在ブロックを記録して終了
        if chain["last_block"] is None:
            chain["last_block"] = latest_int
            print(f"[{chain['name']}] 初期化完了: block={latest_int}")
            return

        # FIX: from_block = last_block+1（-20キャップ削除でブロック漏れ防止）
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

            # ★ 別スレッドに渡してすぐリターン → メインループは止まらない
            t = threading.Thread(
                target=_process_evm_token,
                args=(token_address, chain, latest_int),
                daemon=True,
            )
            t.start()

    except Exception as e:
        print(f"[{chain['name']}] チェックエラー: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# EVM 全般監視 (PancakeSwap V3 / Uniswap V3 PoolCreated)
# FourMeme/Clanker以外の全launchpadトークンを対象とする
# ══════════════════════════════════════════════════════════════════════════════



def check_evm_all_chain(chain):
    """
    DEX Factory の PoolCreated イベントを監視し、新規トークンを検知する。
    token0/token1 のうちベーストークン（WBNB/WETH/USDC等）でない方を新規トークンとして処理。
    known_tokens を FourMeme/Clanker と共有することで二重通知を防止。
    """
    try:
        latest_hex = evm_rpc(chain, "eth_blockNumber", [])
        if not latest_hex:
            return
        latest_int = int(latest_hex, 16)

        # 初回: 現在ブロックを記録して終了
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

        # V2(PairCreated) / V3(PoolCreated) でトピックが異なるため chain["topic"] を使用
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

        event_topic = chain.get("topic", POOL_CREATED_TOPIC)
        is_v2       = (event_topic == PAIR_CREATED_TOPIC)
        base_tokens = chain["base_tokens"]

        for log in logs:
            topics = log.get("topics", [])
            if len(topics) < 3:
                continue
            # topics[1]=token0, topics[2]=token1 (32バイト値、下位20バイト=アドレス)
            token0 = ("0x" + topics[1][-40:]).lower()
            token1 = ("0x" + topics[2][-40:]).lower()

            # ベーストークンでない方が「新規トークン」
            t0_is_base = token0 in base_tokens
            t1_is_base = token1 in base_tokens
            if not t0_is_base and t1_is_base:
                new_token = token0
            elif t0_is_base and not t1_is_base:
                new_token = token1
            else:
                continue  # 両方ベース or 両方非ベース → スキップ

            if new_token in chain["known_tokens"]:
                continue

            # ── ログデータからpair/poolアドレスを抽出 ────────────────────────
            # V2 PairCreated data: pair_addr(32B) + uint(32B)
            #   pair = data[2+24 : 2+64]  (先頭32Bの下位20B)
            # V3 PoolCreated data: tickSpacing(32B) + pool_addr(32B)
            #   pool = data[2+64+24 : 2+64+64]  (2番目32Bの下位20B)
            raw_data  = log.get("data", "0x")
            pair_addr = None
            try:
                if is_v2 and len(raw_data) >= 66:
                    pair_addr = "0x" + raw_data[26:66]    # V2: 先頭32Bの下位20B
                elif not is_v2 and len(raw_data) >= 130:
                    pair_addr = "0x" + raw_data[90:130]   # V3: 2番目32Bの下位20B
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
    _wait_for_rpc_slot()  # グローバル8 RPSレート制限
    for attempt in range(4):
        try:
            r = requests.post(SOLANA_RPC, json={
                "jsonrpc": "2.0", "id": 1,
                "method": method, "params": params,
            }, timeout=15)
            if r.status_code == 200:
                return r.json().get("result")
            if r.status_code == 429:
                # レート制限: 指数バックオフで待機
                wait = 2 ** attempt  # 1秒 → 2秒 → 4秒 → 8秒
                print(f"[Solana RPC] 429 レート制限 → {wait}秒待機 (attempt {attempt+1}/4)")
                time.sleep(wait)
                _wait_for_rpc_slot()  # リトライ前にも全スレッド共通の制限を通す
                continue
            print(f"[Solana RPC] HTTPエラー {r.status_code}: {r.text[:100]}")
        except Exception as e:
            print(f"[Solana RPC] 接続エラー ({method}): {e}")
        if attempt < 3:
            time.sleep(0.5)
    return None


def get_new_pumpfun_transactions():
    """
    last_signature以降の全新規TXをページネーションで取得。
    - 通常時（last_signatureあり）: until指定で新規TXを全件取得
    - 初回/初期化失敗時（last_signatureなし）: 最新50件のみ取得（遡り暴走防止）
    """
    global last_signature
    all_txns  = []
    before    = None
    is_catchup = (last_signature is None)  # 初回または初期化失敗フラグ

    while True:
        opts = {"limit": 50, "commitment": "confirmed"}
        if last_signature:
            opts["until"] = last_signature  # これ以降（新しい側）を取得
        if before:
            opts["before"] = before         # ページネーション用

        result = solana_rpc("getSignaturesForAddress", [PUMPFUN_PROGRAM, opts])
        if not result:
            break

        all_txns.extend(result)

        if len(result) < 50:
            break  # 50件未満 = 全件取得完了

        # 初回/初期化失敗時は最新50件だけで打ち切り（5000件遡り暴走防止）
        if is_catchup:
            print(f"[Pump.fun] 初回起動: 最新{len(all_txns)}件のみ処理（遡り制限）")
            break

        # ページネーション上限: 200件で打ち切り（84回APIコール防止）
        if len(all_txns) >= 200:
            print(f"[Pump.fun] ページネーション上限200件 → 打ち切り")
            break

        # 次ページ: 現在バッチの最古TXの前から取得
        before = result[-1].get("signature")
        time.sleep(0.1)  # ページネーション間のウェイト

    if all_txns:
        last_signature = all_txns[0].get("signature", "")
        if len(all_txns) > 1:
            print(f"[Pump.fun] {len(all_txns)}件の新規TX検出")

    return all_txns


def parse_new_token(signature):
    # Solanaシステムアドレスは新規トークンとして扱わない
    IGNORED_MINTS = {
        "So11111111111111111111111111111111111111112",  # Wrapped SOL (wSOL)
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", # USDC
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", # USDT
        "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",  # mSOL
        "7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj", # stSOL
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
        # False = getTransaction完全失敗（リトライ対象）
        # None  = TX取得成功だが新規mintなし（リトライ不要）
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
    """
    任意のSolana TXから新規ファンジブルトークンのmintを抽出。
    NFT(decimals=0)は除外し、ファンジブルトークン(decimals>=1)のみ返す。
    戻り値: False=getTransaction失敗 / None=対象mintなし / mint文字列
    """
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
        return False  # getTransaction完全失敗

    post_balances = result.get("meta", {}).get("postTokenBalances", [])
    pre_balances  = result.get("meta", {}).get("preTokenBalances", [])
    pre_mints = {b.get("mint") for b in pre_balances}

    for balance in post_balances:
        mint     = balance.get("mint", "")
        decimals = balance.get("uiTokenAmount", {}).get("decimals", 0)
        if not mint or mint in pre_mints or mint in IGNORED_MINTS:
            continue
        if decimals == 0:
            continue  # NFTをスキップ（NFTはdecimals=0）
        print(f"[Solana全般] 新規ファンジブルmint: {mint[:20]} (decimals={decimals})")
        return mint
    return None  # mintなし（メタデータ更新TXなど）






def get_solana_holder_stats(mint):
    """
    getTokenLargestAccounts + getTokenSupply で実際の保有量トップ10データを取得。
    戻り値: {"top10_ratio": float, "top10_detail": [str, ...]} / 失敗時は None。
    """
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
        return {
            "top10_ratio":  top10_ratio,
            "top10_detail": top10_detail,
        }
    except Exception as e:
        print(f"[保有量取得エラー] {e}")
        return None




def format_holder_output(holder_data):
    """
    実際の保有量トップ10データから通知用テキストを生成。
    Solana確定通知のウォレット分析欄に使用。
    """
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
    """
    EVM Transfer eventからトークン保有量を計算し、トップ10保有者データを返す。
    get_solana_holder_stats と同構造 → format_holder_output で共用可能。
    戻り値: {"top10_ratio": float, "top10_detail": [str, ...]} / 失敗時は None
    """
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

        # Transfer eventからバランスマップを構築
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

        # 正の残高のみ抽出
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

        return {
            "top10_ratio":  top10_ratio,
            "top10_detail": top10_detail,
        }

    except Exception as e:
        print(f"[{chain['name']}] EVM保有量トップ10取得エラー: {e}")
        return None




# ══════════════════════════════════════════════════════════════════════════════
# Solana トークン処理スレッド（閾値ベース: 流動性$50k+ かつ トップ10≤60%）
# pump.fun / Solana全般 共通
# ══════════════════════════════════════════════════════════════════════════════

def _process_solana_token(mint, label="Pump.fun", pump_link=True):
    """
    新規Solanaトークンを監視し、流動性$50k+ かつ トップ10保有率≤60% で即通知。
    DexScreenerを10秒毎にポーリング。タイムアウト10分。
    label: ログ・通知に使うチェーン/launchpad名
    pump_link: True → pump.funリンクを表示
    """
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
                # 流動性OK → 保有率チェック
                holder_data = get_solana_holder_stats(mint)
                if holder_data is None:
                    # 取得失敗 → 次のループで再試行
                    time.sleep(POLL_INTERVAL_SEC)
                    continue

                top10 = holder_data["top10_ratio"]
                print(f"[{label}] トップ10保有率: {top10:.1f}%")

                if top10 > TOP10_MAX_PCT:
                    print(f"[{label}] ❌ 保有集中高すぎ ({top10:.1f}% > {TOP10_MAX_PCT}%) → スキップ")
                    return

                # 両条件クリア → 通知
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
# Pump.fun 監視（メインループから呼ばれる・即リターン）
# ══════════════════════════════════════════════════════════════════════════════

def check_pumpfun_onchain():
    """
    新規mintを検知したらすぐ別スレッドへ渡してリターン。
    待機処理は一切ここではやらない。
    """
    global known_token_mints
    txns = get_new_pumpfun_transactions()
    if not txns:
        return

    # ── 古いTX除外（5分超はボンカーブ通知ウィンドウ外）────────────────────────
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
        time.sleep(0.5)  # Heliusレート制限対策（0.3→0.5秒、429削減）
        mint = parse_new_token(sig)
        if mint is False:
            # getTransaction完全失敗 → リトライキューへ保存（検知漏れ防止）
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

        # ★ 別スレッドに渡してすぐリターン → メインループは止まらない
        t = threading.Thread(
            target=_process_solana_token,
            args=(mint,),
            daemon=True,
        )
        t.start()


# ══════════════════════════════════════════════════════════════════════════════
# Pump.fun リトライキュー処理（検知漏れ防止）
# ══════════════════════════════════════════════════════════════════════════════

def process_retry_queue():
    """
    getTransactionが失敗したシグネチャを再試行。
    成功すれば通常通りスレッドを起動する。
    RETRY_EXPIRY秒以上経過したものは破棄。
    """
    global known_token_mints
    now = time.time()

    with RETRY_SIG_LOCK:
        if not RETRY_SIG_QUEUE:
            return
        # 有効期限内のものだけ取り出す
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
        time.sleep(1.0)  # リトライは長めに待つ（429対策）
        mint = parse_new_token(sig)

        if mint is False:
            # まだ失敗 → 有効期限内なら再キュー
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
# Solana 全般監視 (Token Metadata Program) ─ 独立バックグラウンドスレッドで動作
# pump.fun 以外の全launchpad (rapidlaunch.io / moonshot / letsbonk 等) を対象とする
# ══════════════════════════════════════════════════════════════════════════════

def get_new_metadata_transactions():
    """
    SPL Token Metadata Program の新規TXを取得。
    all_solana_last_signature 以降の新規TXのみ返す。
    """
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

        # ページネーション上限: 200件
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
    """DexScreener の dex_id から launchpad名を返す"""
    if not dex:
        return "Unknown"
    dex_id = dex.get("dex_id", "").lower()
    name_map = {
        "raydium":        "Raydium",
        "pump-fun":       "pump.fun",
        "pumpfun":        "pump.fun",
        "orca":           "Orca",
        "meteora":        "Meteora",
        "jupiter":        "Jupiter",
        "rapidlaunch":    "rapidlaunch.io",
        "moonshot":       "Moonshot",
        "letsbonk":       "LetsBonk",
    }
    for key, label in name_map.items():
        if key in dex_id:
            return label
    return dex_id.capitalize() if dex_id else "Unknown"


def _build_dex_text(dex):
    """DexScreener データからテキストを生成"""
    if not dex:
        return "📊 価格データ取得中...\n\n"
    return (
        f"💧 流動性: ${dex['liquidity']:,.0f}\n"
        f"📈 価格変動: {dex['price_change_5m']:+.1f}%/5分\n"
        f"🛒 買い{dex['buys_5m']}件 / 売り{dex['sells_5m']}件 (5分)\n\n"
    )


def check_all_solana_onchain():
    """
    Token Metadata Program の新規TXを監視し、全launchpadの新規トークンを検知。
    pump.fun で既に検知済みのトークンはスキップ（二重通知防止）。
    """
    global known_token_mints
    txns = get_new_metadata_transactions()
    if not txns:
        return

    # 古いTX除外（5分超は通知ウィンドウ外）
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
        time.sleep(0.6)  # pump.funより長め（NFT TXが多いため余裕を持つ）
        mint = parse_new_fungible_mint(sig)
        if mint is False or not mint:
            continue
        with KNOWN_MINTS_LOCK:
            if mint in known_token_mints:
                continue  # pump.fun で既に検知済み → スキップ
            known_token_mints.add(mint)
        new_count += 1
        print(f"[Solana全般] 新規ファンジブルmint → スレッド起動: {mint[:20]}")

        t = threading.Thread(
            target=_process_solana_token,
            args=(mint, "Solana全般", False),  # pump_link=False → solscanリンク
            daemon=True,
        )
        t.start()

    if new_count > 0:
        print(f"[Solana全般] {new_count}件の新規トークンスレッド起動")


def pumpfun_monitor_loop():
    """
    Pump.fun監視の独立ループ。メインループと並行して動作。
    10秒ごとにポーリング。TX上限なしで全件処理するため見逃しなし。
    """
    global last_signature
    print("[Pump.fun] 監視ループ開始中...")

    # 初期化: 起動時点の最新sigを記録し、過去mintを無視
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
        time.sleep(10)  # 10秒ごと（TXが溜まらないよう短くする）


def solana_all_monitor_loop():
    """
    Solana全般監視の独立ループ。メインループと並行して動作。
    60秒ごとに Token Metadata Program をポーリング。
    """
    global all_solana_last_signature
    print("[Solana全般] 監視ループ開始中...")

    # 初期化: 現時点の最新sigを記録し過去TXを無視
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
        time.sleep(60)  # 60秒ごと（pump.funの30秒ループと独立）


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

    # Pump.fun監視を独立バックグラウンドスレッドで起動（TX上限なし・10秒ごと）
    t_pumpfun = threading.Thread(target=pumpfun_monitor_loop, daemon=True)
    t_pumpfun.start()
    print("[Pump.fun] バックグラウンドスレッド起動完了")

    # Solana全般監視を独立バックグラウンドスレッドで起動
    t_all_solana = threading.Thread(target=solana_all_monitor_loop, daemon=True)
    t_all_solana.start()
    print("[Solana全般] バックグラウンドスレッド起動完了")

    loop = 0
    while True:
        # ── メインループはEVM監視のみ ──
        # CEXは30秒毎（6ループ × 5秒）、EVM検知は5秒毎で高速化
        if loop % 6 == 0:
            check_cex_listings()
        for chain in EVM_CHAINS:
            check_evm_chain(chain)
        for chain in EVM_ALL_CHAINS:
            check_evm_all_chain(chain)  # PairCreated/PoolCreated → オンチェーン即時チェック

        time.sleep(5)  # 30→5秒（EVM新規プール検知を高速化）
        loop += 1
        if loop % 360 == 0:  # 360 × 5秒 = 30分ごとにステータス表示
            evm_status = " ".join(
                f"{c['name']}={len(c['known_tokens'])}" for c in EVM_CHAINS
            )
            all_status = " ".join(
                f"{c['name']}={len(c['known_tokens'])}" for c in EVM_ALL_CHAINS
            )
            # 稼働中のスレッド数も表示
            active_threads = threading.active_count() - 1  # メインスレッド除く
            print(
                f"[{datetime.now().strftime('%H:%M')}] 稼働中 "
                f"CEX={len(known_cex_symbols)} "
                f"Solana={len(known_token_mints)} {evm_status} {all_status} "
                f"監視スレッド={active_threads}"
            )


if __name__ == "__main__":
    main()
