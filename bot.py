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
# 監視対象のTelegramグループ/チャンネルIDをカンマ区切りで設定
# 例: TELEGRAM_MONITOR_CHATS=-1001234567890,-1009876543210
# ※ BotをそのグループにAdminで追加してから設定すること
TELEGRAM_MONITOR_CHATS = [
    int(x.strip()) for x in os.environ.get('TELEGRAM_MONITOR_CHATS', '').split(',')
    if x.strip().lstrip('-').isdigit()
]

# ── エンドポイント ────────────────────────────────────────────────────────────
BITGET_SYMBOLS_URL  = "https://api.bitget.com/api/v2/spot/public/symbols"

# Solana RPC ラウンドロビンリスト（429対策: 複数RPC分散）
# Helius(有料/無料10RPS) + Publicnode(無料×2) + 公式(無料) で実効帯域を分散
# ※除外済み: rpc.ankr.com/solana(403), go.getblock.io(404),
#            solana.drpc.org(521 Cloudflareダウン), endpoints.omniatech.io(400 freetier不可)
_SOLANA_RPC_LIST = [r for r in [
    f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}" if HELIUS_KEY else None,
    "https://solana.publicnode.com",
    "https://solana-rpc.publicnode.com",
    "https://api.mainnet-beta.solana.com",
] if r]
SOLANA_RPC = _SOLANA_RPC_LIST[0]  # 後方互換性のために残す
_solana_rpc_idx      = 0
_solana_rpc_idx_lock = threading.Lock()

# ── EVM定数 ──────────────────────────────────────────────────────────────────
TRANSFER_TOPIC  = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO_TOPIC      = "0x0000000000000000000000000000000000000000000000000000000000000000"
PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

# ── Solana全般監視: SPL Token Metadata Program (全launchpad対応) ───────────────
# pump.fun / rapidlaunch.io / moonshot など、Solanaの全launchpadはこのプログラムに
# トークンのメタデータ(名前・シンボル)を登録するため、ここを見れば全て拾える
SPL_METADATA_PROGRAM = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"

# ── Raydium直接監視定数 ──────────────────────────────────────────────────────
# Raydium AMM V4 (旧来の標準AMM) と CPMM (新型) の両プログラムを監視
RAYDIUM_AMM_V4    = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
RAYDIUM_CPMM      = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"

# ── Orca / Meteora 監視定数 ──────────────────────────────────────────────────
# Orca Whirlpool (集中流動性AMM) と Meteora DLMM / Dynamic AMM を監視
# parse_raydium_new_pool の postTokenBalances ベース検出はDEX共通で動作する
ORCA_WHIRLPOOL    = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
METEORA_DLMM      = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"
METEORA_AMM       = "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EkAW7vAr"
WSOL_MINT         = "So11111111111111111111111111111111111111112"
USDC_MINT         = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT         = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
# プール作成TXの新規mintと判定しないベーストークン一覧
SOLANA_BASE_TOKENS = {
    WSOL_MINT,   # Wrapped SOL
    USDC_MINT,   # USDC
    USDT_MINT,   # USDT
    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",  # mSOL
    "7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj",  # stSOL
}

# ── EVM全般監視定数 ────────────────────────────────────────────────────────────
# Uniswap V3 / PancakeSwap V3 共通の PoolCreated イベントトピック
POOL_CREATED_TOPIC      = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"
PANCAKE_V3_FACTORY_BSC  = "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"  # PancakeSwap V3
UNISWAP_V3_FACTORY_BASE = "0x33128a8fC17869897dcE68Ed026d694621f6FDfD"  # Uniswap V3 on Base
# PancakeSwap V2 の PairCreated イベントトピック（BSCミームトークンの主流）
PAIR_CREATED_TOPIC      = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
PANCAKE_V2_FACTORY_BSC  = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"  # PancakeSwap V2

# 流動性追加イベント（Mint）トピック
# V2: Mint(address indexed sender, uint256 amount0, uint256 amount1)
V2_MINT_TOPIC = "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f"
# V3: Mint(address sender, address indexed owner, int24 indexed tickLower,
#          int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)
V3_MINT_TOPIC = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"

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

# ── EVM全般監視チェーン（PoolCreated経由で全launchpad対応）─────────────────────
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

# ── グローバル状態 ────────────────────────────────────────────────────────────
known_cex_symbols = set()
known_token_mints = set()

# ── CA事前登録監視（SNS検知 / 手動 /watch コマンド）────────────────────────
_ca_watch      = {}
_ca_watch_lock = threading.Lock()

# ── 遅延ローンチ監視（作成から20分後に取引開始するトークン）────────────────
_pending_tokens      = {}
_pending_lock        = threading.Lock()

# 通知済みトークンのセット（二重通知防止）
_notified_tokens     = set()
_notified_lock       = threading.Lock()

# Telegram getUpdates の offset（重複受信防止）
_tg_update_offset = None
_tg_update_lock   = threading.Lock()

# EVM / Solana アドレス抽出用正規表現
_EVM_ADDR_RE = re.compile(r'0x[a-fA-F0-9]{40}')
_SOL_ADDR_RE = re.compile(
    r'(?:CA|ca|contract|mint|address|token|アドレス|Contract Address)'
    r'\s*[:：]\s*([1-9A-HJ-NP-Za-km-z]{32,44})'
)
last_signature    = None
all_solana_last_signature = None

# Raydium直接監視: 各プログラムの最終処理済みシグネチャ
raydium_last_sigs = {RAYDIUM_AMM_V4: None, RAYDIUM_CPMM: None}

# Orca / Meteora 監視: 各プログラムの最終処理済みシグネチャ
orca_meteora_last_sigs = {ORCA_WHIRLPOOL: None, METEORA_DLMM: None, METEORA_AMM: None}

# SOL価格キャッシュ: [price_usd, timestamp]
_sol_price_cache  = [None, 0.0]

# ── 閾値ベース通知フィルター ──────────────────────────────────────────────────
LIQUIDITY_MIN       = 10_000
TOP10_MAX_PCT       = 60.0
POLL_INTERVAL_SEC   = 3
MONITOR_TIMEOUT_SEC = 300

# ── スレッドセーフ: known_token_mintsの競合書き込み防止 ─────────────────────
KNOWN_MINTS_LOCK = threading.Lock()

# ── Solana同時getTransaction上限（429対策）────────────────────────────────────
_SOLANA_SEMAPHORE = threading.Semaphore(4)

# ── 5分監視スレッド同時実行上限（OSスレッド枯渇対策）────────────────────────
_MONITOR_SEMAPHORE = threading.Semaphore(50)

# ── Workerスレッドプール（OSスレッド枯渇の根本対策）──────────────────────────
#   Pump.fun:   最大6並列
#   Solana全般: 最大4並列
#   Raydium:    最大4並列
_PUMPFUN_POOL    = ThreadPoolExecutor(max_workers=6, thread_name_prefix="pumpfun")
_SOLANA_ALL_POOL = ThreadPoolExecutor(max_workers=4, thread_name_prefix="solana_all")
_RAYDIUM_POOL    = ThreadPoolExecutor(max_workers=4, thread_name_prefix="raydium")

# ── 検知漏れ防止: getTransaction失敗シグネチャのリトライキュー ──────────────
RETRY_SIG_QUEUE = []
RETRY_SIG_LOCK  = threading.Lock()
RETRY_EXPIRY    = 300

# ── Solana RPCグローバルレート制限（8 RPS）────────────────────────────────────
# 利用RPC: Helius(無料10RPS) / Publicnode×2 / 公式 の計4本にラウンドロビン。
# 1本あたり実効2 RPS × 4本 = 8 RPS。Helius無料枠(10RPS)を超えず429を防止。
_SOLANA_RPS_LIMIT  = 8
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


# ══════════════════════════════════════════════════════════════════════════════
# Solana SOL価格取得（Jupiter Price API）
# ══════════════════════════════════════════════════════════════════════════════

def _get_sol_price_usd():
    global _sol_price_cache
    now = time.time()
    if _sol_price_cache[0] and now - _sol_price_cache[1] < PRICE_CACHE_SEC:
        return _sol_price_cache[0]
    try:
        url = f"https://price.jup.ag/v6/price?ids=SOL"
        r = requests.get(url, headers=HEADERS, timeout=8)
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
            "image_url":       (pair.get("info") or {}).get("imageUrl", ""),
        }
    except Exception as e:
        print(f"DexScreenerエラー: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# PUMP.FUN 独自API
# ══════════════════════════════════════════════════════════════════════════════

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
        url = f"https://frontend-api.pump.fun/coins/{mint}"
        r = requests.get(url, headers=_pf_headers, timeout=10)
        if r.status_code != 200:
            print(f"[Pump.fun API] HTTP {r.status_code} ({mint[:16]})")
            return None
        data = r.json()
        if not data:
            return None
        usd_market_cap = float(data.get("usd_market_cap") or 0)
        complete       = bool(data.get("complete", False))
        name           = data.get("name", "")
        symbol         = data.get("symbol", "")
        return {
            "liquidity": usd_market_cap,
            "complete":  complete,
            "name":      name,
            "symbol":    symbol,
        }
    except Exception as e:
        print(f"[Pump.fun API] エラー: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# アイコン（画像）チェック
# ══════════════════════════════════════════════════════════════════════════════

def _has_token_icon(key: str, chain: str, dex: dict = None) -> bool:
    """
    【Solana】:
      1. 渡されたdexデータの image_url → 即OK
      2. pump.fun API の image_uri → ソーシャル不要で即OK
      3. DexScreener の info.imageUrl OR info.socials/websites → OK
    【EVM（BSC/Base）】:
      1. 渡されたdexデータの image_url → 即OK
      2. DexScreener の info.imageUrl → OK
      3. DexScreener の info.socials/websites が1つでも存在 → OK
    """
    if dex and dex.get("image_url"):
        print(f"[アイコン] dexデータ imageUrl あり ({chain}): {key[:20]}")
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
            r = requests.get(
                f"https://frontend-api.pump.fun/coins/{key}",
                headers=_pf_headers, timeout=8,
            )
            if r.status_code == 200:
                data = r.json()
                if data and data.get("image_uri"):
                    has_social = bool(
                        data.get("twitter") or
                        data.get("website") or
                        data.get("telegram")
                    )
                    if has_social:
                        print(f"[アイコン] pump.fun image_uri + social あり: {key[:20]}")
                    else:
                        print(f"[アイコン] pump.fun image_uri あり (social なし): {key[:20]}")
                    return True  # social有無に関わらず image_uri があればOK
        except Exception:
            pass

    # DexScreener で info.imageUrl / socials / websites をチェック（全チェーン共通）
    try:
        r = requests.get(
            f"https://api.dexscreener.com/latest/dex/tokens/{key}",
            headers=HEADERS, timeout=8,
        )
        if r.status_code == 200:
            for pair in (r.json().get("pairs") or []):
                info = pair.get("info") or {}
                if info.get("imageUrl"):
                    print(f"[アイコン] DexScreener imageUrl あり: {key[:20]}")
                    return True
                if info.get("socials") or info.get("websites"):
                    print(f"[アイコン] DexScreener socials/websites あり ({chain}): {key[:20]}")
                    return True
    except Exception:
        pass

    print(f"[アイコン] なし → スキップ: {key[:20]}")
    return False


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
                }, timeout=8)
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


# ══════════════════════════════════════════════════════════════════════════════
# オンチェーン流動性チェック（getReserves / balanceOf / Chainlink）
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
# DEX Factory ペアアドレス検索
# ══════════════════════════════════════════════════════════════════════════════

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
        native  = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
        factory_v2 = PANCAKE_V2_FACTORY_BSC
        factory_v3 = PANCAKE_V3_FACTORY_BSC
        v3_fees    = (500, 2500, 10000, 100)
    else:
        native  = "0x4200000000000000000000000000000000000006"
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


# ══════════════════════════════════════════════════════════════════════════════
# Mint イベント監視
# ══════════════════════════════════════════════════════════════════════════════

def _wait_for_liquidity_mint(pair_addr, token_address, chain, from_block,
                              token0, token1, is_v2):
    mint_topic = V2_MINT_TOPIC if is_v2 else V3_MINT_TOPIC
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
        t_holder.start()
        t_dex.start()
        t_holder.join()
        t_dex.join()

        holder_data = holder_result[0]
        dex         = dex_result[0]

        if holder_data is None:
            print(f"[{chain['name']}] 保有データ取得失敗 → 監視継続")
            continue

        top10 = holder_data["top10_ratio"]
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
                    time.sleep(1)
                    print(f"[{chain['name']}] 流動性リトライ {attempt+2}/3: {pair_addr[:12]}")

            liq_str = f"${liq:,.0f}" if liq else "$0"
            print(f"[{chain['name']}] オンチェーン流動性: {liq_str} ({token_address[:12]})")

            if liq is None or liq < LIQUIDITY_MIN:
                print(f"[{chain['name']}] オンチェーン流動性不足(${liq or 0:,.0f})"
                      f" → Mint監視へ: {token_address[:12]}")
                _wait_for_liquidity_mint(
                    pair_addr, token_address, chain, from_block,
                    token0, token1, is_v2,
                )
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
                t_holder.start()
                t_dex.start()
                t_holder.join()
                t_dex.join()

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

                top10 = holder_data["top10_ratio"]
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
                _wait_for_liquidity_mint(
                    f_pair, token_address, chain, from_block,
                    f_t0, f_t1, f_is_v2,
                )
                return

            time.sleep(3)

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
            _register_pending_token(token_address.lower(), "evm", chain["name"])
            print(f"[{chain['name']}] 新規トークン検知 → 遅延監視に登録: {token_address}")

    except Exception as e:
        print(f"[{chain['name']}] チェックエラー: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# EVM 全般監視 (PancakeSwap V3 / Uniswap V3 PoolCreated)
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

        event_topic = chain.get("topic", POOL_CREATED_TOPIC)
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
                pair_addr=pair_addr,
                token0=token0, token1=token1,
                is_v2=is_v2,
                evm_chain=chain,
            )
            print(f"[{chain['name']}] 新規トークン検知 → 遅延監視に登録: {new_token}"
                  f" pair={pair_addr[:12] if pair_addr else 'None'}")

    except Exception as e:
        print(f"[{chain['name']}] チェックエラー: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# SOLANA RPC
# ══════════════════════════════════════════════════════════════════════════════

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
        return {
            "top10_ratio":  top10_ratio,
            "top10_detail": top10_detail,
        }
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

        return {
            "top10_ratio":  top10_ratio,
            "top10_detail": top10_detail,
        }

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


# ══════════════════════════════════════════════════════════════════════════════
# Pump.fun 監視
# ══════════════════════════════════════════════════════════════════════════════

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
        print(f"[Pump.fun] 古いTX除外: {before_filter - len(txns)}件スキップ"
              f"（残り{len(txns)}件）")

    sigs = [tx.get("signature", "") for tx in txns
            if tx.get("signature") and not tx.get("err")]
    if not sigs:
        return
    print(f"[Pump.fun] {len(sigs)}件を並列処理開始")
    for sig in sigs:
        _PUMPFUN_POOL.submit(_handle_pumpfun_sig, sig)


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
        _register_pending_token(mint, "sol", "Pump.fun")
        print(f"[Pump.fun] ✅ リトライ成功！mint → 遅延監視に登録: {mint[:20]}")

    if still_failed:
        with RETRY_SIG_LOCK:
            RETRY_SIG_QUEUE.extend(still_failed)
        print(f"[Pump.fun] リトライ再キュー: {len(still_failed)}件")


# ══════════════════════════════════════════════════════════════════════════════
# Solana 全般監視 (Token Metadata Program)
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
        print(f"[Solana全般] 古いTX除外: {before_filter - len(txns)}件スキップ"
              f"（残り{len(txns)}件）")

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


# ══════════════════════════════════════════════════════════════════════════════
# Raydium直接監視
# ══════════════════════════════════════════════════════════════════════════════

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
        mint     = b.get("mint", "")
        ui_amt   = float((b.get("uiTokenAmount") or {}).get("uiAmount") or 0)
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
            platform = "Raydium"
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


def _handle_orca_meteora_tx(sig, label):
    """
    Orca Whirlpool / Meteora シグネチャを処理するワーカー関数。
    parse_raydium_new_pool は postTokenBalances ベースの汎用検出のため
    Raydium以外のDEXでも動作する。
    """
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
            print(f"[{label}] 遅延ローンチ検知！ {mint[:20]} age={age/60:.0f}分 liq=${liq_usd:,.0f}")
            _notify_delayed_launch(mint, "sol", liq_usd, age, label)
        else:
            with _pending_lock:
                _pending_tokens.pop(mint, None)
            print(f"[{label}] 早期取引開始({age/60:.0f}分) → 除外: {mint[:20]}")
        return

    with KNOWN_MINTS_LOCK:
        if mint in known_token_mints:
            return
        known_token_mints.add(mint)

    if liq_usd >= LIQUIDITY_MIN:
        print(f"[{label}] 流動性OK ${liq_usd:,.0f} → 通知処理: {mint[:20]}")
        _RAYDIUM_POOL.submit(_process_raydium_token, mint, liq_usd)
    else:
        _register_pending_token(mint, "sol", label)
        print(f"[{label}] 新規mint → 遅延監視に登録: {mint[:20]} (${liq_usd:,.0f})")


def check_orca_meteora_onchain():
    """
    Orca Whirlpool / Meteora DLMM / AMM の新規TXを並列処理。
    limit:30（Raydiumの50より少なく: Orca/Meteoraの高ボリューム対策）
    """
    global orca_meteora_last_sigs
    now = time.time()

    for program, label in [
        (ORCA_WHIRLPOOL, "Orca"),
        (METEORA_DLMM,   "Meteora_DLMM"),
        (METEORA_AMM,    "Meteora_AMM"),
    ]:
        opts = {"limit": 30, "commitment": "confirmed"}
        if orca_meteora_last_sigs[program]:
            opts["until"] = orca_meteora_last_sigs[program]

        txns = solana_rpc("getSignaturesForAddress", [program, opts])
        if not txns:
            continue

        orca_meteora_last_sigs[program] = txns[0].get("signature", "")

        # 直近10分以内のTXのみ処理（Orcaは高ボリュームのためスワップを早期除去）
        txns = [tx for tx in txns
                if not tx.get("blockTime") or (now - tx["blockTime"]) <= 600]
        if not txns:
            continue

        sigs = [tx.get("signature", "") for tx in txns
                if tx.get("signature") and not tx.get("err")]
        if not sigs:
            continue

        print(f"[{label}] {len(sigs)}件を並列処理開始")
        for sig in sigs:
            _RAYDIUM_POOL.submit(_handle_orca_meteora_tx, sig, label)


def orca_meteora_monitor_loop():
    """
    Orca Whirlpool / Meteora の独立監視ループ。15秒ごとにポーリング。
    """
    global orca_meteora_last_sigs
    print("[Orca/Meteora] 監視ループ開始中...")

    for program, label in [
        (ORCA_WHIRLPOOL, "Orca"),
        (METEORA_DLMM,   "Meteora_DLMM"),
        (METEORA_AMM,    "Meteora_AMM"),
    ]:
        if orca_meteora_last_sigs[program] is not None:
            print(f"[{label}] 起動スキャン済み → 通常監視開始")
            continue
        init = solana_rpc("getSignaturesForAddress", [program, {"limit": 5}])
        if init:
            orca_meteora_last_sigs[program] = init[0].get("signature", "")
            print(f"[{label}] 初期化完了 sig={orca_meteora_last_sigs[program][:20]}")
        else:
            print(f"[{label}] 初期化失敗（RPC応答なし）→ 次回から取得")

    while True:
        try:
            check_orca_meteora_onchain()
        except Exception as e:
            print(f"[Orca/Meteora] ループエラー: {e}")
        time.sleep(15)


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
        init  = solana_rpc("getSignaturesForAddress", [program, {"limit": 5}])
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


# ══════════════════════════════════════════════════════════════════════════════
# CA事前登録監視 / SNS（Telegram）自動検知
# ══════════════════════════════════════════════════════════════════════════════

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
    """遅延ローンチ確定時の通知（重複防止付き）"""
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

                    token0  = info.get("token0")
                    token1  = info.get("token1")
                    is_v2   = info.get("is_v2", True)
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


def _check_pumpfun_pending(key: str, info: dict):
    try:
        pf = analyze_pumpfun_api(key)
        if not pf:
            return
        liq = pf.get("liquidity", 0)
        if liq < LIQUIDITY_MIN:
            return
        age = time.time() - info.get("created_at", time.time())
        source = info.get("source", "pump.fun")
        print(f"[pump.fun fallback] 通知: {key[:20]} liq=${liq:,.0f} age={age/60:.1f}min")
        _notify_delayed_launch(key, "sol", liq, age, source, None)
    except Exception as e:
        print(f"[pump.fun fallback] エラー {key[:20]}: {e}")


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
                    liq  = liq_map.get(key.lower(), 0)
                    info = targets.get(key)
                    if not info:
                        continue

                    if liq < LIQUIDITY_MIN:
                        if info.get("chain") == "sol":
                            last_pf = info.get("pumpfun_last_check", 0)
                            if now - last_pf >= 300:
                                with _pending_lock:
                                    if key in _pending_tokens:
                                        _pending_tokens[key]["pumpfun_last_check"] = now
                                        _PUMPFUN_POOL.submit(
                                            _check_pumpfun_pending, key, dict(info)
                                        )
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
