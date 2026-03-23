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
        # mainnet.base.org → drpc → publicnode の順
        # llamarpc: 403 / 1rpc.io: SSL EOF → 除外
        # publicnode: eth_getLogs ブロック範囲 ~200制限 → 最後尾
        "rpc_list": [
            "https://mainnet.base.org",
            "https://base.drpc.org",
            "https://base-rpc.publicnode.com",
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
        "top10_max_pct": 80.0,  # BSCミームは保有集中が多いため80%に緩和
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
        # mainnet.base.org → drpc → publicnode の順
        # llamarpc: 403 / 1rpc.io: SSL EOF → 除外
        # publicnode: eth_getLogs ブロック範囲 ~200制限 → 最後尾
        "rpc_list": [
            "https://mainnet.base.org",
            "https://base.drpc.org",
            "https://base-rpc.publicnode.com",
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
known_token_mints = set()   # pump.fun + Solana全般 + Raydium共通（重複通知防止）

# ── 遅延ローンチ監視（作成から20分後に取引開始するトークン）────────────────
# 検知済みの全トークンを保存し、20分後から流動性チェックを開始する
# {token_key: {"chain": "sol"/"evm", "created_at": float, "source": str,
#              "evm_chain": dict|None, "pair_addr": str|None, ...}}
_pending_tokens      = {}
_pending_lock        = threading.Lock()

# 通知済みトークンのセット（二重通知防止）
_notified_tokens     = set()
_notified_lock       = threading.Lock()

# Solana起動スキャン完了フラグ（_startup_notify_scanとの同期用）
_solana_startup_done = threading.Event()

# EVM / Solana アドレス抽出用正規表現
_EVM_ADDR_RE = re.compile(r'0x[a-fA-F0-9]{40}')
# Solanaアドレスは誤検知しやすいため「CA:」等のキーワード必須
_SOL_ADDR_RE = re.compile(
    r'(?:CA|ca|contract|mint|address|token|アドレス|Contract Address)'
    r'\s*[:：]\s*([1-9A-HJ-NP-Za-km-z]{32,44})'
)
last_signature    = None    # pump.fun 用
all_solana_last_signature = None  # Solana全般（Metadata Program）用

# Raydium直接監視: 各プログラムの最終処理済みシグネチャ
raydium_last_sigs = {RAYDIUM_AMM_V4: None, RAYDIUM_CPMM: None}

# Orca / Meteora 監視: 各プログラムの最終処理済みシグネチャ
orca_meteora_last_sigs = {ORCA_WHIRLPOOL: None, METEORA_DLMM: None, METEORA_AMM: None}

# SOL価格キャッシュ: [price_usd, timestamp]
_sol_price_cache  = [None, 0.0]

# ── 閾値ベース通知フィルター ──────────────────────────────────────────────────
TOP10_MAX_PCT       = 60.0     # トップ10保有率上限（%）: これ超えでスキップ
POLL_INTERVAL_SEC   = 3        # DexScreenerポーリング間隔（秒）: 10→3秒
MONITOR_TIMEOUT_SEC = 300      # 監視タイムアウト（5分）: 未達成はスキップ

# ── スレッドセーフ: known_token_mintsの競合書き込み防止 ─────────────────────
# pump.fun（バックグラウンド）とSolana全般（バックグラウンド）が同時に
# 同一mintを検知して二重通知するのを防ぐ
KNOWN_MINTS_LOCK = threading.Lock()

# ── Solana同時getTransaction上限（429対策）────────────────────────────────────
# 全スレッド合計で同時にgetTransactionを呼べるのは4つまで。
# _SOLANA_RPS_LIMIT=8 と揃えることで同時RPC数がRPS制限を超えないよう制御する。
_SOLANA_SEMAPHORE = threading.Semaphore(4)

# ── 5分監視スレッド同時実行上限（OSスレッド枯渇対策）────────────────────────
# _process_solana_token / _process_raydium_token は最大5分生き続ける。
# 無制限に起動するとOSのスレッド上限(32768)に到達し
# "新しいスレッドを開始できません"エラーが発生する。
# 同時に最大50スレッドまでに制限する（超過分はブロック待機）。
_MONITOR_SEMAPHORE = threading.Semaphore(50)

# ── Workerスレッドプール（OSスレッド枯渇の根本対策）──────────────────────────
# _handle_pumpfun_sig / _handle_metadata_sig / _handle_raydium_tx は
# _SOLANA_SEMAPHORE(4) でブロック待機するため、バッチごとに50件ずつ
# スレッドを生成し続けるとOSのスレッド上限に到達する。
# ThreadPoolExecutor はスレッドを再利用するため上限到達が発生しない。
#   Pump.fun:   最大6並列（10秒バッチを6スレッドで処理）
#   Solana全般: 最大4並列（_SOLANA_RPS_LIMIT=8に合わせて3プール合計8 RPSを抑制）
#   Raydium:    最大4並列（3プール×4=12スレッドが_wait_for_rpc_slotで8 RPS以内に調整）
_PUMPFUN_POOL    = ThreadPoolExecutor(max_workers=6, thread_name_prefix="pumpfun")
_SOLANA_ALL_POOL = ThreadPoolExecutor(max_workers=4, thread_name_prefix="solana_all")
_RAYDIUM_POOL    = ThreadPoolExecutor(max_workers=4, thread_name_prefix="raydium")

# ── 検知漏れ防止: getTransaction失敗シグネチャのリトライキュー ──────────────
# parse_new_tokenでgetTransactionが全試行失敗した場合にここへ保存し、
# 次のメインループで再試行する（5分以内に成功しなければ破棄）
RETRY_SIG_QUEUE = []               # [(signature, enqueued_time), ...]
RETRY_SIG_LOCK  = threading.Lock()
RETRY_EXPIRY    = 300              # 秒: 5分

# ── Solana RPCグローバルレート制限（8 RPS）────────────────────────────────────
# 全スレッド合計で1秒間に呼べるSolana RPC数の上限。
# 利用RPC: Helius(無料10RPS) / Publicnode×2 / 公式 の計4本にラウンドロビン。
# 1本あたり実効2 RPS × 4本 = 8 RPS。Helius無料枠(10RPS)を超えず429を防止。
# ※ Helius有料プランに切り替えた場合は 24〜48 に引き上げ可能。
_SOLANA_RPS_LIMIT  = 8
_solana_rpc_times  = []            # 直近1秒間のRPC呼び出しタイムスタンプ
_solana_rpc_lock   = threading.Lock()


def _wrapped_process_solana(mint, label="Pump.fun", is_pumpfun=True):
    """
    _process_solana_token を _MONITOR_SEMAPHORE で保護したラッパー。
    同時50スレッドを超えた場合はブロック待機してから実行する。
    スレッド対象はこちらを使い _process_solana_token は直接呼ばない。
    """
    with _MONITOR_SEMAPHORE:
        _process_solana_token(mint, label, is_pumpfun)


def _wrapped_process_raydium(mint, liq_usd):
    """
    _process_raydium_token を _MONITOR_SEMAPHORE で保護したラッパー。
    同時50スレッドを超えた場合はブロック待機してから実行する。
    """
    with _MONITOR_SEMAPHORE:
        _process_raydium_token(mint, liq_usd)


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
# Solana SOL価格取得（Jupiter Price API）
# ══════════════════════════════════════════════════════════════════════════════

def _get_sol_price_usd():
    """
    Jupiter Price APIからSOL/USDを取得。5分キャッシュ。
    失敗時はキャッシュ値を返す（なければ150ドルのデフォルト値）。
    """
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
    # フォールバック: キャッシュが古くても使う、なければ$150
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
            "dex_id":          pair.get("dexId", ""),   # launchpad特定用
            "image_url":       (pair.get("info") or {}).get("imageUrl", ""),  # アイコンURL
        }
    except Exception as e:
        print(f"DexScreenerエラー: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# PUMP.FUN 独自API（DexScreenerより高速・インデックス遅延なし）
# ══════════════════════════════════════════════════════════════════════════════

def analyze_pumpfun_api(mint):
    """
    pump.fun 独自APIでトークンデータを取得。
    DexScreenerのインデックス遅延（30〜90秒）を排除し 5〜15秒で応答。
    bonding curve卒業前はusd_market_capを流動性代わりに使用。
    戻り値: {"liquidity": float, "complete": bool, "name": str, "symbol": str} / None
    Cloudflare(530)対策: pump.fun同一オリジンに見せるブラウザ偽装ヘッダーを使用。
    """
    # Cloudflare回避: pump.fun内部リクエストに見せかけるヘッダー
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
            print(f"[Pump.fun API] 空レスポンス ({mint[:16]})")
            return None
        usd_market_cap = float(data.get("usd_market_cap") or 0)
        complete       = bool(data.get("complete", False))  # True=Raydiumに卒業済み
        name           = data.get("name", "")
        symbol         = data.get("symbol", "")
        return {
            "liquidity": usd_market_cap,  # market capを流動性の代替として使用
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
    トークンがアイコン（画像）を持つかチェック。
    アイコンなしトークン（素のSPLトークン・スパム）はFalseを返す。

    【Solana】:
      1. 渡されたdexデータの image_url → 即OK
      2. pump.fun API の image_uri → ソーシャル不要で即OK（ソーシャルは品質加点のみ）
      3. DexScreener の info.imageUrl OR info.socials/websites → OK
    【EVM（BSC/Base）】:
      1. 渡されたdexデータの image_url → 即OK
      2. DexScreener の info.imageUrl → OK
      3. DexScreener の info.socials/websites が1つでも存在 → OK
         ※ imageUrl は手動申請必須のため、新規トークンは未登録が多い。
           ソーシャル情報でカバー。取りこぼしは /watch CA で手動監視。
    """
    # ── 渡されたdexデータに image_url があれば即OK（EVM/Solana共通）────────
    if dex and dex.get("image_url"):
        print(f"[アイコン] dexデータ imageUrl あり ({chain}): {key[:20]}")
        return True

    if chain == "sol":
        # ── pump.fun API で image_uri をチェック ──────────────────────────
        # ソーシャルリンクは必須条件ではなく加点要素
        # image_uri があれば → グレー背景テキスト文字ではなく実際の画像がある = OK
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

    # ── DexScreener で info.imageUrl / socials / websites をチェック（全チェーン共通）──
    # imageUrl: 手動申請必須。未申請の新規トークンはsocialsで代替判定
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
                # imageUrl 未登録でも socials/websites があれば本物プロジェクトと判断
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
                }, timeout=8)
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
# DEX Factory ペアアドレス検索（FourMeme/Clanker高速化用）
# getPair(V2) / getPool(V3) を直接クエリしてペアアドレスを即取得
# ══════════════════════════════════════════════════════════════════════════════

def _get_v2_pair(token_a, token_b, factory, chain):
    """
    PancakeSwap V2 Factory の getPair(tokenA, tokenB) を呼びペアアドレスを返す。
    ペアが存在しない場合は None。
    """
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
    """
    Uniswap/PancakeSwap V3 Factory の getPool(tokenA, tokenB, fee) を呼びプールアドレスを返す。
    プールが存在しない場合は None。
    fee: 100 / 500 / 2500 / 3000 / 10000
    """
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
    """
    FourMeme/BSC または Clanker/Base のトークンに対して
    PancakeSwap V2/V3 または Uniswap V3 でペアアドレスを検索。
    見つかれば (pair_addr, token0, token1, is_v2) を返す。見つからなければ None。

    BSC: WBNB ペアを V2 → V3(500/2500/10000) の順で試す
    Base: WETH ペアを V3(500/3000/10000/100) の順で試す
    """
    ta  = token_addr.lower()
    is_bsc = "BSC" in chain["name"] or "BNB" in chain["name"] or "FourMeme" in chain["name"]

    if is_bsc:
        native  = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"  # WBNB
        factory_v2 = PANCAKE_V2_FACTORY_BSC
        factory_v3 = PANCAKE_V3_FACTORY_BSC
        v3_fees    = (500, 2500, 10000, 100)
    else:
        native  = "0x4200000000000000000000000000000000000006"  # WETH (Base)
        factory_v2 = None
        factory_v3 = UNISWAP_V3_FACTORY_BASE
        v3_fees    = (500, 3000, 10000, 100)

    nb = native.lower()
    # token0/token1 は小文字辞書順
    t0, t1 = (ta, nb) if ta < nb else (nb, ta)

    # V2 を先に試す（BSCのみ）
    if factory_v2:
        pair = _get_v2_pair(ta, native, factory_v2, chain)
        if pair:
            return pair, t0, t1, True

    # V3 を fee tier 順に試す
    for fee in v3_fees:
        pool = _get_v3_pool_addr(ta, native, factory_v3, fee, chain)
        if pool:
            return pool, t0, t1, False

    return None  # まだペアが存在しない


# ══════════════════════════════════════════════════════════════════════════════
# Mint イベント監視（流動性追加を直接検知 → DexScreener不要で5〜10秒通知）
# ══════════════════════════════════════════════════════════════════════════════

def _wait_for_liquidity_mint(pair_addr, token_address, chain, from_block,
                              token0, token1, is_v2):
    """
    Pair/Pool の Mint イベントを 3秒ごとにポーリングし、
    流動性追加を検知した瞬間に getReserves/balanceOf → 即通知。

    DexScreenerを使わないためインデックス遅延（30〜90秒）を完全排除。
    平均通知時間: 5〜10秒（Pool作成〜流動性追加のタイムラグ次第）。

    タイムアウト10分: 流動性が追加されなければスキップ。
    """
    mint_topic = V2_MINT_TOPIC if is_v2 else V3_MINT_TOPIC
    deadline           = time.time() + MONITOR_TIMEOUT_SEC
    last_checked_block = from_block

    print(f"[{chain['name']}] Mint監視開始: {pair_addr[:12]} ({token_address[:12]})")

    while time.time() < deadline:
        time.sleep(3)  # 3秒ごとにチェック（10秒→3秒で応答性向上）

        latest_hex = evm_rpc(chain, "eth_blockNumber", [])
        if not latest_hex:
            continue
        latest_int = int(latest_hex, 16)
        if latest_int <= last_checked_block:
            continue

        # Mint イベントをペアコントラクトアドレス宛に絞って取得
        logs = evm_rpc(chain, "eth_getLogs", [{
            "fromBlock": hex(last_checked_block + 1),
            "toBlock":   hex(latest_int),
            "address":   pair_addr,
            "topics":    [mint_topic],
        }])
        last_checked_block = latest_int

        if logs:
            print(f"[{chain['name']}] ⚡ Mint検知！即getReserves: {pair_addr[:12]}")
        # Mintイベントなしでも流動性を直接チェック（爆上がり検知）
        # ← if not logs: continue を廃止し毎ループ流動性を見る

        # 流動性を毎ループ取得（Mintイベントあり/なし問わず）
        if is_v2:
            liq = _get_v2_pair_liquidity_usd(pair_addr, token0, token1, chain)
        else:
            liq = _get_v3_pool_liquidity_usd(pair_addr, token0, token1, chain)

        if not liq:
            continue  # 流動性なし → 監視継続

        print(f"[{chain['name']}] 流動性OK: ${liq:,.0f} → 保有率チェックへ")

        # 流動性OK → holder_stats と DexScreener を並列取得
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
        # チェーンごとのtop10_max_pctを優先、なければグローバル値を使用
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
# EVM トークン処理スレッド（閾値ベース: 流動性$10k+ かつ トップ10≤60%）
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
                    time.sleep(1)   # 旧3秒→1秒（リトライ合計最大2秒に短縮）
                    print(f"[{chain['name']}] 流動性リトライ {attempt+2}/3: {pair_addr[:12]}")

            liq_str = f"${liq:,.0f}" if liq else "$0"
            print(f"[{chain['name']}] オンチェーン流動性: {liq_str} ({token_address[:12]})")

            # liq=0 → Mint イベント監視へ（DexScreener不要）
            # 流動性追加TXを直接検知するため 30〜90秒の遅延を排除し 5〜10秒で通知
            if not liq:
                print(f"[{chain['name']}] オンチェーン流動性なし(${liq or 0:,.0f})"
                      f" → Mint監視へ: {token_address[:12]}")
                _wait_for_liquidity_mint(
                    pair_addr, token_address, chain, from_block,
                    token0, token1, is_v2,
                )
                return  # Mint監視内で通知まで完結

            else:
                # 流動性OK → holder_stats と DexScreener を並列取得（直列→並列で数秒短縮）
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
                    # 保有データ取得失敗でもスキップせず流動性のみで通知（見逃し防止）
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

        # ── フォールバック: Factory直接クエリ → Mint監視（pair_addr なし）────────
        # FourMeme/Clanker用: getPair/getPoolを3秒ごとにポーリングしてペアを発見
        # ペア発見後は即 _wait_for_liquidity_mint へ渡す（DexScreener不要）
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
                return  # Mint監視内で通知まで完結

            time.sleep(3)  # 3秒ごとにペア検索

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
            _register_pending_token(token_address.lower(), "evm", chain["name"])
            print(f"[{chain['name']}] 新規トークン検知 → 遅延監視に登録: {token_address}")

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
    _wait_for_rpc_slot()  # グローバルRPSレート制限
    for attempt in range(4):
        # ラウンドロビン: 呼び出しごとに次のRPCへ（429時は強制ローテート）
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
                # このRPCが429 → 次のRPCへ（短め待機）
                wait = 2 ** attempt  # 1秒 → 2秒 → 4秒 → 8秒
                rpc_name = url.split("//")[1].split("/")[0][:20]
                print(f"[Solana RPC] 429 ({rpc_name}) → {wait}秒待機 (attempt {attempt+1}/4)")
                time.sleep(wait)
                _wait_for_rpc_slot()
                continue
            if r.status_code == 403:
                # 403はAPIキー不要RPCでは永続的に失敗 → 即次のRPCへ（待機なし）
                rpc_name = url.split("//")[1].split("/")[0][:20]
                print(f"[Solana RPC] 403 ({rpc_name}) APIキー必須 → 次のRPCへ")
                break  # このattemptをスキップし次のattempt（次のRPC）へ
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

        # ページネーション上限: 50件で打ち切り
        # 200件→50件に削減: 10秒ポーリングで50件÷8スレッド≒9秒で処理完了し遅延蓄積を防ぐ
        if len(all_txns) >= 50:
            print(f"[Pump.fun] ページネーション上限50件 → 打ち切り")
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

        # publicnodeなど無料RPCのブロック範囲上限に対応（5000ブロック上限）
        effective_from = max(from_block, latest - 5000)

        # トークン生成直後でTransferがまだない場合は3秒待ってリトライ
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
    pump.funトークン（pump_link=True）:
        pump.fun独自APIを優先使用（インデックス遅延なし → 5〜15秒）。
        API失敗・卒業済みはDexScreenerにフォールバック。
    Solana全般（pump_link=False）:
        まずpump.fun APIを1回試行し、使えればfast path（高速化 → 15〜45秒）。
        使えない場合はDexScreenerを3秒毎にポーリング（タイムアウト5分）。
    """
    try:
        deadline = time.time() + MONITOR_TIMEOUT_SEC
        print(f"[{label}] 閾値監視開始: {mint[:20]}"
              f" (トップ10≤{TOP10_MAX_PCT}%)")

        # pump_link=False でも pump.fun API が使えるかチェック（Solana全般高速化）
        effective_pump = pump_link
        if not pump_link:
            pf_probe = analyze_pumpfun_api(mint)
            if pf_probe:
                effective_pump = True  # pump.fun APIが使えるのでfast pathへ
                print(f"[{label}] pump.fun API使用可 → fast path切替")

        while time.time() < deadline:

            # ── pump.fun独自API優先（pump.funトークン or Solana全般でAPI使用可）──
            if effective_pump:
                pf = analyze_pumpfun_api(mint)
                if pf:
                    liq = pf["liquidity"]
                    print(f"[{label}] pump.fun API 時価総額: ${liq:,.0f} ({mint[:16]})")

                    if liq > 0:
                        holder_data = get_solana_holder_stats(mint)
                        if holder_data is None:
                            time.sleep(POLL_INTERVAL_SEC)
                            continue

                        top10 = holder_data["top10_ratio"]
                        print(f"[{label}] トップ10保有率: {top10:.1f}%")
                        if top10 > TOP10_MAX_PCT:
                            print(f"[{label}] ❌ 保有集中高すぎ → スキップ")
                            return

                        # DexScreenerでも補足データ取得（なくても通知する）
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
                # pump.fun API失敗 → 以降DexScreener一本化（530連打を防ぐ）
                effective_pump = False

            # ── DexScreenerポーリング（Solana全般 / pump.fun APIフォールバック）──
            dex = analyze_dexscreener(mint)
            if not dex:
                time.sleep(POLL_INTERVAL_SEC)
                continue

            liq = dex['liquidity']
            print(f"[{label}] DexScreener 流動性: ${liq:,.0f} ({mint[:16]})")

            if liq > 0:
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
# Pump.fun 監視（メインループから呼ばれる・即リターン）
# ══════════════════════════════════════════════════════════════════════════════

def _handle_pumpfun_sig(sig):
    """Pump.fun シグネチャを並列処理するワーカー関数"""
    # セマフォで同時getTransaction数を8に制限し429を根絶
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
    """
    新規mintを並列で一気に処理。_wait_for_rpc_slot()でRPS制限管理。
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

    # ── 並列処理（全件を同時にスレッド起動）────────────────────────────────────
    # _wait_for_rpc_slot()がグローバル8RPSを管理するため429は発生しない
    sigs = [tx.get("signature", "") for tx in txns
            if tx.get("signature") and not tx.get("err")]
    if not sigs:
        return
    print(f"[Pump.fun] {len(sigs)}件を並列処理開始")
    for sig in sigs:
        _PUMPFUN_POOL.submit(_handle_pumpfun_sig, sig)


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
        _register_pending_token(mint, "sol", "Pump.fun")
        print(f"[Pump.fun] ✅ リトライ成功！mint → 遅延監視に登録: {mint[:20]}")

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


def _handle_metadata_sig(sig):
    """Solana全般 シグネチャを並列処理するワーカー関数"""
    # セマフォで同時getTransaction数を8に制限し429を根絶
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
    """
    Token Metadata Program の新規TXを並列で一気に処理。
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

    # ── 並列処理（全件を同時にスレッド起動）────────────────────────────────────
    sigs = [tx.get("signature", "") for tx in txns
            if tx.get("signature") and not tx.get("err")]
    if not sigs:
        return
    print(f"[Solana全般] {len(sigs)}件を並列処理開始")
    for sig in sigs:
        _SOLANA_ALL_POOL.submit(_handle_metadata_sig, sig)


def pumpfun_monitor_loop():
    """
    Pump.fun監視の独立ループ。メインループと並行して動作。
    10秒ごとにポーリング。TX上限なしで全件処理するため見逃しなし。
    """
    global last_signature
    print("[Pump.fun] 監視ループ開始中...")

    # 起動スキャン未実施の場合のみ初期化（スキャン済みなら現在の最新sigを設定）
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
        time.sleep(3)   # 10→3秒（Pump.fun検知速度3倍改善）


def solana_all_monitor_loop():
    """
    Solana全般監視の独立ループ。メインループと並行して動作。
    60秒ごとに Token Metadata Program をポーリング。
    """
    global all_solana_last_signature
    print("[Solana全般] 監視ループ開始中...")

    # 起動スキャン未実施の場合のみ初期化
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
        time.sleep(5)   # 60→15→5秒（Solana全般検知速度改善）


# ══════════════════════════════════════════════════════════════════════════════
# Raydium直接監視（プール作成TXを直接検知 → DexScreener不要で5〜15秒通知）
# AMM V4 / CPMM の getSignaturesForAddress でポーリング
# postTokenBalances の新規mint + WSOL/USDC残高からUSD流動性を即計算
# ══════════════════════════════════════════════════════════════════════════════

def parse_raydium_new_pool(signature):
    """
    Raydium TX を解析し、新規プール作成なら (mint, liq_usd) を返す。
    スワップTX（既存mintのみ）は None を返してスキップ。
    getTransaction失敗時は False を返す（リトライ対象）。
    """
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

    meta          = result.get("meta", {}) or {}
    post_balances = meta.get("postTokenBalances", []) or []
    pre_balances  = meta.get("preTokenBalances",  []) or []
    pre_mints     = {b.get("mint") for b in pre_balances}

    # 新規mint検出: postにあってpreにない、かつベーストークン以外
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
        return None  # スワップTX → スキップ

    # USD流動性を postTokenBalances の WSOL/USDC 残高から計算
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
    """
    Raydium新規プールのトークンを通知。
    liq_usd > 0 なら即保有率チェック → 通知。
    未達なら DexScreener ポーリングにフォールバック。
    """
    try:
        if liq_usd > 0:
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
            # 流動性不足 → DexScreener ポーリング（タイムアウト5分）
            print(f"[Raydium] 流動性不足 ${liq_usd:,.0f} → DexScreener監視: {mint[:20]}")
            _process_solana_token(mint, "Raydium", False)

    except Exception as e:
        print(f"[Raydium] スレッドエラー ({mint[:20]}): {e}")


def _handle_raydium_tx(sig):
    """Raydium シグネチャを処理するワーカー関数"""
    with _SOLANA_SEMAPHORE:
        parsed = parse_raydium_new_pool(sig)
    if parsed is False or parsed is None:
        return
    mint, liq_usd = parsed
    now = time.time()

    # ── 既に pending 登録済みのトークンがRaydiumプールを作成した場合 ──────
    with _pending_lock:
        pending_info = _pending_tokens.get(mint)

    if pending_info is not None:
        age = now - pending_info["created_at"]
        if age >= 20 * 60:
            # 20分以上経過 → 遅延ローンチ確定！オンチェーンで即通知
            print(f"[Raydium] 遅延ローンチ検知！ {mint[:20]} age={age/60:.0f}分 liq=${liq_usd:,.0f}")
            _notify_delayed_launch(mint, "sol", liq_usd, age, "Raydium")
        else:
            # 20分未満 → 早期取引開始 → pending から削除（通知なし）
            with _pending_lock:
                _pending_tokens.pop(mint, None)
            print(f"[Raydium] 早期取引開始({age/60:.0f}分) → 除外: {mint[:20]}")
        return

    # ── 新規mint（初めて検知）────────────────────────────────────────────
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

    # 既にpending登録済みのトークンがOrca/Meteoraプールを作成した場合
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

    # 新規mint（初めて検知）
    with KNOWN_MINTS_LOCK:
        if mint in known_token_mints:
            return
        known_token_mints.add(mint)

    if liq_usd > 0:
        # 流動性OK → 即通知フロー（Raydiumと同じ _process_raydium_token を再利用）
        print(f"[{label}] 流動性OK ${liq_usd:,.0f} → 通知処理: {mint[:20]}")
        _RAYDIUM_POOL.submit(_process_raydium_token, mint, liq_usd)
    else:
        # 流動性不足 → 遅延監視に登録
        _register_pending_token(mint, "sol", label)
        print(f"[{label}] 新規mint → 遅延監視に登録: {mint[:20]} (${liq_usd:,.0f})")


def check_orca_meteora_onchain():
    """
    Orca Whirlpool / Meteora DLMM / AMM の新規TXを並列処理。
    getSignaturesForAddress で orca_meteora_last_sigs 以降を取得。
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
    Raydiumの5秒より遅め（高ボリューム対策・RPC節約）。
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
        time.sleep(15)  # 15秒ごと（Raydiumの5秒より遅め）


def check_raydium_onchain():
    """
    Raydium AMM V4 / CPMM の新規TXを並列で処理。
    getSignaturesForAddress で raydium_last_sigs 以降を取得。
    """
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

        # 最新シグを更新
        raydium_last_sigs[program] = txns[0].get("signature", "")

        # 古いTX除外（5分超）
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
    """
    Raydium直接監視の独立ループ。10秒ごとにポーリング。
    AMM V4: 流動性追加～通知まで 5〜15秒
    CPMM  : 流動性追加～通知まで 5〜15秒
    """
    global raydium_last_sigs
    print("[Raydium] 監視ループ開始中...")

    # 起動スキャン未設定のプログラムのみ初期化
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
        time.sleep(5)  # 5秒ごと（高速化）


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
    """
    Bitget コイン情報APIからチェーン別コントラクトアドレスを取得。
    戻り値例: {"BSC": "0xabc...", "Solana": "mint...", "Ethereum": "0x..."}
    アドレスなし or API失敗時は {}。
    """
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


def _notify_delayed_launch(key: str, chain: str, liq_usd: float, age: float, source: str, dex: dict = None):
    """遅延ローンチ確定時の通知（重複防止付き）"""
    # ── アイコンチェック ──────────────────────────────────────────────────────
    # 取引開始済み + アイコンなし → 削除（監視終了）
    # ※ 未取引トークンのアイコン変化は pending_watch_loop ③ で別途検知
    if not _has_token_icon(key, chain, dex):
        with _pending_lock:
            _pending_tokens.pop(key, None)
        print(f"[遅延ローンチ] アイコンなし → 削除: {key[:20]}")
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
    """
    新規検知トークンを遅延ローンチ監視リストに登録。
    20分後から流動性チェックを開始し、取引開始の瞬間に通知する。
    """
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
    """
    EVMの遅延ローンチを eth_getLogs で直接監視。
    pair_addr が登録されているトークンのMintイベントを3秒ごとにチェック。
    DexScreenerの30〜90秒遅延を排除し、取引開始から数秒で通知する。
    """
    # チェーンごとの最終チェックブロック
    last_blocks = {}

    while True:
        try:
            now = time.time()

            # pair_addr があるEVMトークンだけ抽出
            with _pending_lock:
                evm_items = {
                    k: v for k, v in _pending_tokens.items()
                    if v.get("chain") == "evm" and v.get("pair_addr") and v.get("evm_chain")
                }

            # チェーンごとにグループ化
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

                # 現在ブロック取得
                latest_hex = evm_rpc(chain, "eth_blockNumber", [])
                if not latest_hex:
                    continue
                latest_int = int(latest_hex, 16)
                from_block = last_blocks.get(chain_name, latest_int - 3)
                last_blocks[chain_name] = latest_int

                if from_block > latest_int:
                    continue

                # pair_addrリストを一括でLogs検索（V2 Mint イベント）
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

                # Mintイベントがあったpairアドレスのセット
                minted_pairs = {log["address"].lower() for log in logs}

                for key, info in items:
                    pair_addr = (info.get("pair_addr") or "").lower()
                    if pair_addr not in minted_pairs:
                        continue

                    age = now - info["created_at"]

                    if age < 20 * 60:
                        # 20分未満 → 早期取引 → 削除
                        with _pending_lock:
                            _pending_tokens.pop(key, None)
                        print(f"[EVM遅延監視] 早期取引({age/60:.0f}分) → 除外: {key[:16]}")
                        continue

                    # 流動性チェック
                    token0  = info.get("token0")
                    token1  = info.get("token1")
                    is_v2   = info.get("is_v2", True)
                    if is_v2:
                        liq = _get_v2_pair_liquidity_usd(pair_addr, token0, token1, chain)
                    else:
                        liq = _get_v3_pool_liquidity_usd(pair_addr, token0, token1, chain)

                    if not liq:
                        continue

                    print(f"[EVM遅延監視] 遅延ローンチ検知！ {key[:16]} age={age/60:.0f}分 liq=${liq:,.0f}")
                    _notify_delayed_launch(key, "evm", liq, age, chain_name)

        except Exception as e:
            print(f"[EVM遅延監視] エラー: {e}")

        time.sleep(3)  # 3秒ごと（EVMブロック生成は3〜5秒）


def _check_pumpfun_pending(key: str, info: dict):
    """
    pending中のSolanaトークンをpump.fun APIで確認（DexScreener $0 fallback）。
    ボンディングカーブ中はDexScreenerの流動性が$0だが、
    pump.fun APIのusd_market_capは実態を反映しているため、
    流動性あれば即通知する。
    """
    try:
        pf = analyze_pumpfun_api(key)
        if not pf:
            return
        liq = pf.get("liquidity", 0)  # usd_market_cap
        if not liq:
            return
        age = time.time() - info.get("created_at", time.time())
        source = info.get("source", "pump.fun")
        print(f"[pump.fun fallback] 通知: {key[:20]} liq=${liq:,.0f} age={age/60:.1f}min")
        _notify_delayed_launch(key, "sol", liq, age, source, None)
    except Exception as e:
        print(f"[pump.fun fallback] エラー {key[:20]}: {e}")


def pending_watch_loop():
    """
    作成から20分経過したトークンの流動性を5秒ごとにチェック。
    流動性が追加された瞬間に通知する。
    24時間経過で自動削除。
    """
    WAIT_SEC         = 20 * 60    # 20分待機してから通知対象
    MAX_SEC          = 8 * 3600   # 8時間で削除
    BATCH_SIZE       = 30         # DexScreener バッチ上限
    MAX_WATCH        = 2000       # 最大監視件数
    EARLY_CHECK_INTV = 5 * 60     # 早期取引開始チェックは5分おき

    last_early_check = 0.0        # 最後に早期チェックした時刻

    while True:
        try:
            now = time.time()

            # ── 期限切れ・通知済みを削除 ────────────────────────────────────
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

                # 最大件数超過 → 古い順に削除
                if len(_pending_tokens) > MAX_WATCH:
                    sorted_keys = sorted(
                        _pending_tokens,
                        key=lambda k: _pending_tokens[k]["created_at"]
                    )
                    for k in sorted_keys[:len(_pending_tokens) - MAX_WATCH]:
                        _pending_tokens.pop(k, None)

                all_tokens = dict(_pending_tokens)

            # ── ① 早期取引開始チェック（5分おき・20分未満のトークン）────────
            # 取引が20分以内に始まったトークンをリストから素早く除去する
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

                    # 流動性があるものだけ抽出
                    has_liq = set()
                    for pair in pairs:
                        addr = (pair.get("baseToken") or {}).get("address", "")
                        liq  = (pair.get("liquidity") or {}).get("usd", 0) or 0
                        if addr and liq > 0:
                            has_liq.add(addr.lower())

                    for key in batch:
                        if key.lower() in has_liq:
                            info_e  = early_targets.get(key, {})
                            chain_e = info_e.get("chain", "sol")
                            if not _has_token_icon(key, chain_e):
                                # 20分未満 + アイコンなし → スパム確定、削除
                                with _pending_lock:
                                    _pending_tokens.pop(key, None)
                                print(f"[遅延監視] 早期取引+アイコンなし → スパム除去: {key[:20]}...")
                            # else: 20分未満 + アイコンあり → 維持（直接監視で通知済みのはず）

                    time.sleep(0.3)

            # ── ② 通知チェック（20分以上経過したトークン）─────────────────
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

                    if not liq:
                        # ── Solana: pump.fun APIフォールバック ────────────────────
                        # ボンディングカーブ中はDexScreener流動性=$0 だが
                        # pump.fun APIのusd_market_capは実態を反映している。
                        # 5分おきに再チェック（1回限りではなく定期チェック）。
                        if info.get("chain") == "sol":
                            last_pf = info.get("pumpfun_last_check", 0)
                            if now - last_pf >= 300:  # 5分経過したら再チェック
                                with _pending_lock:
                                    if key in _pending_tokens:
                                        _pending_tokens[key]["pumpfun_last_check"] = now
                                        _PUMPFUN_POOL.submit(
                                            _check_pumpfun_pending, key, dict(info)
                                        )
                        continue

                    # pairCreatedAt で20分以内取引開始を最終確認
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

            # ── ③ アイコン出現チェック（20分以上経過した全トークン）──────────────
            # 流動性あり・なし問わず、アイコンが設定された瞬間に即通知する。
            # 取引開始前でもアイコン変更を検知して通知（5分おき）。
            ICON_CHECK_INTV = 5 * 60  # 5分おき
            icon_waiting = {
                k: v for k, v in all_tokens.items()
                if now - v["created_at"] >= WAIT_SEC               # 20分以上経過した全件
                and now - v.get("icon_last_checked", 0) >= ICON_CHECK_INTV
            }
            for key, info in icon_waiting.items():
                # チェック時刻を更新（次の5分まで再チェックしない）
                with _pending_lock:
                    if key in _pending_tokens:
                        _pending_tokens[key]["icon_last_checked"] = now

                chain = info.get("chain", "sol")
                # 最新DexScreenerデータで再チェック
                try:
                    fresh_dex = analyze_dexscreener(key)
                except Exception:
                    fresh_dex = None

                if not _has_token_icon(key, chain, fresh_dex):
                    continue  # まだアイコンなし → 次の5分後に再チェック

                # ── アイコン出現！通知 ─────────────────────────────────────
                with _notified_lock:
                    if key in _notified_tokens:
                        continue
                    _notified_tokens.add(key)
                with _pending_lock:
                    _pending_tokens.pop(key, None)

                liq_display = (fresh_dex or {}).get("liquidity") or info.get("liq_usd") or 0
                has_trade   = liq_display > 0
                age_icon    = now - info["created_at"]
                age_h       = int(age_icon // 3600)
                age_m       = int((age_icon % 3600) // 60)
                age_str     = f"{age_h}時間{age_m}分" if age_h > 0 else f"{age_m}分"
                dex_url     = (
                    f"https://dexscreener.com/solana/{key}" if chain == "sol"
                    else f"https://dexscreener.com/{key}"
                )

                if has_trade:
                    title      = "🎨 <b>[アイコン追加] 取引中トークンがアイコンを設定！</b>"
                    liq_line   = f"💧 流動性: <b>${liq_display:,.0f}</b>\n"
                    pc5        = (fresh_dex or {}).get("price_change_5m", 0) or 0
                    b5         = (fresh_dex or {}).get("buys_5m", 0) or 0
                    s5         = (fresh_dex or {}).get("sells_5m", 0) or 0
                    trade_line = f"📈 5分変動: {pc5:+.1f}%\n🛒 買い{b5}件 / 売り{s5}件 (5分)\n\n"
                else:
                    title      = "🎨 <b>[アイコン追加] 未取引トークンがアイコンを設定！</b>"
                    liq_line   = "💧 まだ取引開始していません\n"
                    trade_line = "\n"

                send_telegram(
                    f"{title}\n\n"
                    f"📋 アドレス: <code>{key}</code>\n"
                    f"⛓ チェーン: {chain.upper()}\n"
                    f"⏱ 作成から <b>{age_str}後</b> にアイコン設定\n\n"
                    f"{liq_line}"
                    f"{trade_line}"
                    f"🔗 {dex_url}"
                )
                print(f"[アイコン追加] ✅ 通知: {key[:20]} age={age_str} liq=${liq_display:,.0f} 取引={'あり' if has_trade else 'なし'}")
                time.sleep(0.3)

        except Exception as e:
            print(f"[遅延監視] エラー: {e}")

        time.sleep(10)


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

            # チェーン別コントラクトアドレスを取得
            addresses = get_bitget_contract_addresses(base)

            # アドレス表示テキストを生成（最大5チェーン）
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


# ══════════════════════════════════════════════════════════════════════════════
# 起動時スキャン（デプロイ直後に過去20分〜8時間のトークンを遅延監視に登録）
# ══════════════════════════════════════════════════════════════════════════════

def _startup_scan_one_evm(chain, is_all, now, min_age, max_age):
    """
    EVM 1チェーン分の起動時スキャン。
    ブロック番号から作成時刻を推定し、20分〜8時間のトークンのみ登録。
    """
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
    """
    Solana起動時スキャン（バックグラウンド実行）。
    Raydium AMM V4 / CPMM / Orca Whirlpool / Meteora DLMM / AMM を対象に
    過去20分〜8時間のプール作成TXを解析。
    getTransactionを0.5秒間隔でレート制限し429を回避。
    """
    global raydium_last_sigs, orca_meteora_last_sigs

    # last_sigs の参照マップ（プログラムアドレス → どの dict に書くか）
    _last_sigs_map = {
        RAYDIUM_AMM_V4: (raydium_last_sigs,      RAYDIUM_AMM_V4),
        RAYDIUM_CPMM:   (raydium_last_sigs,      RAYDIUM_CPMM),
        ORCA_WHIRLPOOL: (orca_meteora_last_sigs,  ORCA_WHIRLPOOL),
        METEORA_DLMM:   (orca_meteora_last_sigs,  METEORA_DLMM),
        METEORA_AMM:    (orca_meteora_last_sigs,  METEORA_AMM),
    }

    for program, label in [
        (RAYDIUM_AMM_V4, "Raydium_AMM_V4"),
        (RAYDIUM_CPMM,   "Raydium_CPMM"),
        (ORCA_WHIRLPOOL, "Orca"),
        (METEORA_DLMM,   "Meteora_DLMM"),
        (METEORA_AMM,    "Meteora_AMM"),
    ]:
        first_sig  = None
        before     = None
        to_process = []  # [(sig, block_time), ...]

        # Step1: blockTimeだけ見てsigを収集（getTransaction不要）
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

        # last_sig を設定（通常ループへ引き継ぎ）
        # last_sig をプログラムに対応する dict へ書き込み（通常ループへ引き継ぎ）
        if first_sig:
            sig_dict, sig_key = _last_sigs_map[program]
            sig_dict[sig_key] = first_sig

        if not to_process:
            continue

        print(f"[起動スキャン/{label}] {len(to_process)}件を処理開始（バックグラウンド）")
        count = 0

        # Step2: getTransactionでmint抽出（レート制限つき）
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

    # 全プログラムのスキャン完了 → 通知スキャンに知らせる
    _solana_startup_done.set()
    print("[起動スキャン/Solana] 全プログラム完了 → 通知スキャンに通知")


def _startup_scan():
    """
    デプロイ直後に過去20分〜8時間のトークンを遅延監視リストに事前登録する。
    - EVM: 5チェーンを並列スキャン（eth_getLogs → 30〜60秒で完了）
    - Solana: Raydiumをバックグラウンドスレッドでスキャン（429対策）
    Pump.fun / SPL_METADATA は量が多すぎるためスキップ（通常ループで補完）
    """
    now     = time.time()
    MIN_AGE = 20 * 60   # 20分
    MAX_AGE = 8 * 3600  # 8時間

    print(f"[起動スキャン] 開始: 過去20分〜8時間のトークンを遅延監視に登録中...")

    # ── EVM: 全チェーンを並列スキャン ────────────────────────────────────────
    total  = [0]
    lock   = threading.Lock()

    def _run(chain, is_all):
        n = _startup_scan_one_evm(chain, is_all, now, MIN_AGE, MAX_AGE)
        with lock:
            total[0] += n

    threads = []
    for chain in EVM_CHAINS:
        t = threading.Thread(target=_run, args=(chain, False), daemon=True)
        t.start()
        threads.append(t)
    for chain in EVM_ALL_CHAINS:
        t = threading.Thread(target=_run, args=(chain, True), daemon=True)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    print(f"[起動スキャン] EVM完了: {total[0]}件を遅延監視に登録")

    # ── Solana: Raydiumのみバックグラウンドスキャン ──────────────────────────
    threading.Thread(
        target=_startup_scan_solana, args=(now, MIN_AGE, MAX_AGE), daemon=True
    ).start()
    print("[起動スキャン] Solana(Raydium)はバックグラウンドでスキャン中...")


def _periodic_pumpfun_scan_loop():
    """
    30分ごとにpump.fun APIをスキャンし、429等でgetTransactionが失敗して
    _pending_tokensに登録されなかったトークンを救済する。
    起動スキャンと同じロジックを定期実行（getTransactionに依存しない）。
    """
    # 最初の実行は起動スキャンと重複するため45分待ってから開始
    time.sleep(45 * 60)
    while True:
        try:
            print("[定期スキャン/pump.fun] 開始...")
            _startup_pumpfun_notify_scan()
        except Exception as e:
            print(f"[定期スキャン/pump.fun] エラー: {e}")
        time.sleep(30 * 60)  # 30分ごと


def _startup_pumpfun_notify_scan():
    """
    起動前に作成されたpump.funトークンの補完スキャン。
    pump.fun APIで「直近取引されたコイン」を降順取得し、
    20分〜8時間前に作成 + アイコンあり + 時価総額$10k+ のものを即通知。

    pump.funは1日数千件作成されるためRPCスキャンは不可。
    last_trade_timestampソートで最近アクティブなもの優先 → 最大500件チェック。
    """
    now     = time.time()
    MIN_AGE = 20 * 60    # 20分
    MAX_AGE = 8 * 3600   # 8時間

    _pf_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                      " (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://pump.fun/",
        "Origin": "https://pump.fun",
    }

    print("[起動スキャン/pump.fun] 最近アクティブなコインをスキャン開始（最大500件）")
    notified = 0
    checked  = 0

    for page in range(10):   # 10ページ × 50件 = 最大500件
        try:
            r = requests.get(
                "https://frontend-api.pump.fun/coins"
                f"?offset={page * 50}&limit=50"
                "&sort=last_trade_timestamp&order=DESC&includeNsfw=false",
                headers=_pf_headers, timeout=12,
            )
            if r.status_code != 200:
                print(f"[起動スキャン/pump.fun] HTTP {r.status_code} → スキャン終了")
                break
            coins = r.json()
            if not coins:
                break
        except Exception as e:
            print(f"[起動スキャン/pump.fun] APIエラー: {e}")
            break

        found_in_range = False
        for coin in coins:
            created_ms = coin.get("created_timestamp") or 0
            if not created_ms:
                continue
            created_at = created_ms / 1000.0   # ms → sec
            age        = now - created_at

            if age < MIN_AGE:
                continue   # まだ20分未経過 → スキップ
            if age > MAX_AGE:
                continue   # 8時間超え → スキップ（このページには混在する）

            found_in_range = True
            mint = coin.get("mint", "")
            if not mint:
                continue

            # 既に通知済みならスキップ
            with _notified_lock:
                if mint in _notified_tokens:
                    continue

            # アイコンチェック（image_uri が必須）
            if not coin.get("image_uri"):
                continue

            market_cap = float(coin.get("usd_market_cap") or 0)
            checked += 1
            age_m = int(age // 60)
            age_h = int(age // 3600)
            age_str = f"{age_h}時間{int((age % 3600) // 60)}分" if age_h > 0 else f"{age_m}分"
            print(f"[起動スキャン/pump.fun] 対象発見: {coin.get('name','?')} "
                  f"age={age_str} mc=${market_cap:,.0f}")

            # 通知（_notify_delayed_launch はアイコンチェック再実行するがpump.fun確認済み）
            with _notified_lock:
                if mint in _notified_tokens:
                    continue
                _notified_tokens.add(mint)
            with _pending_lock:
                _pending_tokens.pop(mint, None)

            name_sym = f"{coin.get('name','')} (${coin.get('symbol','')})\n" if coin.get("name") else ""
            send_telegram(
                f"🚀 <b>[Solana/pump.fun] 起動前トークン検知！</b>\n\n"
                f"時刻: {datetime.now().strftime('%H:%M:%S')}\n"
                f"{name_sym}"
                f"📋 Mintアドレス（タップでコピー）\n"
                f"<code>{mint}</code>\n\n"
                f"⏱ 作成から <b>{age_str}後</b> に検知\n"
                f"💧 時価総額: <b>${market_cap:,.0f}</b>\n\n"
                f"📊 https://dexscreener.com/solana/{mint}\n"
                f"📱 <a href=\"https://pump.fun/{mint}\">pump.fun</a>"
            )
            notified += 1
            print(f"[起動スキャン/pump.fun] ✅ 通知: {mint[:20]} age={age_str}")

        # 最後のページでも対象が1件もなかったらループ継続（混在しているため）
        # 10ページ全チェックして終了
        time.sleep(0.5)   # pump.fun API レート制限

    print(f"[起動スキャン/pump.fun] 完了: チェック{checked}件 → 通知{notified}件")
    send_telegram(
        f"✅ <b>起動通知スキャン完了（pump.fun補完）</b>\n\n"
        f"🚀 pump.fun直近500件をスキャン\n"
        f"📋 条件適合: {checked}件（20分〜8時間前, アイコンあり, $10k+）\n"
        f"✅ 通知送信: <b>{notified}件</b>"
    )


def _startup_notify_scan():
    """
    起動スキャン完了後に1回だけ実行。
    _pending_tokens の中で既に取引が始まっているトークンを即通知する。

    目的:
      - アイコンフィルターが正常に動作しているか確認できる
      - デプロイ直後に過去8時間分の「今すぐ買える」トークンを把握できる

    フロー:
      1. _pending_tokens から 20分以上経過したトークンを取得
      2. DexScreener で 30件ずつ一括チェック
      3. 流動性 $10k+ かつ 取引開始が作成から20分以降 → _notify_delayed_launch
         （アイコンフィルターはそこで適用される）
      4. 完了後にTelegramでチェック件数・通知件数を報告
    """
    BATCH_SIZE = 30
    WAIT_SEC   = 20 * 60  # 20分

    # Solana起動スキャンが完了するまで待機（最大10分）
    # time.sleep(10) では間に合わないため、完了イベントで同期する
    print("[起動通知スキャン] Solana起動スキャン完了待機中...")
    _solana_startup_done.wait(timeout=600)
    time.sleep(3)  # 登録の書き込みが落ち着くまで少し待つ
    print("[起動通知スキャン] Solana起動スキャン完了確認 → 通知スキャン開始")

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
        f"（アイコンありのみ通知）"
    )

    keys        = list(targets.keys())
    notified    = 0
    skipped_liq = 0  # 流動性不足でスキップ
    skipped_age = 0  # 早期取引開始でスキップ

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

        # 流動性マップを構築
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
            if not liq:
                skipped_liq += 1
                continue

            info = targets.get(key)
            if not info:
                continue

            # プール作成から20分未満ならスキップ（まだ新鮮すぎる）
            # ※ 起動スキャンは「プール作成TX」を created_at に登録するため
            #   launch_delay = pair_created_at - created_at ≈ 0 となり
            #   旧ロジック（launch_delay < 20分）では全件除外されてしまう。
            #   正しくは「今の時刻からプール作成まで20分以上経過しているか」で判定する。
            dex_data     = dex_map.get(key.lower(), {})
            pair_created = dex_data.get("pair_created_at", 0)
            if pair_created > 0 and (now - pair_created) < WAIT_SEC:
                skipped_age += 1
                with _pending_lock:
                    _pending_tokens.pop(key, None)
                print(f"[起動通知スキャン] プール作成20分未満 → スキップ: {key[:20]}")
                continue

            age = now - info["created_at"]
            # _notify_delayed_launch 内でアイコンフィルターが適用される
            _notify_delayed_launch(key, info["chain"], liq, age, info["source"], dex_data)
            notified += 1

        time.sleep(0.3)  # DexScreenerレート制限

    print(f"[起動通知スキャン] 完了: チェック{total_checked}件 → 通知{notified}件")
    send_telegram(
        f"✅ <b>起動通知スキャン完了（EVM/Raydium）</b>\n\n"
        f"📋 チェック: {total_checked}件\n"
        f"💧 流動性不足でスキップ: {skipped_liq}件\n"
        f"⏱ 早期取引（20分以内）でスキップ: {skipped_age}件\n"
        f"✅ 通知送信: <b>{notified}件</b>\n\n"
        f"🚀 次: pump.fun直近アクティブコインをスキャン中..."
    )

    # ── pump.fun 起動前トークン補完スキャン ─────────────────────────────────
    # pump.funは1日数千件作成されるためRPC遡りは不可。
    # 代わりにpump.fun APIで「最近取引されたコイン」を取得し、
    # 20分〜8時間前に作成 + image_uri あり + $10k+ のものを即通知。
    # これにより起動前に作成されたpump.funトークンの漏れを補完する。
    _startup_pumpfun_notify_scan()


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
        "⚡ Raydium直接監視（AMM V4 / CPMM）\n"
        "🟡 DEX: FourMeme / BSC\n"
        "🟡 DEX: BNB Chain全般（PancakeSwap V2/V3）\n"
        "🔵 DEX: Clanker / Base\n"
        "🔵 DEX: Base全般（Uniswap V3）\n\n"
        f"⚡ 新戦略: プール作成時オンチェーン即時チェック\n"
        f"👛 トップ10保有率 ≤{TOP10_MAX_PCT:.0f}%\n"
        f"🔄 EVM検知5秒毎 / 通知まで平均10〜15秒\n"
        f"⚡ Raydium: プール作成TX直接検知 5〜15秒\n\n"
        "🔍 Solana監視対象：\n"
        "pump.fun / Raydium / rapidlaunch.io\n"
        "moonshot / letsbonk / その他全Solana launchpad"
    )

    # ── 起動時スキャン（デプロイ直後に過去20分〜8時間のトークンを登録）──────
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

    # ── 起動通知スキャン（既に取引済みのトークンを即通知・アイコンフィルター確認）──
    # Solanaバックグラウンドスキャンの登録を待ちながら並行して実行
    threading.Thread(target=_startup_notify_scan, daemon=True).start()
    print("[起動通知スキャン] バックグラウンドスレッド起動完了")

    # Pump.fun監視を独立バックグラウンドスレッドで起動（TX上限なし・10秒ごと）
    t_pumpfun = threading.Thread(target=pumpfun_monitor_loop, daemon=True)
    t_pumpfun.start()
    print("[Pump.fun] バックグラウンドスレッド起動完了")

    # Solana全般監視を独立バックグラウンドスレッドで起動
    t_all_solana = threading.Thread(target=solana_all_monitor_loop, daemon=True)
    t_all_solana.start()
    print("[Solana全般] バックグラウンドスレッド起動完了")

    # Raydium直接監視を独立バックグラウンドスレッドで起動（AMM V4 + CPMM）
    t_raydium = threading.Thread(target=raydium_monitor_loop, daemon=True)
    t_raydium.start()
    print("[Raydium] バックグラウンドスレッド起動完了")

    # Orca Whirlpool / Meteora 監視（15秒ごと）
    t_orca_meteora = threading.Thread(target=orca_meteora_monitor_loop, daemon=True)
    t_orca_meteora.start()
    print("[Orca/Meteora] バックグラウンドスレッド起動完了")

    # 遅延ローンチ監視 - DexScreenerフォールバック（Solana・pair_addrなしEVM）
    t_pending = threading.Thread(target=pending_watch_loop, daemon=True)
    t_pending.start()
    print("[遅延監視] 遅延ローンチ監視スレッド起動完了")

    # pump.fun定期スキャン（30分ごと・429でgetTransaction失敗した分を救済）
    t_pf_periodic = threading.Thread(target=_periodic_pumpfun_scan_loop, daemon=True)
    t_pf_periodic.start()
    print("[定期スキャン] pump.fun 30分ごとスキャン起動完了")

    # 遅延ローンチ監視 - EVMオンチェーン（pair_addrありEVM → 3秒で即検知）
    t_evm_onchain = threading.Thread(target=evm_pending_onchain_loop, daemon=True)
    t_evm_onchain.start()
    print("[EVM遅延監視] EVMオンチェーン監視スレッド起動完了")

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

        time.sleep(2)  # 30→5→2秒（EVM新規プール検知を高速化）
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
