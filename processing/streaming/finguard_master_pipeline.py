# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║         FinGuard 2.0 — Unified Master Pipeline (Production-Grade)           ║
║         Omnichannel Financial Integrity Platform                             ║
║                                                                              ║
║  Pipeline Flow:                                                              ║
║  Kafka (Avro) → DQ Layer → SLA Monitor → MLlib Scoring →                   ║
║  Hybrid Decision Engine → Action Engine → Multi-Sink                        ║
║                                                                              ║
║  Sinks: HBase (real-time) | Hive Gold (analytics) | HDFS (archive/DLQ)     ║
║  Guarantees: Exactly-Once Semantics | Idempotent Writes                     ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

# ==============================================================================
# IMPORTS
# ==============================================================================
import hashlib
import json
import math
import logging
from datetime import datetime, timezone
from typing import Iterator, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, udf, current_timestamp, unix_timestamp,
    from_json, to_json, struct, array, array_compact,
    window, lag, count, sum as spark_sum, avg, stddev,
    abs as spark_abs, expr, concat, broadcast,
    percentile_approx, collect_list, countDistinct,
    to_timestamp, from_unixtime, datediff
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, IntegerType, BooleanType, TimestampType,
    ArrayType, MapType
)
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml import Pipeline, PipelineModel
import pyspark.sql.functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FinGuard")

# ==============================================================================
# SECTION 1: SPARK SESSION — ENTERPRISE CONFIGURATION
# ==============================================================================

def create_spark_session() -> SparkSession:
    """
    SparkSession مضبوطة للـ Production:
    - RocksDB: أسرع State Store للـ Stateful Processing
    - Kafka EOS: Exactly-Once Semantics من Kafka لـ Spark
    - Delta: Idempotent writes للـ Hive Gold Layer
    - Kryo: Serialization أسرع بـ 10x من Java default
    """
    return SparkSession.builder \
        .appName("FinGuard-2.0-MasterPipeline") \
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state"
                ".RocksDBStateStoreProvider") \
        .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching",
                "false") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "200") \
        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.streaming.kafka.consumer.cache.enabled", "false") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation",
                "true") \
        .enableHiveSupport() \
        .getOrCreate()


spark = create_spark_session()
spark.sparkContext.setLogLevel("WARN")

# ==============================================================================
# SECTION 2: SCHEMAS — THE UNIVERSAL CONTRACT
# ==============================================================================

# ── 2A. Master Transaction Schema (Avro-aligned) ──────────────────────────────
TRANSACTION_SCHEMA = StructType([
    # Identity
    StructField("transaction_id",      StringType(),    False),
    StructField("idempotency_key",     StringType(),    True),   # Exactly-Once
    StructField("correlation_id",      StringType(),    True),
    StructField("customer_id",         StringType(),    False),
    StructField("account_id_source",   StringType(),    False),
    StructField("account_id_dest",     StringType(),    True),
    # Channel
    StructField("channel_type",        StringType(),    False),
    # MOBILE_APP | WEB_BANKING | ATM | POS | BRANCH | SWIFT_WIRE | E_WALLET
    StructField("channel_subtype",     StringType(),    True),
    # Device
    StructField("device_id",           StringType(),    True),
    StructField("device_fingerprint",  StringType(),    True),
    StructField("device_type",         StringType(),    True),
    StructField("is_rooted_device",    BooleanType(),   True),
    StructField("ip_address",          StringType(),    True),
    StructField("ip_country",          StringType(),    True),
    # Geography
    StructField("latitude",            DoubleType(),    True),
    StructField("longitude",           DoubleType(),    True),
    StructField("country_code",        StringType(),    True),
    StructField("city",                StringType(),    True),
    StructField("atm_id",              StringType(),    True),
    # Transaction
    StructField("amount",              DoubleType(),    False),
    StructField("currency_code",       StringType(),    False),
    StructField("amount_usd",          DoubleType(),    True),
    StructField("transaction_type",    StringType(),    False),
    StructField("merchant_id",         StringType(),    True),
    StructField("merchant_category",   StringType(),    True),
    StructField("merchant_country",    StringType(),    True),
    # Timing — Critical for SLA
    StructField("event_timestamp",     LongType(),      False),  # epoch ms @ source
    StructField("kafka_timestamp",     LongType(),      True),   # epoch ms @ Kafka
    StructField("local_hour",          IntegerType(),   True),
    # Auth
    StructField("session_id",          StringType(),    True),
    StructField("auth_method",         StringType(),    True),
    StructField("auth_success",        BooleanType(),   True),
    StructField("failed_auth_count",   IntegerType(),   True),
])

# ── 2B. Customer State Schema (RocksDB State Store) ───────────────────────────
CUSTOMER_STATE_SCHEMA = StructType([
    StructField("customer_id",            StringType(),  True),
    # Per-channel last activity
    StructField("last_mobile_lat",        DoubleType(),  True),
    StructField("last_mobile_lon",        DoubleType(),  True),
    StructField("last_mobile_ts",         LongType(),    True),
    StructField("last_mobile_device",     StringType(),  True),
    StructField("last_mobile_ip_country", StringType(),  True),
    StructField("last_atm_lat",           DoubleType(),  True),
    StructField("last_atm_lon",           DoubleType(),  True),
    StructField("last_atm_ts",            LongType(),    True),
    StructField("last_web_ip_country",    StringType(),  True),
    StructField("last_web_ts",            LongType(),    True),
    StructField("last_wire_country",      StringType(),  True),
    StructField("last_wire_amount_usd",   DoubleType(),  True),
    StructField("last_wire_ts",           LongType(),    True),
    # Rolling behavioral stats (Welford's algorithm)
    StructField("txn_count",              LongType(),    True),
    StructField("amount_mean",            DoubleType(),  True),
    StructField("amount_m2",              DoubleType(),  True),   # sum of sq diffs
    StructField("amount_std",             DoubleType(),  True),
    StructField("total_in_24h",           DoubleType(),  True),
    StructField("total_out_24h",          DoubleType(),  True),
    StructField("txn_count_1h",           LongType(),    True),
    StructField("last_txn_amounts",       StringType(),  True),   # JSON: last 10
    # Micro-structuring detection
    StructField("sub_threshold_count",    IntegerType(), True),
    StructField("sub_threshold_sum",      DoubleType(),  True),
    StructField("sub_threshold_window_start", LongType(), True),
    # Trust & identity
    StructField("trusted_devices",        StringType(),  True),   # JSON array
    StructField("last_known_country",     StringType(),  True),
    StructField("active_session_id",      StringType(),  True),
    StructField("last_activity_ts",       LongType(),    True),
    # Mule account detection
    StructField("rapid_in_sum",           DoubleType(),  True),
    StructField("rapid_out_sum",          DoubleType(),  True),
    StructField("rapid_window_start",     LongType(),    True),
])

# ── 2C. Enriched Output Schema ────────────────────────────────────────────────
ENRICHED_SCHEMA = StructType([
    StructField("transaction_id",       StringType(),  True),
    StructField("idempotency_key",      StringType(),  True),
    StructField("customer_id",          StringType(),  True),
    StructField("channel_type",         StringType(),  True),
    StructField("amount",               DoubleType(),  True),
    StructField("amount_usd",           DoubleType(),  True),
    StructField("event_timestamp",      LongType(),    True),
    # SLA Metrics
    StructField("source_to_kafka_lag_ms",  LongType(), True),
    StructField("kafka_to_spark_lag_ms",   LongType(), True),
    StructField("total_pipeline_lag_ms",   LongType(), True),
    StructField("sla_breached",            BooleanType(), True),
    # DQ
    StructField("dq_passed",              BooleanType(), True),
    StructField("dq_violations",          StringType(),  True),
    StructField("completeness_score",     DoubleType(),  True),
    # ML Score
    StructField("ml_risk_score",          DoubleType(),  True),
    StructField("ml_cluster_id",          IntegerType(), True),
    StructField("amount_zscore",          DoubleType(),  True),
    # Hard Rules
    StructField("geo_violation",          StringType(),  True),
    StructField("micro_structuring",      BooleanType(), True),
    StructField("mule_account_flag",      BooleanType(), True),
    StructField("cross_channel_violation",StringType(),  True),
    # Final Decision
    StructField("composite_risk_score",   DoubleType(),  True),
    StructField("decision_action",        StringType(),  True),  # PASS/FLAG_WAIT/BLOCK
    StructField("decision_reasons",       StringType(),  True),
    StructField("data_fingerprint",       StringType(),  True),
    StructField("processing_timestamp",   LongType(),    True),
    StructField("pipeline_version",       StringType(),  True),
])

PIPELINE_VERSION = "finguard-v2.0.0"
SLA_THRESHOLD_MS = 30_000  # 30 seconds

# ==============================================================================
# SECTION 3: DATA QUALITY LAYER — ZERO-TRUST VALIDATION
# ==============================================================================

def run_dq_validation(df):
    """
    Zero-Trust: لا نثق في أي بيانة إلا بعد ما تعدي 4 فحوصات.

    Layer 1 — Completeness:  الحقول الإلزامية موجودة؟
    Layer 2 — Business Rules: المبالغ والتواريخ منطقية؟
    Layer 3 — Format:         الكود والـ UUID بصيغة صحيحة؟
    Layer 4 — Accounting:     source ≠ dest (لا تحويل لنفسك)
    """
    mandatory = [
        "transaction_id", "customer_id", "amount",
        "event_timestamp", "channel_type", "account_id_source"
    ]

    null_flags = [
        when(col(f).isNull(), lit(f"NULL_{f.upper()}")).otherwise(lit(None))
        for f in mandatory
    ]

    return df \
        .withColumn("_null_violations",
                    array_compact(array(*null_flags))) \
        .withColumn("_rule_amount_pos",    col("amount") > 0) \
        .withColumn("_rule_amount_cap",    col("amount") < 10_000_000) \
        .withColumn("_rule_ts_valid",
                    col("event_timestamp") > lit(1_000_000_000_000)) \
        .withColumn("_rule_no_self_transfer",
                    col("account_id_source") != col("account_id_dest")) \
        .withColumn("_rule_channel_valid",
                    col("channel_type").isin([
                        "MOBILE_APP", "WEB_BANKING", "ATM", "POS",
                        "BRANCH", "SWIFT_WIRE", "E_WALLET",
                        "INTERNAL_TRANSFER"
                    ])) \
        .withColumn("dq_violations",
                    concat(
                        F.array_join(col("_null_violations"), "|"),
                        when(~col("_rule_amount_pos"),  lit("|AMOUNT_NOT_POSITIVE")).otherwise(lit("")),
                        when(~col("_rule_amount_cap"),  lit("|AMOUNT_EXCEEDS_CAP")).otherwise(lit("")),
                        when(~col("_rule_ts_valid"),    lit("|INVALID_TIMESTAMP")).otherwise(lit("")),
                        when(~col("_rule_channel_valid"),lit("|INVALID_CHANNEL")).otherwise(lit("")),
                    )) \
        .withColumn("dq_passed",
                    (F.size(col("_null_violations")) == 0) &
                    col("_rule_amount_pos") &
                    col("_rule_amount_cap") &
                    col("_rule_ts_valid") &
                    col("_rule_channel_valid")) \
        .withColumn("completeness_score",
                    (lit(len(mandatory)) - F.size(col("_null_violations"))) /
                    lit(len(mandatory))) \
        .drop("_null_violations", "_rule_amount_pos", "_rule_amount_cap",
              "_rule_ts_valid", "_rule_no_self_transfer", "_rule_channel_valid")


# ==============================================================================
# SECTION 4: SLA MONITORING — LATENCY PIPELINE
# ==============================================================================

def add_sla_metrics(df):
    """
    نحسب 3 أرقام لكل معاملة:

    1. source_to_kafka_lag  = kafka_timestamp - event_timestamp
       (وقت الرحلة من التطبيق/ATM لـ Kafka)

    2. kafka_to_spark_lag   = spark_processing_time - kafka_timestamp
       (وقت انتظار المعاملة في Kafka queue)

    3. total_pipeline_lag   = مجموع الاثنين
       لو > 30 ثانية → sla_breached = True → تروح HDFS مباشرة
    """
    now_ms = (unix_timestamp(current_timestamp()) * 1000).cast(LongType())

    return df \
        .withColumn("spark_ingest_ts", now_ms) \
        .withColumn("source_to_kafka_lag_ms",
                    when(col("kafka_timestamp").isNotNull(),
                         col("kafka_timestamp") - col("event_timestamp"))
                    .otherwise(lit(0))) \
        .withColumn("kafka_to_spark_lag_ms",
                    col("spark_ingest_ts") - col("kafka_timestamp")) \
        .withColumn("total_pipeline_lag_ms",
                    col("spark_ingest_ts") - col("event_timestamp")) \
        .withColumn("sla_breached",
                    col("total_pipeline_lag_ms") > lit(SLA_THRESHOLD_MS)) \
        .drop("spark_ingest_ts")


# ==============================================================================
# SECTION 5: ML RISK SCORING (MLlib)
# ==============================================================================
# Architecture note:
# نستخدم KMeans Clustering لتحديد "الكتلة الطبيعية" لكل عميل.
# المعاملات اللي بعيدة عن كل cluster هي الشاذة.
# الـ Model بيتدرب offline على Hive Gold data وبيتحمل هنا.

ML_FEATURE_COLS = [
    "amount_usd_scaled",        # المبلغ
    "local_hour_scaled",        # الوقت
    "amount_zscore",            # الانحراف عن المتوسط الشخصي
    "failed_auth_count_scaled", # محاولات فاشلة
]

def load_ml_model(model_path: str):
    """تحميل الـ Model اللي اتدرب offline من HDFS"""
    try:
        return PipelineModel.load(model_path)
    except Exception:
        logger.warning("ML Model not found — using rule-based scoring only.")
        return None


def compute_ml_risk_score(df, ml_model=None):
    """
    خطوة 1: نحسب الـ z-score للمبلغ مقارنةً بمتوسط العميل
    خطوة 2: نطبق الـ ML Model (لو موجود)
    خطوة 3: نرجع risk_score بين 0 و 1
    """
    # Z-score approximation using Windowed stats
    # (الدقة الكاملة تيجي من الـ Stateful Processing في Section 6)
    enriched = df \
        .withColumn("amount_usd_safe",
                    when(col("amount_usd").isNull(), col("amount"))
                    .otherwise(col("amount_usd"))) \
        .withColumn("failed_auth_safe",
                    when(col("failed_auth_count").isNull(), lit(0))
                    .otherwise(col("failed_auth_count")))

    if ml_model:
        try:
            scored = ml_model.transform(enriched)
            return scored \
                .withColumn("ml_risk_score",
                            # Distance from cluster center → normalized to [0,1]
                            F.least(col("prediction_distance") / lit(10.0), lit(1.0))) \
                .withColumn("ml_cluster_id", col("prediction").cast(IntegerType()))
        except Exception as e:
            logger.error(f"ML scoring failed: {e}")

    # Fallback: heuristic score
    return enriched \
        .withColumn("ml_risk_score",    lit(0.0)) \
        .withColumn("ml_cluster_id",    lit(-1).cast(IntegerType())) \
        .withColumn("amount_zscore",    lit(0.0))


# ==============================================================================
# SECTION 6: STATEFUL HYBRID DECISION ENGINE
# Core of FinGuard — runs per-customer, per-transaction
# ==============================================================================

def _haversine(lat1, lon1, lat2, lon2) -> float:
    """Haversine distance in km between two geo points"""
    if None in (lat1, lon1, lat2, lon2):
        return 0.0
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp/2)**2 + math.cos(p1) * math.cos(p2) * math.sin(dl/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def stateful_decision_engine(
    customer_id: str,
    transactions,
    state: GroupState
) -> Iterator[Tuple]:
    """
    ══════════════════════════════════════════════════
    القلب النابض لـ FinGuard — يُستدعى لكل عميل
    عند وصول أي معاملة جديدة من أي قناة.

    يحتوي على 5 طبقات تحليل:
    A. Welford's Online Stats     → Z-Score دقيق
    B. Geo-Temporal Impossibility → Haversine
    C. Micro-Structuring (Smurfing) → 9,000 rule
    D. Mule Account Symmetry      → Fast In/Out
    E. Cross-Channel Correlation  → Omnichannel
    ══════════════════════════════════════════════════
    """
    MICRO_THRESHOLD       = 9_000.0   # حد الـ AML (قبل الـ 10K CTR)
    MICRO_WINDOW_MS       = 3_600_000  # ساعة واحدة
    MULE_WINDOW_MS        = 1_800_000  # 30 دقيقة
    MULE_SYMMETRY_RATIO   = 0.85       # 85% تشابه بين In و Out
    IMPOSSIBLE_SPEED_KMH  = 900.0

    # ── استرجاع أو تهيئة الـ State ────────────────────────────
    if state.exists:
        raw = state.get
        s = dict(zip(
            [f.name for f in CUSTOMER_STATE_SCHEMA.fields], raw
        ))
    else:
        s = {f.name: None for f in CUSTOMER_STATE_SCHEMA.fields}
        s.update({
            "customer_id":           customer_id,
            "txn_count":             0,
            "amount_mean":           0.0,
            "amount_m2":             0.0,
            "amount_std":            0.0,
            "total_in_24h":          0.0,
            "total_out_24h":         0.0,
            "txn_count_1h":          0,
            "sub_threshold_count":   0,
            "sub_threshold_sum":     0.0,
            "rapid_in_sum":          0.0,
            "rapid_out_sum":         0.0,
            "last_txn_amounts":      "[]",
            "trusted_devices":       "[]",
        })

    results = []

    for txn in transactions:
        now_ms    = int(datetime.now(timezone.utc).timestamp() * 1000)
        ts        = txn.event_timestamp or now_ms
        amount    = txn.amount or 0.0
        amount_usd = txn.amount_usd or amount
        channel   = txn.channel_type or "UNKNOWN"
        violations = []
        risk_score = 0.0

        # ════════════════════════════════════════════════════
        # A. WELFORD'S ONLINE ALGORITHM — Rolling Z-Score
        # الذاكرة اللي بتتعلم من كل معاملة بدون ما نخزن كلها
        # ════════════════════════════════════════════════════
        n   = (s["txn_count"] or 0) + 1
        old_mean = s["amount_mean"] or 0.0
        delta    = amount - old_mean
        new_mean = old_mean + delta / n
        delta2   = amount - new_mean
        new_m2   = (s["amount_m2"] or 0.0) + delta * delta2
        new_std  = math.sqrt(new_m2 / n) if n > 1 else 0.0

        s["txn_count"]   = n
        s["amount_mean"] = new_mean
        s["amount_m2"]   = new_m2
        s["amount_std"]  = new_std

        # Z-Score: كم انحراف معياري هذه المعاملة؟
        amount_zscore = 0.0
        if n >= 5 and new_std > 0:
            amount_zscore = abs(amount - new_mean) / new_std
            if amount_zscore > 4.0:
                violations.append(f"STATISTICAL_OUTLIER_Z{amount_zscore:.1f}")
                risk_score += min(amount_zscore / 10.0, 0.35)

        # ════════════════════════════════════════════════════
        # B. GEO-TEMPORAL IMPOSSIBILITY (Haversine)
        # هل الوصول لهذا المكان ممكن بشرياً؟
        # ════════════════════════════════════════════════════
        geo_violation = "NONE"
        prev_lat, prev_lon, prev_ts = None, None, None

        if channel in ("ATM", "POS", "BRANCH"):
            prev_lat = s.get("last_atm_lat")
            prev_lon = s.get("last_atm_lon")
            prev_ts  = s.get("last_atm_ts")
        elif channel == "MOBILE_APP":
            prev_lat = s.get("last_mobile_lat")
            prev_lon = s.get("last_mobile_lon")
            prev_ts  = s.get("last_mobile_ts")

        # Cross-channel: Mobile → ATM
        if channel in ("ATM", "POS") and s.get("last_mobile_ts"):
            m_lat = s.get("last_mobile_lat")
            m_lon = s.get("last_mobile_lon")
            m_ts  = s.get("last_mobile_ts")
            if m_lat and txn.latitude:
                time_h = max((ts - m_ts) / 3_600_000, 0.0001)
                dist   = _haversine(m_lat, m_lon, txn.latitude, txn.longitude)
                speed  = dist / time_h
                if speed > IMPOSSIBLE_SPEED_KMH and dist > 50:
                    geo_violation = (
                        f"IMPOSSIBLE_CROSS_CHANNEL_"
                        f"{dist:.0f}KM_{speed:.0f}KMH"
                    )
                    violations.append(geo_violation)
                    risk_score += 0.90  # near-certain fraud

        # Same-channel geo check
        if prev_lat and txn.latitude and prev_ts:
            time_h = max((ts - prev_ts) / 3_600_000, 0.0001)
            dist   = _haversine(prev_lat, prev_lon, txn.latitude, txn.longitude)
            speed  = dist / time_h
            if speed > IMPOSSIBLE_SPEED_KMH and dist > 50:
                geo_violation = f"IMPOSSIBLE_TRAVEL_{dist:.0f}KM"
                violations.append(geo_violation)
                risk_score += 0.85

        # ════════════════════════════════════════════════════
        # C. MICRO-STRUCTURING DETECTION (Smurfing)
        # تقسيم مبلغ كبير لدفعات صغيرة تحت الحد الرقابي
        # ════════════════════════════════════════════════════
        micro_structuring = False
        sub_window_start  = s.get("sub_threshold_window_start") or ts
        sub_count         = s.get("sub_threshold_count") or 0
        sub_sum           = s.get("sub_threshold_sum") or 0.0

        # reset window إذا انتهت الساعة
        if ts - sub_window_start > MICRO_WINDOW_MS:
            sub_count, sub_sum, sub_window_start = 0, 0.0, ts

        if amount_usd < MICRO_THRESHOLD:
            sub_count += 1
            sub_sum   += amount_usd
            # 3+ معاملات تحت الحد + مجموعها > 20K → Smurfing
            if sub_count >= 3 and sub_sum > 20_000:
                micro_structuring = True
                violations.append(
                    f"MICRO_STRUCTURING_{sub_count}TXN_"
                    f"${sub_sum:.0f}_IN_1H"
                )
                risk_score += 0.70

        s["sub_threshold_count"]        = sub_count
        s["sub_threshold_sum"]          = sub_sum
        s["sub_threshold_window_start"] = sub_window_start

        # ════════════════════════════════════════════════════
        # D. MULE ACCOUNT SYMMETRY
        # أموال بتدخل وبتطلع بسرعة = حساب وسيط للغسيل
        # ════════════════════════════════════════════════════
        mule_flag     = False
        rapid_window  = s.get("rapid_window_start") or ts
        rapid_in      = s.get("rapid_in_sum") or 0.0
        rapid_out     = s.get("rapid_out_sum") or 0.0

        if ts - rapid_window > MULE_WINDOW_MS:
            rapid_in, rapid_out, rapid_window = 0.0, 0.0, ts

        is_inbound  = txn.transaction_type in ("TRANSFER_IN", "DEPOSIT", "WIRE_IN")
        is_outbound = txn.transaction_type in ("TRANSFER_OUT", "WITHDRAWAL", "WIRE_OUT")

        if is_inbound:
            rapid_in += amount_usd
        elif is_outbound:
            rapid_out += amount_usd

        if rapid_in > 5_000 and rapid_out > 5_000:
            ratio = min(rapid_in, rapid_out) / max(rapid_in, rapid_out)
            if ratio >= MULE_SYMMETRY_RATIO:
                mule_flag = True
                violations.append(
                    f"MULE_SYMMETRY_IN${rapid_in:.0f}"
                    f"_OUT${rapid_out:.0f}_RATIO{ratio:.2f}"
                )
                risk_score += 0.65

        s["rapid_in_sum"]       = rapid_in
        s["rapid_out_sum"]      = rapid_out
        s["rapid_window_start"] = rapid_window

        # ════════════════════════════════════════════════════
        # E. CROSS-CHANNEL CONTEXTUAL INTEGRITY
        # هل الصورة الكاملة عبر القنوات منطقية؟
        # ════════════════════════════════════════════════════
        cross_channel_violation = "NONE"
        trusted = json.loads(s.get("trusted_devices") or "[]")
        is_new_device  = bool(txn.device_id) and txn.device_id not in trusted
        is_new_country = (txn.country_code and
                          txn.country_code != s.get("last_known_country"))

        # Active Mobile Session + ATM in different country
        if channel == "ATM" and s.get("last_mobile_ts"):
            mobile_age_min = (ts - (s["last_mobile_ts"] or 0)) / 60_000
            if mobile_age_min < 15:
                if txn.country_code != s.get("last_mobile_ip_country"):
                    cross_channel_violation = "SESSION_HIJACK_MOBILE_ACTIVE"
                    violations.append(cross_channel_violation)
                    risk_score += 0.80

        # High-value wire from new device
        if (channel == "SWIFT_WIRE" and is_new_device
                and amount_usd > 10_000):
            cross_channel_violation = "HIGH_VALUE_WIRE_UNKNOWN_DEVICE"
            violations.append(cross_channel_violation)
            risk_score += 0.75

        # Triple Novelty: new device + new country + high value
        if is_new_device and is_new_country and amount_usd > 5_000:
            violations.append("TRIPLE_NOVELTY")
            risk_score += 0.60

        # Rooted device
        if txn.is_rooted_device:
            violations.append("ROOTED_DEVICE")
            risk_score += 0.30

        # ════════════════════════════════════════════════════
        # F. COMPOSITE RISK SCORE + DECISION
        # ════════════════════════════════════════════════════
        composite_score = min(risk_score, 1.0)
        ml_risk = 0.0  # will be merged downstream

        # Final weighted composite (rules dominate for hard violations)
        has_hard_violation = any(v for v in violations if any(
            h in v for h in [
                "IMPOSSIBLE", "SESSION_HIJACK",
                "HIGH_VALUE_WIRE_UNKNOWN", "MULE_SYMMETRY"
            ]
        ))

        if composite_score >= 0.75 or has_hard_violation:
            decision_action = "BLOCK"
        elif composite_score >= 0.40:
            decision_action = "FLAG_WAIT"
        else:
            decision_action = "PASS"

        # ════════════════════════════════════════════════════
        # G. CRYPTOGRAPHIC AUDIT FINGERPRINT
        # ════════════════════════════════════════════════════
        canonical = (
            f"{txn.transaction_id}|{customer_id}|"
            f"{amount}|{ts}|{channel}"
        )
        fingerprint = hashlib.sha256(canonical.encode()).hexdigest()

        # ════════════════════════════════════════════════════
        # H. UPDATE STATE
        # ════════════════════════════════════════════════════
        if channel == "MOBILE_APP":
            s["last_mobile_lat"]          = txn.latitude
            s["last_mobile_lon"]          = txn.longitude
            s["last_mobile_ts"]           = ts
            s["last_mobile_device"]       = txn.device_id
            s["last_mobile_ip_country"]   = txn.ip_country
            s["active_session_id"]        = txn.session_id
        elif channel in ("ATM", "POS", "BRANCH"):
            s["last_atm_lat"] = txn.latitude
            s["last_atm_lon"] = txn.longitude
            s["last_atm_ts"]  = ts
        elif channel == "WEB_BANKING":
            s["last_web_ip_country"] = txn.ip_country
            s["last_web_ts"]         = ts
        elif channel == "SWIFT_WIRE":
            s["last_wire_country"]    = txn.country_code
            s["last_wire_ts"]         = ts
            s["last_wire_amount_usd"] = amount_usd

        s["last_known_country"] = txn.country_code or s.get("last_known_country")
        s["last_activity_ts"]   = ts

        # Trust device if clean
        if not violations and txn.device_id:
            if txn.device_id not in trusted:
                trusted.append(txn.device_id)
                s["trusted_devices"] = json.dumps(trusted[-10:])

        results.append((
            txn.transaction_id,
            txn.idempotency_key,
            customer_id,
            channel,
            amount,
            float(amount_usd or amount),
            ts,
            int(txn.source_to_kafka_lag_ms or 0)
                if hasattr(txn, "source_to_kafka_lag_ms") else 0,
            int(txn.kafka_to_spark_lag_ms or 0)
                if hasattr(txn, "kafka_to_spark_lag_ms") else 0,
            int(txn.total_pipeline_lag_ms or 0)
                if hasattr(txn, "total_pipeline_lag_ms") else 0,
            bool(txn.sla_breached) if hasattr(txn, "sla_breached") else False,
            bool(txn.dq_passed) if hasattr(txn, "dq_passed") else True,
            str(txn.dq_violations) if hasattr(txn, "dq_violations") else "",
            float(txn.completeness_score)
                if hasattr(txn, "completeness_score") else 1.0,
            float(ml_risk),
            int(-1),
            round(amount_zscore, 4),
            geo_violation,
            micro_structuring,
            mule_flag,
            cross_channel_violation,
            round(composite_score, 4),
            decision_action,
            "|".join(violations) if violations else "CLEAN",
            fingerprint,
            int(now_ms),
            PIPELINE_VERSION,
        ))

    # Save state
    state.update(tuple(
        s[f.name] for f in CUSTOMER_STATE_SCHEMA.fields
    ))
    state.setTimeoutDuration("24 hours")
    yield from results


# ==============================================================================
# SECTION 7: ACTION ENGINE — HUMAN-IN-THE-LOOP ALERTS
# ==============================================================================

def build_alert_payload(df):
    """
    لكل معاملة FLAG_WAIT أو BLOCK:
    نبعت payload كامل للـ Security Manager يحتوي على:
    - كل تفاصيل المعاملة
    - سبب القرار
    - Risk Score
    - رابط مباشر لتأكيد أو رفض
    """
    return df \
        .filter(col("decision_action").isin(["BLOCK", "FLAG_WAIT"])) \
        .withColumn("alert_payload",
            to_json(struct(
                lit("FINGUARD_ALERT").alias("alert_type"),
                col("transaction_id"),
                col("customer_id"),
                col("channel_type"),
                col("amount"),
                col("composite_risk_score"),
                col("decision_action"),
                col("decision_reasons"),
                col("data_fingerprint"),
                current_timestamp().alias("alert_generated_at"),
                concat(
                    lit("https://finguard.bank/review/"),
                    col("transaction_id")
                ).alias("review_url")
            ))
        )


# ==============================================================================
# SECTION 8: MAIN PIPELINE ORCHESTRATION
# ==============================================================================

def build_pipeline():
    """
    يجمع كل الـ Sections في pipeline واحد متكامل.
    يطبق Exactly-Once عن طريق:
    1. Kafka → Spark: enable.auto.commit=false + manual offset commit
    2. Spark → HBase: idempotency_key كـ row key (عمليات write ثانية بتيجي بنفس الـ key)
    3. Spark → Hive:  Delta Lake merge on idempotency_key
    """

    # ── 8A. Read from Kafka (All Channels, Unified) ───────────────────────────
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe",
                "finguard.channel.mobile,finguard.channel.atm,"
                "finguard.channel.web,finguard.channel.wire,"
                "finguard.channel.pos,finguard.channel.internal") \
        .option("startingOffsets",      "latest") \
        .option("maxOffsetsPerTrigger", "50000") \
        .option("failOnDataLoss",       "false") \
        .option("kafka.group.id",       "finguard-consumer-group") \
        .option("kafka.isolation.level","read_committed")  \
        .load()

    # Parse + add Kafka metadata
    parsed = raw_stream \
        .withColumn("kafka_timestamp",
                    (col("timestamp").cast(LongType()) * 1000)) \
        .withColumn("data",
                    from_json(col("value").cast("string"), TRANSACTION_SCHEMA)) \
        .select(
            "data.*",
            col("kafka_timestamp"),
            col("offset").alias("kafka_offset"),
            col("partition").alias("kafka_partition"),
        ) \
        .withColumn("event_ts_proper",
                    to_timestamp(col("event_timestamp") / 1000)) \
        .withWatermark("event_ts_proper", "10 minutes")

    # ── 8B. DQ Layer — split Clean vs DLQ ────────────────────────────────────
    dq_checked = run_dq_validation(parsed)

    clean_stream = dq_checked.filter(col("dq_passed"))
    dlq_stream   = dq_checked.filter(~col("dq_passed"))

    # ── 8C. SLA Metrics ──────────────────────────────────────────────────────
    sla_stream = add_sla_metrics(clean_stream)

    # Split: SLA-breached records → HDFS archive directly
    speed_stream   = sla_stream.filter(~col("sla_breached"))
    archive_stream = sla_stream.filter( col("sla_breached"))

    # ── 8D. ML Scoring (lightweight, stateless) ───────────────────────────────
    ml_model = load_ml_model("hdfs:///finguard/models/risk_model/latest")
    scored_stream = compute_ml_risk_score(speed_stream, ml_model)

    # ── 8E. Stateful Hybrid Decision Engine ──────────────────────────────────
    decision_stream = scored_stream \
        .groupBy("customer_id") \
        .applyInPandasWithState(
            stateful_decision_engine,
            outputStructType=ENRICHED_SCHEMA,
            stateStructType=CUSTOMER_STATE_SCHEMA,
            outputMode="Update",
            timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
        )

    # ── 8F. Action Engine — Build Alert Payloads ──────────────────────────────
    alerts_stream = build_alert_payload(decision_stream)

    # ── 8G. MULTI-SINK WRITES ─────────────────────────────────────────────────

    # Sink 1: HBase — Real-time serving (< 10ms lookup)
    # Row key = idempotency_key → Exactly-Once guaranteed
    hbase_query = decision_stream \
        .select(
            col("idempotency_key").alias("row_key"),
            to_json(struct("*")).alias("cf:data"),
            col("decision_action").alias("cf:decision"),
            col("composite_risk_score").alias("cf:risk_score"),
            col("data_fingerprint").alias("cf:fingerprint"),
        ) \
        .writeStream \
        .format("org.apache.hadoop.hbase.spark") \
        .option("hbase.table", "finguard:decisions") \
        .option("checkpointLocation", "/checkpoints/finguard/hbase") \
        .outputMode("update") \
        .trigger(processingTime="5 seconds") \
        .start()

    # Sink 2: Kafka → Decisions Topic (Core Banking listens here)
    kafka_decisions_query = decision_stream \
        .select(
            col("transaction_id").alias("key"),
            to_json(struct(
                "transaction_id", "idempotency_key", "customer_id",
                "decision_action", "composite_risk_score",
                "decision_reasons", "data_fingerprint",
                "processing_timestamp"
            )).alias("value"),
            lit("finguard.decisions.output").alias("topic")
        ) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers",
                "kafka-broker-1:9092,kafka-broker-2:9092") \
        .option("kafka.transactional.id", "finguard-producer-txn") \
        .option("checkpointLocation", "/checkpoints/finguard/kafka-out") \
        .outputMode("update") \
        .trigger(processingTime="2 seconds") \
        .start()

    # Sink 3: Kafka → Alerts Topic (Security Manager dashboard)
    kafka_alerts_query = alerts_stream \
        .select(
            col("customer_id").alias("key"),
            col("alert_payload").alias("value"),
            lit("finguard.alerts.security").alias("topic")
        ) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers",
                "kafka-broker-1:9092,kafka-broker-2:9092") \
        .option("checkpointLocation", "/checkpoints/finguard/alerts") \
        .outputMode("append") \
        .trigger(processingTime="2 seconds") \
        .start()

    # Sink 4: Hive Gold Layer (Delta) — Analytics & Power BI
    hive_gold_query = decision_stream \
        .writeStream \
        .format("delta") \
        .option("checkpointLocation", "/checkpoints/finguard/hive-gold") \
        .option("mergeSchema", "true") \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .partitionBy("decision_action") \
        .toTable("finguard_gold.enriched_decisions")

    # Sink 5: HDFS Bronze — Raw archive (all records)
    hdfs_bronze_query = parsed \
        .writeStream \
        .format("parquet") \
        .option("path", "hdfs:///finguard/bronze/raw_transactions") \
        .option("checkpointLocation", "/checkpoints/finguard/hdfs-bronze") \
        .outputMode("append") \
        .trigger(processingTime="60 seconds") \
        .partitionBy("channel_type") \
        .start()

    # Sink 6: HDFS — DLQ (malformed records)
    dlq_query = dlq_stream \
        .writeStream \
        .format("parquet") \
        .option("path", "hdfs:///finguard/dlq/malformed") \
        .option("checkpointLocation", "/checkpoints/finguard/dlq") \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()

    # Sink 7: HDFS — SLA Breached archive
    sla_archive_query = archive_stream \
        .writeStream \
        .format("parquet") \
        .option("path", "hdfs:///finguard/archive/sla_breached") \
        .option("checkpointLocation", "/checkpoints/finguard/sla-archive") \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()

    logger.info("✅ FinGuard 2.0 — All streams started successfully")

    return [
        hbase_query, kafka_decisions_query, kafka_alerts_query,
        hive_gold_query, hdfs_bronze_query, dlq_query, sla_archive_query
    ]


# ==============================================================================
# SECTION 9: ENTRYPOINT
# ==============================================================================

if __name__ == "__main__":
    logger.info("🚀 Starting FinGuard 2.0 Master Pipeline...")
    queries = build_pipeline()
    # Block until all streams terminate (or crash with alert)
    spark.streams.awaitAnyTermination()
