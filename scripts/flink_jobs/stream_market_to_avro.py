import json
import math
from datetime import datetime, timezone
from typing import Optional, List

from pyflink.common import Types, Time
from pyflink.common.typeinfo import TypeInformation
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.functions import KeySelector, RichFlatMapFunction
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy, BulkWriterFactory

# fastavro for Avro OCF writes
import fastavro

# Paths inside containers
AVRO_SCHEMA_PATH = "/opt/flink/app/schemas/market_tick.avsc"

# ---------- Helpers ----------

def parse_ts(ts_str: str) -> int:
    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)

class SymbolKeySelector(KeySelector):
    def get_key(self, value):
        return value['symbol']

# ---------- Stateful TA computation (per symbol) ----------

from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor

class TAEnricher(RichFlatMapFunction):
    def open(self, runtime_context: RuntimeContext):
        # previous values
        self.prev_close_state = runtime_context.get_state(ValueStateDescriptor("prev_close", Types.DOUBLE()))
        self.prev_high_state  = runtime_context.get_state(ValueStateDescriptor("prev_high",  Types.DOUBLE()))
        self.prev_low_state   = runtime_context.get_state(ValueStateDescriptor("prev_low",   Types.DOUBLE()))

        # SMA/Bollinger rolling closes (20)
        self.closes_20_state  = runtime_context.get_list_state(ListStateDescriptor("closes_20", Types.DOUBLE()))

        # EMA/MACD
        self.ema12_state      = runtime_context.get_state(ValueStateDescriptor("ema12", Types.DOUBLE()))
        self.ema26_state      = runtime_context.get_state(ValueStateDescriptor("ema26", Types.DOUBLE()))
        self.macd_sig_state   = runtime_context.get_state(ValueStateDescriptor("macd_signal9", Types.DOUBLE()))

        # RSI(14) Wilder
        self.rsi_avg_gain     = runtime_context.get_state(ValueStateDescriptor("rsi_avg_gain", Types.DOUBLE()))
        self.rsi_avg_loss     = runtime_context.get_state(ValueStateDescriptor("rsi_avg_loss", Types.DOUBLE()))
        self.rsi_inited       = runtime_context.get_state(ValueStateDescriptor("rsi_inited", Types.BOOLEAN()))

        # ATR(14)
        self.atr_state        = runtime_context.get_state(ValueStateDescriptor("atr_14", Types.DOUBLE()))

        # OBV
        self.obv_state        = runtime_context.get_state(ValueStateDescriptor("obv", Types.DOUBLE()))

        # Session VWAP accumulators
        self.vwap_num_state   = runtime_context.get_state(ValueStateDescriptor("vwap_num", Types.DOUBLE()))
        self.vwap_den_state   = runtime_context.get_state(ValueStateDescriptor("vwap_den", Types.DOUBLE()))
        self.vwap_date_state  = runtime_context.get_state(ValueStateDescriptor("vwap_date", Types.STRING()))

        # Cumulative return
        self.cum_ret_state    = runtime_context.get_state(ValueStateDescriptor("cum_return", Types.DOUBLE()))

    def _get(self, state, default=None):
        v = state.value()
        return v if v is not None else default

    def _set(self, state, value):
        state.update(value)

    def _ema(self, prev: Optional[float], x: float, period: int) -> float:
        alpha = 2.0 / (period + 1.0)
        return x if prev is None else (alpha * x + (1 - alpha) * prev)

    def flat_map(self, tick, out):
        # Required fields from producers
        symbol = tick['symbol']
        # normalized earlier to numbers; timestamp is string
        ts_millis = parse_ts(tick['timestamp'])
        open_  = float(tick['open'])
        high   = float(tick['high'])
        low    = float(tick['low'])
        close  = float(tick['close'])
        volume = float(tick.get('volume', 0.0))
        ing_ms = parse_ts(tick['ingested_at']) if isinstance(tick.get('ingested_at'), str) else None

        prev_close = self._get(self.prev_close_state)
        prev_high  = self._get(self.prev_high_state)
        prev_low   = self._get(self.prev_low_state)

        # Log return and cumulative return
        log_ret = None
        cum_ret = self._get(self.cum_ret_state, 0.0)
        if prev_close and prev_close > 0:
            r = close / prev_close
            if r > 0:
                log_ret = math.log(r)
                cum_ret = (1.0 + cum_ret) * r - 1.0
        self._set(self.cum_ret_state, cum_ret)

        # EMA/MACD
        ema12 = self._ema(self._get(self.ema12_state), close, 12)
        ema26 = self._ema(self._get(self.ema26_state), close, 26)
        self._set(self.ema12_state, ema12)
        self._set(self.ema26_state, ema26)

        macd = ema12 - ema26 if (ema12 is not None and ema26 is not None) else None
        macd_sig_prev = self._get(self.macd_sig_state)
        macd_signal9 = self._ema(macd_sig_prev, macd, 9) if macd is not None else None
        if macd_signal9 is not None:
            self._set(self.macd_sig_state, macd_signal9)
        macd_hist = macd - macd_signal9 if (macd is not None and macd_signal9 is not None) else None

        # RSI(14) via Wilder
        rsi = None
        delta = (close - prev_close) if prev_close is not None else None
        avg_gain = self._get(self.rsi_avg_gain)
        avg_loss = self._get(self.rsi_avg_loss)
        inited   = self._get(self.rsi_inited, False)

        if delta is not None:
            gain = max(delta, 0.0)
            loss = max(-delta, 0.0)
            if not inited:
                # bootstrap: gradually converge
                avg_gain = gain if avg_gain is None else (avg_gain * 13 + gain) / 14.0
                avg_loss = loss if avg_loss is None else (avg_loss * 13 + loss) / 14.0
                if avg_gain is not None and avg_loss is not None:
                    self._set(self.rsi_inited, True)
            else:
                avg_gain = (avg_gain * 13 + gain) / 14.0
                avg_loss = (avg_loss * 13 + loss) / 14.0

            self._set(self.rsi_avg_gain, avg_gain)
            self._set(self.rsi_avg_loss, avg_loss)

            if avg_loss is not None:
                if avg_loss == 0 and avg_gain is not None:
                    rsi = 100.0
                elif avg_gain is not None:
                    rs = avg_gain / avg_loss if avg_loss > 0 else float('inf')
                    rsi = 100.0 - (100.0 / (1.0 + rs))

        # ATR(14)
        atr = self._get(self.atr_state)
        if prev_close is not None:
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            atr = tr if atr is None else (atr * 13 + tr) / 14.0
            self._set(self.atr_state, atr)

        # OBV
        obv = self._get(self.obv_state, 0.0)
        if prev_close is not None:
            if close > prev_close:
                obv += volume
            elif close < prev_close:
                obv -= volume
            self._set(self.obv_state, obv)

        # SMA(20) and Bollinger(20, 2σ)
        closes = list(self.closes_20_state.get())
        closes.append(close)
        if len(closes) > 20:
            closes.pop(0)
        self.closes_20_state.update(closes)

        sma_20 = None
        boll_mid = None
        boll_upper = None
        boll_lower = None
        if closes:
            sma_20 = sum(closes) / len(closes)
            if len(closes) >= 2:
                mean = sma_20
                var = sum((c - mean) ** 2 for c in closes) / len(closes)
                std = math.sqrt(var)
                boll_mid = mean
                boll_upper = mean + 2.0 * std
                boll_lower = mean - 2.0 * std

        # Session VWAP (UTC day)
        ses_date = datetime.fromtimestamp(ts_millis / 1000, tz=timezone.utc).date().isoformat()
        vwap_date = self._get(self.vwap_date_state)
        vwap_num  = self._get(self.vwap_num_state, 0.0)
        vwap_den  = self._get(self.vwap_den_state, 0.0)
        if vwap_date != ses_date:
            vwap_num, vwap_den = 0.0, 0.0
        typical = (high + low + close) / 3.0
        vwap_num += typical * volume
        vwap_den += volume
        vwap = (vwap_num / vwap_den) if vwap_den > 0 else None
        self._set(self.vwap_num_state, vwap_num)
        self._set(self.vwap_den_state, vwap_den)
        self._set(self.vwap_date_state, ses_date)

        # persist prevs
        self._set(self.prev_close_state, close)
        self._set(self.prev_high_state, high)
        self._set(self.prev_low_state, low)

        out.collect({
            "symbol": symbol,
            "timestamp": ts_millis,
            "ingested_at": ing_ms,

            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "adj_close": None,
            "volume": volume,

            "sma_20": sma_20,
            "ema_12": ema12,
            "ema_26": ema26,
            "macd": macd,
            "macd_signal_9": macd_signal9,
            "macd_hist": (macd - macd_signal9) if (macd is not None and macd_signal9 is not None) else None,

            "rsi_14": rsi,
            "boll_mid_20": boll_mid,
            "boll_upper_20": boll_upper,
            "boll_lower_20": boll_lower,

            "atr_14": atr,
            "obv": obv,
            "vwap_session": vwap,

            "log_return": log_ret,
            "cum_return": cum_ret
        })

# ---------- Avro Bulk Writer for FileSink (fastavro OCF) ----------

class FastAvroBulkWriter:
    def __init__(self, out_stream, schema_dict):
        self.out = out_stream
        self.schema = schema_dict
        # Create a writer over the stream; fastavro can write to file-like objects
        self.writer = fastavro.writer(self.out, self.schema, [])

    def add_element(self, element: dict):
        # fastavro requires list-like append; we can write one by one via internal method
        # simpler: buffer records and write in batches — but for stream we can emit per add
        # fastavro doesn't expose append-one easily; workaround: maintain our own buffer
        # To keep it efficient, write as a dict list of size 1 through writer
        fastavro.writer(self.out, self.schema, [element])

    def flush(self):
        self.out.flush()

    def finish(self):
        try:
            self.out.flush()
        except Exception:
            pass

class FastAvroBulkWriterFactory(BulkWriterFactory):
    def __init__(self, schema_path: str):
        with open(schema_path, "r") as f:
            self.schema = json.load(f)

    def create(self, out_stream):
        return FastAvroBulkWriter(out_stream, self.schema)

# ---------- Build job ----------

def build_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    # Watermarks: 10s OOO
    wm = (WatermarkStrategy
          .for_bounded_out_of_orderness(Time.seconds(10))
          .with_timestamp_assigner(lambda e, ts: parse_ts(e['timestamp']) if isinstance(e['timestamp'], str) else e['timestamp']))

    props = {
        'bootstrap.servers': 'redpanda:9092',
        'group.id': 'flink_market_enricher',
        'auto.offset.reset': 'latest'
    }

    consumer = FlinkKafkaConsumer(
        topics=['crypto_ticks', 'equities_ticks'],
        deserialization_schema=Types.STRING().__to_java__(),  # use SimpleStringSchema via implicit
        properties=props
    )

    # In PyFlink 1.17, prefer:
    from pyflink.common.serialization import SimpleStringSchema
    consumer = FlinkKafkaConsumer(
        topics=['crypto_ticks', 'equities_ticks'],
        deserialization_schema=SimpleStringSchema(),
        properties=props
    )

    raw = env.add_source(consumer).name("kafka-source")

    # Parse JSON
    parsed = raw.map(lambda s: json.loads(s), output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY())).name("json-parse")

    # Normalize numeric types into Python floats
    def normalize(d):
        out = dict(d)
        for k in ['open', 'high', 'low', 'close', 'volume']:
            v = out.get(k)
            if isinstance(v, str):
                out[k] = float(v)
        return out

    normalized = parsed.map(normalize, output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY())).name("normalize")

    # Event-time + key by symbol
    keyed = normalized.assign_timestamps_and_watermarks(wm).key_by(SymbolKeySelector(), key_type=Types.STRING())

    enriched = keyed.flat_map(TAEnricher(), output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY())).name("ta-enrich")

    # Partitioned S3A path: date=YYYY-MM-DD/symbol=SYM
    def bucket_assigner(el: dict) -> str:
        d = datetime.fromtimestamp(el['timestamp'] / 1000, tz=timezone.utc).date().isoformat()
        return f"date={d}/symbol={el['symbol']}"

    writer_factory = FastAvroBulkWriterFactory(AVRO_SCHEMA_PATH)

    sink = (FileSink
            .for_bulk_format("s3a://quant/streaming/market_avro", writer_factory)
            .with_bucket_assigner(bucket_assigner)
            .with_rolling_policy(RollingPolicy.default_rolling_policy())
            .with_output_file_config(OutputFileConfig.builder()
                                     .with_part_prefix("part")
                                     .with_part_suffix(".avro")
                                     .build())
            .build())

    enriched.sink_to(sink).name("avro-to-minio")

    env.execute("Market streaming TA → Avro")

if __name__ == "__main__":
    build_job()
