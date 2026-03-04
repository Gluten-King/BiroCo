#!/usr/bin/env python3
"""Generate BiroCo synthetic 3NF data into a multi-sheet XLSX workbook."""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from openpyxl import Workbook

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

# Directory containing this script; all default I/O paths resolve relative to it.
_HERE = Path(__file__).parent
_RESOURCES = _HERE / "resources"


def _res(p: str) -> Path:
    """Return p as a Path, resolved relative to _HERE if not absolute."""
    path = Path(p)
    return path if path.is_absolute() else _HERE / path


# ---------------------------------------------------------------------------
# Config  (src/config.json)
# ---------------------------------------------------------------------------

_CFG = json.loads((_HERE / "config.json").read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Config validation  — runs immediately after loading, before any constants
# ---------------------------------------------------------------------------

def _validate_config(cfg: dict) -> None:
    """Validate config.json values and raise ValueError with a full error list."""
    errors: List[str] = []

    def check(cond: bool, msg: str) -> None:
        if not cond:
            errors.append(f"  - {msg}")

    def weights_ok(path: str, d: object) -> None:
        """All values in a weight dict must be >= 0 and at least one must be > 0."""
        if not isinstance(d, dict):
            check(False, f"{path}: must be an object, got {type(d).__name__}")
            return
        for k, v in d.items():
            check(isinstance(v, (int, float)) and v >= 0,
                  f"{path}.{k}: weight must be >= 0 (got {v!r})")
        check(any(v > 0 for v in d.values()),
              f"{path}: at least one weight must be > 0")

    # time
    t = cfg.get("time", {})
    wd  = t.get("window_days", 0)
    rwd = t.get("recent_window_days", 0)
    check(isinstance(wd, int) and wd > 0,
          f"time.window_days: must be a positive integer (got {wd!r})")
    check(isinstance(rwd, int) and rwd > 0,
          f"time.recent_window_days: must be a positive integer (got {rwd!r})")
    check(isinstance(rwd, int) and isinstance(wd, int) and rwd < wd,
          f"time.recent_window_days ({rwd}) must be < time.window_days ({wd})")
    ror = t.get("recent_order_ratio", 0)
    check(isinstance(ror, (int, float)) and 0 < ror <= 1,
          f"time.recent_order_ratio: must be in (0, 1] (got {ror!r})")

    # rates
    r = cfg.get("rates", {})
    for key in ("return_rate", "rating_coverage"):
        v = r.get(key, None)
        check(v is not None and isinstance(v, (int, float)) and 0 <= v <= 1,
              f"rates.{key}: must be in [0, 1] (got {v!r})")

    # order.line_count
    lc = cfg.get("order", {}).get("line_count", {})
    lc_min = lc.get("min", 0)
    lc_max = lc.get("max", 0)
    lc_mean = lc.get("mean", 0)
    lc_std  = lc.get("std", 0)
    check(isinstance(lc_min, int) and lc_min >= 1,
          f"order.line_count.min: must be an integer >= 1 (got {lc_min!r})")
    check(isinstance(lc_max, int) and lc_max >= lc_min,
          f"order.line_count.max: must be an integer >= min ({lc_min}) (got {lc_max!r})")
    check(isinstance(lc_mean, (int, float)) and lc_mean > 0,
          f"order.line_count.mean: must be > 0 (got {lc_mean!r})")
    check(isinstance(lc_std, (int, float)) and lc_std > 0,
          f"order.line_count.std: must be > 0 (got {lc_std!r})")

    # pricing.gross_margin
    gm = cfg.get("pricing", {}).get("gross_margin", {})
    gm_min  = gm.get("min",  None)
    gm_max  = gm.get("max",  None)
    gm_mean = gm.get("mean", None)
    gm_std  = gm.get("std",  None)
    check(gm_min is not None and isinstance(gm_min, (int, float)) and 0 <= gm_min < 1,
          f"pricing.gross_margin.min: must be in [0, 1) (got {gm_min!r})")
    check(gm_max is not None and isinstance(gm_max, (int, float)) and 0 < gm_max <= 1,
          f"pricing.gross_margin.max: must be in (0, 1] (got {gm_max!r})")
    if isinstance(gm_min, (int, float)) and isinstance(gm_max, (int, float)):
        check(gm_min < gm_max,
              f"pricing.gross_margin.min ({gm_min}) must be < max ({gm_max})")
    check(gm_mean is not None and isinstance(gm_mean, (int, float)) and 0 < gm_mean < 1,
          f"pricing.gross_margin.mean: must be in (0, 1) (got {gm_mean!r})")
    check(gm_std is not None and isinstance(gm_std, (int, float)) and gm_std > 0,
          f"pricing.gross_margin.std: must be > 0 (got {gm_std!r})")

    # id_generation
    id_cfg = cfg.get("id_generation", {})
    gap_rate = id_cfg.get("gap_event_rate_default", 0)
    gap_min  = id_cfg.get("gap_min", 1)
    gap_max  = id_cfg.get("gap_max", 1)
    check(isinstance(gap_rate, (int, float)) and 0 <= gap_rate <= 1,
          f"id_generation.gap_event_rate_default: must be in [0, 1] (got {gap_rate!r})")
    check(isinstance(gap_min, int) and gap_min >= 1,
          f"id_generation.gap_min: must be an integer >= 1 (got {gap_min!r})")
    check(isinstance(gap_max, int) and gap_max >= gap_min,
          f"id_generation.gap_max: must be an integer >= gap_min ({gap_min}) (got {gap_max!r})")

    # weights
    w = cfg.get("weights", {})
    for wkey in ("fulfillment_status", "line_qty", "platform", "delivery_method",
                 "payment_method", "return_reason", "return_status",
                 "rating_band", "comment_prob_by_band"):
        weights_ok(f"weights.{wkey}", w.get(wkey))
    for method, sub in w.get("courier_by_delivery_method", {}).items():
        weights_ok(f"weights.courier_by_delivery_method.{method}", sub)

    if errors:
        raise ValueError(
            f"config.json has {len(errors)} error(s):\n" + "\n".join(errors)
        )


_validate_config(_CFG)


# ---------------------------------------------------------------------------
# CLI defaults
# ---------------------------------------------------------------------------

DEFAULT_OUTPUT_XLSX    = "biroco_3nf_generated.xlsx"
DEFAULT_OUTPUT_CSV_DIR = "output"
DEFAULT_SEED           = 20260225
DEFAULT_ORDER_COUNT    = 200

# ---------------------------------------------------------------------------
# Parameters loaded from config.json
# ---------------------------------------------------------------------------

# ID gap simulation (technical; rarely need changing).
_id_cfg = _CFG.get("id_generation", {})
ID_DIGITS                  = 5
ID_GAP_EVENT_RATE_DEFAULT  = _id_cfg.get("gap_event_rate_default", 0.03)
ID_GAP_MIN                 = _id_cfg.get("gap_min", 5)
ID_GAP_MAX                 = _id_cfg.get("gap_max", 30)
# Disable gaps for small/stable dimension tables.
ID_GAP_EVENT_RATE_BY_PREFIX: Dict[str, float] = _id_cfg.get(
    "gap_event_rate_by_prefix", {"S": 0.00, "P": 0.00, "I": 0.00}
)

# Time window.
_t = _CFG["time"]
TIME_WINDOW_DAYS:    int   = _t["window_days"]
RECENT_WINDOW_DAYS:  int   = _t["recent_window_days"]
RECENT_ORDER_RATIO:  float = _t["recent_order_ratio"]

# Categorical weights.
_w = _CFG["weights"]
FULFILLMENT_STATUS_WEIGHTS: Dict[str, int]         = _w["fulfillment_status"]
LINE_QTY_WEIGHTS:           Dict[int, int]         = {int(k): v for k, v in _w["line_qty"].items()}
PLATFORM_WEIGHTS:           Dict[str, int]         = _w["platform"]
DELIVERY_METHOD_WEIGHTS:    Dict[str, int]         = _w["delivery_method"]
PAYMENT_METHOD_WEIGHTS:     Dict[str, int]         = _w["payment_method"]
RETURN_REASON_WEIGHTS:      Dict[str, int]         = _w["return_reason"]
RETURN_STATUS_WEIGHTS:      Dict[str, int]         = _w["return_status"]
RATING_BAND_WEIGHTS:        Dict[str, float]       = _w["rating_band"]
COMMENT_PROB_BY_BAND:       Dict[str, float]       = _w["comment_prob_by_band"]
COURIER_WEIGHTS_BY_DELIVERY_METHOD: Dict[str, Dict[str, int]] = _w["courier_by_delivery_method"]
DELIVERY_METHOD_TO_SHIPPING: Dict[str, Decimal]   = {
    k: Decimal(v) for k, v in _w["shipping_cost"].items()
}

# Pricing model.
_gm = _CFG["pricing"]["gross_margin"]
GROSS_MARGIN_MIN  = Decimal(str(_gm["min"]))
GROSS_MARGIN_MAX  = Decimal(str(_gm["max"]))
GROSS_MARGIN_MEAN = Decimal(str(_gm["mean"]))
GROSS_MARGIN_STD  = Decimal(str(_gm["std"]))
_lt = _CFG["pricing"]["lead_time_margin"]
LEADTIME_MARGIN_REF_DAYS = _lt["ref_days"]
LEADTIME_MARGIN_SLOPE    = Decimal(str(_lt["slope"]))

# Return / rating rates.
_r = _CFG["rates"]
RETURN_RATE:                float = _r["return_rate"]
RATING_COVERAGE_ON_ELIGIBLE: float = _r["rating_coverage"]

# Line-count distribution (clipped normal).
_lc = _CFG["order"]["line_count"]
LINE_COUNT_NORMAL_MEAN: float = _lc["mean"]
LINE_COUNT_NORMAL_STD:  float = _lc["std"]
LINE_COUNT_MIN:         int   = _lc["min"]
LINE_COUNT_MAX:         int   = _lc["max"]

# ---------------------------------------------------------------------------
# Resource tables  (src/resources/*.csv)
# ---------------------------------------------------------------------------

def _load_products() -> List[Tuple[str, str, str, Decimal]]:
    """Load product catalogue from ref_products.csv."""
    with (_RESOURCES / "ref_products.csv").open(encoding="utf-8-sig", newline="") as f:
        return [
            (r["ProductName"], r["Category"], r["SupplierName"], Decimal(r["UnitPriceGBP"]))
            for r in csv.DictReader(f)
        ]


def _load_discounts() -> Tuple[List[Tuple[str, str, str, Decimal]], Dict[str, int]]:
    """Load discount catalogue and pick-weights from ref_discounts.csv."""
    with (_RESOURCES / "ref_discounts.csv").open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    dictionary = [
        (r["DiscountID"], r["DiscountType"], r["DiscountMethod"], Decimal(r["DiscountValue"]))
        for r in rows
    ]
    pick_weights = {r["DiscountID"]: int(r["PickWeight"]) for r in rows}
    return dictionary, pick_weights


def _load_comments() -> Dict[str, List[str]]:
    """Load comment bank from ref_comments.csv, grouped by rating band.

    Band values in the CSV are prefixed with '_' (e.g. '_1-3') to prevent
    Excel from auto-converting range notation to dates. The prefix is stripped
    here so internal keys remain '0', '1-3', '4-6', '7-8', '9-10'.
    """
    result: Dict[str, List[str]] = {}
    with (_RESOURCES / "ref_comments.csv").open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            band = row["Band"].lstrip("_")
            result.setdefault(band, []).append(row["Comment"])
    return result


PRODUCT_CATALOG:    List[Tuple[str, str, str, Decimal]] = _load_products()
DISCOUNT_DICTIONARY, DISCOUNT_PICK_WEIGHTS              = _load_discounts()
DISCOUNT_BY_ID: Dict[str, Tuple[str, str, Decimal]]     = {
    did: (dtype, method, value) for did, dtype, method, value in DISCOUNT_DICTIONARY
}
COMMENT_BANK: Dict[str, List[str]] = _load_comments()

# Supplier names are derived from the product catalogue (first-seen order, deduplicated).
SUPPLIER_NAMES: List[str] = list(dict.fromkeys(p[2] for p in PRODUCT_CATALOG))

# ---------------------------------------------------------------------------
# Data pools loaded from resources / config
# ---------------------------------------------------------------------------

def _load_names() -> Tuple[List[str], List[str]]:
    """Load first and last name pools from ref_names.csv."""
    first, last = [], []
    with (_RESOURCES / "ref_names.csv").open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            first.append(row["FirstName"])
            last.append(row["LastName"])
    return first, last


FIRST_NAMES, LAST_NAMES = _load_names()

def _load_email_domains() -> List[str]:
    """Load email domain pool from ref_email_domains.csv."""
    with (_RESOURCES / "ref_email_domains.csv").open(encoding="utf-8-sig", newline="") as f:
        return [row["Domain"] for row in csv.DictReader(f)]


def _load_postcodes() -> List[str]:
    """Load GB postcode pool from ref_postcodes.csv."""
    with (_RESOURCES / "ref_postcodes.csv").open(encoding="utf-8-sig", newline="") as f:
        return [row["Postcode"] for row in csv.DictReader(f)]


EMAIL_DOMAINS:     List[str] = _load_email_domains()
GB_POSTCODE_PARTS: List[str] = _load_postcodes()

WAREHOUSES = ["WarehouseA", "WarehouseB", "WarehouseC"]

MONEY_Q = Decimal("0.01")
NULL = "NULL"

SHEET_ORDER = [
    "OrderTable",
    "Customer",
    "Product",
    "Orderline",
    "Supplier",
    "SupplierProduct",
    "Discount",
    "Inventory",
    "Delivery",
    "Return",
    "FlatView",
]


# =========================
# Helpers
# =========================

def quantize_money(x: Decimal) -> Decimal:
    return x.quantize(MONEY_Q, rounding=ROUND_HALF_UP)


def money_str(x: Decimal) -> str:
    return f"{quantize_money(x):.2f}"


def decimal_token(x: Decimal) -> str:
    txt = format(x, "f")
    if "." in txt:
        txt = txt.rstrip("0").rstrip(".")
    return txt if txt else "0"


def value_or_null(v: object) -> object:
    if v is None:
        return NULL
    if isinstance(v, str) and v.strip() == "":
        return NULL
    return v


def weighted_choice(rng: random.Random, weights: Dict[object, int]) -> object:
    items = list(weights.keys())
    vals = list(weights.values())
    return rng.choices(items, weights=vals, k=1)[0]


def weighted_plan(rng: random.Random, weights: Dict[str, int], total_count: int) -> List[str]:
    if total_count <= 0:
        return []
    keys = list(weights.keys())
    total_w = sum(weights.values())
    if total_w <= 0:
        return []

    raw = {k: (Decimal(weights[k]) * Decimal(total_count) / Decimal(total_w)) for k in keys}
    counts = {k: int(raw[k]) for k in keys}
    remain = total_count - sum(counts.values())
    if remain > 0:
        frac_sorted = sorted(keys, key=lambda k: (raw[k] - counts[k]), reverse=True)
        for i in range(remain):
            counts[frac_sorted[i % len(frac_sorted)]] += 1

    out: List[str] = []
    for k in keys:
        out.extend([k] * counts[k])
    rng.shuffle(out)
    return out


def weighted_sample_without_replacement(
    rng: random.Random,
    items: Sequence[str],
    weight_map: Dict[str, float],
    k: int,
) -> List[str]:
    pool = list(items)
    out: List[str] = []
    take = min(k, len(pool))
    for _ in range(take):
        weights = [max(0.0001, float(weight_map.get(x, 1.0))) for x in pool]
        chosen = rng.choices(pool, weights=weights, k=1)[0]
        out.append(chosen)
        pool.remove(chosen)
    return out


def sample_line_count(rng: random.Random) -> int:
    raw = int(round(rng.gauss(LINE_COUNT_NORMAL_MEAN, LINE_COUNT_NORMAL_STD)))
    return max(LINE_COUNT_MIN, min(LINE_COUNT_MAX, raw))


def random_date_in_window(rng: random.Random, today: date) -> date:
    if rng.random() < RECENT_ORDER_RATIO:
        days_back = rng.randint(0, RECENT_WINDOW_DAYS - 1)
    else:
        days_back = rng.randint(RECENT_WINDOW_DAYS, TIME_WINDOW_DAYS - 1)
    return today - timedelta(days=days_back)


def pct(amount: Decimal, p: Decimal) -> Decimal:
    return quantize_money(amount * p / Decimal("100"))


def clamp_decimal(x: Decimal, low: Decimal, high: Decimal) -> Decimal:
    if x < low:
        return low
    if x > high:
        return high
    return x


def clamp_discount(subtotal: Decimal, discount: Decimal) -> Decimal:
    if discount < Decimal("0"):
        return Decimal("0")
    if discount > subtotal:
        return subtotal
    return quantize_money(discount)


def sample_clipped_normal_decimal(
    rng: random.Random,
    mean: Decimal,
    std: Decimal,
    low: Decimal,
    high: Decimal,
) -> Decimal:
    raw = Decimal(str(rng.gauss(float(mean), float(std))))
    return clamp_decimal(raw, low, high)


def margin_from_leadtime(base_margin: Decimal, leadtime_days: int) -> Decimal:
    offset_days = Decimal(leadtime_days - LEADTIME_MARGIN_REF_DAYS)
    margin = base_margin + (offset_days * LEADTIME_MARGIN_SLOPE)
    return clamp_decimal(margin, GROSS_MARGIN_MIN, GROSS_MARGIN_MAX)


def discount_effective_pct(subtotal: Decimal, discount_id: str) -> Decimal:
    if subtotal <= Decimal("0"):
        return Decimal("0")
    _, method, raw_value = DISCOUNT_BY_ID[discount_id]
    if method == "percentage":
        return raw_value / Decimal("100")
    return clamp_decimal(raw_value / subtotal, Decimal("0"), Decimal("1"))


def discount_safe_for_margin(subtotal: Decimal, discount_id: str, min_margin_pct: Decimal) -> bool:
    return discount_effective_pct(subtotal, discount_id) <= (min_margin_pct + Decimal("0.000001"))


def safe_discount_ids_for_order(subtotal: Decimal, min_margin_pct: Decimal) -> List[str]:
    return [
        did
        for did, _dtype, _method, _value in DISCOUNT_DICTIONARY
        if discount_safe_for_margin(subtotal, did, min_margin_pct)
    ]


def pick_safe_discount_id(
    rng: random.Random,
    subtotal: Decimal,
    min_margin_pct: Decimal,
    preferred_discount_id: Optional[str] = None,
) -> str:
    safe_ids = safe_discount_ids_for_order(subtotal, min_margin_pct)
    if preferred_discount_id and preferred_discount_id in safe_ids:
        return preferred_discount_id
    if not safe_ids:
        return "D00000"
    safe_weights = {did: DISCOUNT_PICK_WEIGHTS.get(did, 1) for did in safe_ids}
    return str(weighted_choice(rng, safe_weights))


def gb_mobile(rng: random.Random) -> str:
    return "07" + "".join(str(rng.randint(0, 9)) for _ in range(9))


def slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", s.lower())


def random_upper_letters(rng: random.Random, length: int) -> str:
    return "".join(rng.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ") for _ in range(length))


def random_alnum_upper(rng: random.Random, length: int) -> str:
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "".join(rng.choice(alphabet) for _ in range(length))


def generate_tracking_number(rng: random.Random, delivery_method: str) -> str:
    courier_weights = COURIER_WEIGHTS_BY_DELIVERY_METHOD.get(
        delivery_method,
        COURIER_WEIGHTS_BY_DELIVERY_METHOD["Standard"],
    )
    courier = str(weighted_choice(rng, courier_weights))

    # Format-inspired templates:
    # - Royal Mail: 2 letters + 9 digits + GB
    # - Evri: H00AA + 11 digits
    # - DHL: 10 digits
    # - UPS: 1Z + 16 upper alnum
    if courier == "royalmail":
        return f"{random_upper_letters(rng, 2)}{rng.randint(0, 999999999):09d}GB"
    if courier == "evri":
        return f"H00AA{rng.randint(0, 99999999999):011d}"
    if courier == "dhl":
        return f"{rng.randint(0, 9999999999):010d}"
    if courier == "ups":
        return f"1Z{random_alnum_upper(rng, 16)}"
    return f"{rng.randint(0, 9999999999):010d}"


class IdGenerator:
    def __init__(
        self,
        prefix: str,
        rng: random.Random,
        start: int = 1,
        digits: int = ID_DIGITS,
        gap_event_rate: float = ID_GAP_EVENT_RATE_DEFAULT,
        max_gap_event_rate: Optional[float] = ID_GAP_EVENT_RATE_DEFAULT,
        gap_min: int = ID_GAP_MIN,
        gap_max: int = ID_GAP_MAX,
    ) -> None:
        self.prefix = prefix
        self.rng = rng
        self.current = start
        self.digits = digits
        self.gap_event_rate = gap_event_rate
        self.max_gap_event_rate = max_gap_event_rate
        self.gap_min = gap_min
        self.gap_max = gap_max
        self.emitted = 0
        self.gap_events = 0

    def next(self) -> str:
        can_gap_by_cap = True
        if self.max_gap_event_rate is not None and self.emitted > 0:
            # Validator uses: gap_event_rate = gap_events / (id_count - 1).
            # Before emitting next ID, denominator after emit will equal current emitted.
            allowed_events = int(self.max_gap_event_rate * self.emitted)
            can_gap_by_cap = (self.gap_events + 1) <= allowed_events

        if (
            self.emitted > 0
            and self.gap_event_rate > 0
            and can_gap_by_cap
            and self.rng.random() < self.gap_event_rate
        ):
            self.current += self.rng.randint(self.gap_min, self.gap_max)
            self.gap_events += 1
        value = self.current
        self.current += 1
        self.emitted += 1
        return f"{self.prefix}{value:0{self.digits}d}"


def make_id_generator(prefix: str, rng: random.Random, start: int = 1) -> IdGenerator:
    return IdGenerator(
        prefix=prefix,
        rng=rng,
        start=start,
        digits=ID_DIGITS,
        gap_event_rate=ID_GAP_EVENT_RATE_BY_PREFIX.get(prefix, ID_GAP_EVENT_RATE_DEFAULT),
        max_gap_event_rate=ID_GAP_EVENT_RATE_BY_PREFIX.get(prefix, ID_GAP_EVENT_RATE_DEFAULT),
        gap_min=ID_GAP_MIN,
        gap_max=ID_GAP_MAX,
    )


@dataclass
class ProductInfo:
    product_id: str
    product_name: str
    category: str
    supplier_name: str
    unit_price_gbp: Decimal


# =========================
# Generation
# =========================

def build_master_data(rng: random.Random) -> Tuple[
    List[Dict[str, object]],
    List[Dict[str, object]],
    List[Dict[str, object]],
    List[Dict[str, object]],
    Dict[str, ProductInfo],
    Dict[str, str],
]:
    supplier_id_gen = make_id_generator("S", rng)
    product_id_gen = make_id_generator("P", rng)

    supplier_id_by_name: Dict[str, str] = {}
    suppliers: List[Dict[str, object]] = []
    used_supplier_phones: set = set()

    for name in SUPPLIER_NAMES:
        sid = supplier_id_gen.next()
        supplier_id_by_name[name] = sid
        phone = gb_mobile(rng)
        while phone in used_supplier_phones:
            phone = gb_mobile(rng)
        used_supplier_phones.add(phone)
        suppliers.append(
            {
                "SupplierID": sid,
                "SupplierEmail": f"sales@{slugify(name)}.co.uk",
                "SupplierPhone": phone,
                "SupplierName": name,
            }
        )

    products: List[Dict[str, object]] = []
    product_infos: Dict[str, ProductInfo] = {}

    for pname, cat, sname, price in PRODUCT_CATALOG:
        pid = product_id_gen.next()
        info = ProductInfo(pid, pname, cat, sname, price)
        product_infos[pid] = info
        products.append(
            {
                "ProductID": pid,
                "ProductName": pname,
                "Category": cat,
                "UnitPriceGBP": money_str(price),
            }
        )

    supplier_products: List[Dict[str, object]] = []
    base_margin_by_product: Dict[str, Decimal] = {}
    for pid in product_infos.keys():
        base_margin_by_product[pid] = sample_clipped_normal_decimal(
            rng,
            GROSS_MARGIN_MEAN,
            GROSS_MARGIN_STD,
            GROSS_MARGIN_MIN,
            GROSS_MARGIN_MAX,
        )

    def build_supplier_product_row(pid: str, sid: str, leadtime_days: int) -> Dict[str, object]:
        unit_price = quantize_money(product_infos[pid].unit_price_gbp)
        margin_pct = margin_from_leadtime(base_margin_by_product[pid], leadtime_days)
        unit_cost = quantize_money(unit_price * (Decimal("1.00") - margin_pct))
        return {
            "SupplierID": sid,
            "ProductID": pid,
            "LeadTimeDays": str(leadtime_days),
            "UnitCostGBP": money_str(unit_cost),
        }

    pid_list = list(product_infos.keys())
    for pid in pid_list:
        primary_name = product_infos[pid].supplier_name
        primary_sid = supplier_id_by_name[primary_name]
        linked = {primary_sid}
        for sid in linked:
            supplier_products.append(build_supplier_product_row(pid, sid, rng.randint(2, 14)))

        if rng.random() < 0.35:
            alt_sid = rng.choice([x for x in supplier_id_by_name.values() if x not in linked])
            supplier_products.append(build_supplier_product_row(pid, alt_sid, rng.randint(3, 18)))

    inventory: List[Dict[str, object]] = []
    inventory_counter_by_wh = {w: 0 for w in WAREHOUSES}
    for pid in pid_list:
        sales_factor = rng.uniform(0.8, 1.3)
        num_shelves = 1 if rng.random() < 0.65 else 2
        chosen_locations = rng.sample(WAREHOUSES, k=num_shelves)
        for loc in chosen_locations:
            inventory_counter_by_wh[loc] += 1
            wh_letter = loc[-1].upper()
            inv_id = f"I00{wh_letter}{inventory_counter_by_wh[loc]}"
            threshold = int(10 * sales_factor + rng.randint(0, 15))
            stock = threshold + rng.randint(10, 80)
            reorder = rng.randint(max(5, threshold), max(10, threshold + 25))
            restock_days_ago = rng.randint(1, 45)
            inventory.append(
                {
                    "InventoryID": inv_id,
                    "ProductID": pid,
                    "Location": loc,
                    "StockOnHand": str(stock),
                    "RestockThreshold": str(threshold),
                    "LastReorderQty": str(reorder),
                    "LastRestockDate": (date.today() - timedelta(days=restock_days_ago)).isoformat(),
                }
            )

    discounts: List[Dict[str, object]] = []
    for did, dtype, method, value in DISCOUNT_DICTIONARY:
        discounts.append(
            {
                "DiscountID": did,
                "DiscountType": dtype,
                "DiscountMethod": method,
                "DiscountValue": decimal_token(value),
            }
        )

    return suppliers, products, supplier_products, inventory, product_infos, supplier_id_by_name


def generate_customers(rng: random.Random, order_count: int) -> List[Dict[str, object]]:
    customer_id_gen = make_id_generator("C", rng)
    target_customers = max(50, int(order_count * 0.62))
    customers: List[Dict[str, object]] = []
    used_emails: set = set()
    used_phones: set = set()

    for i in range(target_customers):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        suffix = f"{rng.randint(1, 999):03d}"
        domain = rng.choice(EMAIL_DOMAINS)
        email = f"{first.lower()}.{last.lower()}{suffix}@{domain}"
        while email in used_emails:
            suffix = f"{rng.randint(1, 999):03d}"
            email = f"{first.lower()}.{last.lower()}{suffix}@{domain}"
        used_emails.add(email)

        phone = gb_mobile(rng)
        while phone in used_phones:
            phone = gb_mobile(rng)
        used_phones.add(phone)

        customers.append(
            {
                "CustomerID": customer_id_gen.next(),
                "CustomerEmail": email,
                "CustomerName": f"{first} {last}",
                "Postcode": rng.choice(GB_POSTCODE_PARTS),
                "CustomerPhone": phone,
            }
        )

    return customers


def assign_shipping_to_deliveries(total_shipping: Decimal, delivery_count: int) -> List[Decimal]:
    if delivery_count <= 0:
        return []
    if total_shipping == Decimal("0.00"):
        return [Decimal("0.00")] * delivery_count

    equal = quantize_money(total_shipping / Decimal(delivery_count))
    amounts = [equal] * delivery_count
    assigned = sum(amounts)
    diff = quantize_money(total_shipping - assigned)
    amounts[-1] = quantize_money(amounts[-1] + diff)
    if amounts[-1] < Decimal("0.00"):
        amounts[-1] = Decimal("0.00")
    return amounts


def apply_discount(subtotal: Decimal, discount_id: str) -> Decimal:
    row = next(x for x in DISCOUNT_DICTIONARY if x[0] == discount_id)
    _, _, method, raw_value = row
    if method == "fixed":
        discount = raw_value
    else:
        discount = pct(subtotal, raw_value)
    return clamp_discount(subtotal, discount)


def order_pricing_context(
    provisional_lines: Sequence[Dict[str, object]],
    product_infos: Dict[str, ProductInfo],
    unit_cost_by_supplier_product: Dict[Tuple[str, str], Decimal],
) -> Tuple[Decimal, Decimal]:
    subtotal = Decimal("0.00")
    min_margin_pct = Decimal("1.00")
    for line in provisional_lines:
        pid = str(line["ProductID"])
        sid = str(line["SupplierID"])
        qty = int(str(line["Quantity"]))
        unit_price = quantize_money(product_infos[pid].unit_price_gbp)
        unit_cost = quantize_money(unit_cost_by_supplier_product.get((sid, pid), unit_price))
        line_amount = quantize_money(Decimal(qty) * unit_price)
        subtotal += line_amount
        if unit_price <= Decimal("0.00"):
            line_margin_pct = Decimal("0.00")
        else:
            line_margin_pct = clamp_decimal((unit_price - unit_cost) / unit_price, Decimal("0.00"), Decimal("1.00"))
        if line_margin_pct < min_margin_pct:
            min_margin_pct = line_margin_pct
    return quantize_money(subtotal), min_margin_pct


def top_up_subtotal_to_make_discount_safe(
    provisional_lines: List[Dict[str, object]],
    subtotal: Decimal,
    min_margin_pct: Decimal,
    discount_id: str,
    product_infos: Dict[str, ProductInfo],
) -> Decimal:
    if subtotal <= Decimal("0.00") or min_margin_pct <= Decimal("0.00"):
        return subtotal
    _dtype, method, raw_value = DISCOUNT_BY_ID[discount_id]
    if method != "fixed":
        return subtotal
    required_subtotal = raw_value / min_margin_pct
    if required_subtotal <= subtotal:
        return subtotal
    target_line = max(
        provisional_lines,
        key=lambda line: quantize_money(product_infos[str(line["ProductID"])].unit_price_gbp),
    )
    unit_price = quantize_money(product_infos[str(target_line["ProductID"])].unit_price_gbp)
    if unit_price <= Decimal("0.00"):
        return subtotal
    gap = required_subtotal - subtotal
    add_qty = max(1, int(math.ceil(float(gap / unit_price))))
    target_line["Quantity"] = str(int(str(target_line["Quantity"])) + add_qty)
    return quantize_money(subtotal + (Decimal(add_qty) * unit_price))


def pick_score_band(rng: random.Random) -> str:
    return weighted_choice(rng, RATING_BAND_WEIGHTS)


def pick_score_in_band(rng: random.Random, band: str) -> int:
    if band == "0":
        return 0
    if band == "1-3":
        return rng.randint(1, 3)
    if band == "4-6":
        return rng.randint(4, 6)
    if band == "7-8":
        return rng.randint(7, 8)
    return rng.randint(9, 10)


def delivery_statuses_for_order(
    rng: random.Random,
    fulfillment_status: str,
    delivery_count: int,
) -> List[str]:
    if delivery_count <= 0:
        return []
    if fulfillment_status in {"pending", "packed", "shipped", "delivered"}:
        return [fulfillment_status] * delivery_count
    if fulfillment_status == "partially_delivered":
        if delivery_count == 1:
            return ["shipped"]
        delivered_n = rng.randint(1, delivery_count - 1)
        other_n = delivery_count - delivered_n
        other_status = rng.choice(["pending", "packed", "shipped"])
        statuses = (["delivered"] * delivered_n) + ([other_status] * other_n)
        rng.shuffle(statuses)
        return statuses
    return ["pending"] * delivery_count


def derive_fulfillment_status(
    is_cancelled: bool,
    delivery_statuses: Sequence[str],
) -> str:
    if is_cancelled:
        return "cancelled"
    if not delivery_statuses:
        return "pending"
    s = set(delivery_statuses)
    if s == {"pending"}:
        return "pending"
    if s == {"packed"}:
        return "packed"
    if "delivered" in s:
        return "delivered" if s == {"delivered"} else "partially_delivered"
    if "shipped" in s:
        return "shipped"
    if "packed" in s:
        return "packed"
    return "pending"


def derive_aftersales_status(return_statuses: Sequence[str]) -> str:
    if not return_statuses:
        return "no_return"
    s = set(return_statuses)
    if "requested" in s or "approved" in s:
        return "return_in_progress"
    if s == {"refund"}:
        return "refunded"
    if s == {"rejected"}:
        return "return_rejected"
    if s.issubset({"refund", "rejected"}):
        return "partially_refunded"
    return "return_in_progress"


def build_flatview(
    customers_by_id: Dict[str, Dict[str, object]],
    products_by_id: Dict[str, Dict[str, object]],
    suppliers_by_id: Dict[str, Dict[str, object]],
    supplier_products_by_pair: Dict[Tuple[str, str], Dict[str, object]],
    discounts_by_id: Dict[str, Dict[str, object]],
    inventory_by_product: Dict[str, List[Dict[str, object]]],
    orders: List[Dict[str, object]],
    orderlines: List[Dict[str, object]],
    deliveries: List[Dict[str, object]],
    returns: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    delivery_by_id = {str(x["DeliveryID"]): x for x in deliveries}
    return_by_id = {str(x["ReturnID"]): x for x in returns}
    order_by_id = {str(x["OrderID"]): x for x in orders}
    rows: List[Dict[str, object]] = []

    for ol in orderlines:
        oid = str(ol["OrderID"])
        did = str(ol["DeliveryID"]) if ol["DeliveryID"] != NULL else None
        rid = str(ol["ReturnID"]) if ol.get("ReturnID") != NULL else None
        order = order_by_id[oid]
        customer = customers_by_id[str(order["CustomerID"])]
        product = products_by_id[str(ol["ProductID"])]
        supplier = suppliers_by_id[str(ol["SupplierID"])]
        supplier_product = supplier_products_by_pair.get((str(ol["SupplierID"]), str(ol["ProductID"])))
        discount = discounts_by_id[str(order["DiscountID"])]
        delivery = delivery_by_id.get(did) if did else None
        ret = return_by_id.get(rid) if rid else None
        if delivery:
            # Delivery present: use the inventory row whose Location matches the delivery Warehouse.
            wh = str(delivery.get("Warehouse", ""))
            matched = [
                inv for inv in inventory_by_product.get(str(ol["ProductID"]), [])
                if str(inv.get("Location", "")) == wh
            ]
            inv_rows = matched if matched else [{}]
        else:
            # No delivery (cancelled or pending): all inventory fields stay NULL.
            inv_rows = [{}]

        for inv in inv_rows:
            rows.append(
                {
                    "OrderID": oid,
                    "StartDate": order["StartDate"],
                    "EndDate": order["EndDate"],
                    "FulfillmentStatus": order["FulfillmentStatus"],
                    "AfterSalesStatus": order["AfterSalesStatus"],
                    "Platform": order["Platform"],
                    "PaymentMethod": order["PaymentMethod"],
                    "CustomerID": order["CustomerID"],
                    "CustomerEmail": customer["CustomerEmail"],
                    "CustomerName": customer["CustomerName"],
                    "CustomerPhone": customer["CustomerPhone"],
                    "Postcode": customer["Postcode"],
                    "DiscountID": order["DiscountID"],
                    "DiscountType": discount["DiscountType"],
                    "DiscountMethod": discount["DiscountMethod"],
                    "DiscountValue": discount["DiscountValue"],
                    "OrderlineID": ol["OrderlineID"],
                    "ProductID": ol["ProductID"],
                    "ProductName": product["ProductName"],
                    "Category": product["Category"],
                    "UnitPriceGBP": product["UnitPriceGBP"],
                    "UnitCostGBP": supplier_product["UnitCostGBP"] if supplier_product else NULL,
                    "SupplierID": ol["SupplierID"],
                    "SupplierName": supplier["SupplierName"],
                    "SupplierEmail": supplier["SupplierEmail"],
                    "SupplierPhone": supplier["SupplierPhone"],
                    "LeadTimeDays": supplier_product["LeadTimeDays"] if supplier_product else NULL,
                    "Quantity": ol["Quantity"],
                    "DeliveryID": ol["DeliveryID"],
                    "DeliveryMethod": delivery["DeliveryMethod"] if delivery else NULL,
                    "DeliveryStatus": delivery["DeliveryStatus"] if delivery else NULL,
                    "Warehouse": delivery["Warehouse"] if delivery else NULL,
                    "ShippedDate": delivery["ShippedDate"] if delivery else NULL,
                    "DeliveredDate": delivery["DeliveredDate"] if delivery else NULL,
                    "TrackingNumber": delivery["TrackingNumber"] if delivery else NULL,
                    "ShippedGBP": delivery["ShippedGBP"] if delivery else NULL,
                    "RatingScore": ol["RatingScore"],
                    "RatingCreatedAt": ol["RatingCreatedAt"],
                    "Comment": ol["Comment"],
                    "ReturnID": ol["ReturnID"],
                    "ReturnReason": ret["ReturnReason"] if ret else NULL,
                    "ReturnQty": ret["ReturnQty"] if ret else NULL,
                    "ReturnDate": ret["ReturnDate"] if ret else NULL,
                    "ReturnStatus": ret["ReturnStatus"] if ret else NULL,
                    "InventoryID": inv.get("InventoryID", NULL),
                    "Location": inv.get("Location", NULL),
                    "StockOnHand": inv.get("StockOnHand", NULL),
                    "RestockThreshold": inv.get("RestockThreshold", NULL),
                    "LastReorderQty": inv.get("LastReorderQty", NULL),
                    "LastRestockDate": inv.get("LastRestockDate", NULL),
                }
            )
    return rows


def generate_dataset(order_count: int, seed: int) -> Dict[str, List[Dict[str, object]]]:
    rng = random.Random(seed)
    today = date.today()

    suppliers, products, supplier_products, inventory, product_infos, supplier_id_by_name = build_master_data(rng)
    customers = generate_customers(rng, order_count)

    orders: List[Dict[str, object]] = []
    orderlines: List[Dict[str, object]] = []
    deliveries: List[Dict[str, object]] = []
    returns: List[Dict[str, object]] = []

    order_id_gen = make_id_generator("O", rng)
    orderline_id_gen = make_id_generator("OL", rng)
    delivery_id_gen = make_id_generator("DLV", rng)
    return_id_gen = make_id_generator("R", rng)

    customers_by_id = {str(x["CustomerID"]): x for x in customers}
    products_by_id = {str(x["ProductID"]): x for x in products}
    suppliers_by_id = {str(x["SupplierID"]): x for x in suppliers}

    customer_ids = list(customers_by_id.keys())
    customer_activity = {cid: rng.uniform(0.3, 2.2) for cid in customer_ids}

    product_ids = list(product_infos.keys())
    product_popularity = {pid: rng.uniform(0.5, 2.5) for pid in product_ids}
    primary_supplier_by_product = {
        pid: supplier_id_by_name[product_infos[pid].supplier_name]
        for pid in product_ids
    }
    supplier_ids_by_product: Dict[str, List[str]] = defaultdict(list)
    unit_cost_by_supplier_product: Dict[Tuple[str, str], Decimal] = {}
    for sp in supplier_products:
        pid = str(sp["ProductID"])
        sid = str(sp["SupplierID"])
        supplier_ids_by_product[pid].append(sid)
        unit_cost_by_supplier_product[(sid, pid)] = quantize_money(Decimal(str(sp["UnitCostGBP"])))

    inventory_by_product: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for inv in inventory:
        inventory_by_product[str(inv["ProductID"])].append(inv)

    delivery_ids_by_order: Dict[str, List[str]] = defaultdict(list)
    delivery_status_by_id: Dict[str, str] = {}
    delivered_date_by_delivery_id: Dict[str, date] = {}

    customer_coverage_queue = customer_ids.copy()
    rng.shuffle(customer_coverage_queue)
    discount_coverage_queue = [x[0] for x in DISCOUNT_DICTIONARY]
    rng.shuffle(discount_coverage_queue)
    supplier_product_coverage_queue = [
        (str(sp["ProductID"]), str(sp["SupplierID"])) for sp in supplier_products
    ]
    rng.shuffle(supplier_product_coverage_queue)
    fulfillment_plan = weighted_plan(rng, FULFILLMENT_STATUS_WEIGHTS, order_count)

    for idx in range(order_count):
        oid = order_id_gen.next()
        if customer_coverage_queue:
            cid = customer_coverage_queue.pop()
        else:
            cid = rng.choices(customer_ids, weights=[customer_activity[x] for x in customer_ids], k=1)[0]
        start_date = random_date_in_window(rng, today)
        target_fulfillment = fulfillment_plan[idx]
        platform = weighted_choice(rng, PLATFORM_WEIGHTS)
        payment = weighted_choice(rng, PAYMENT_METHOD_WEIGHTS)
        delivery_method = weighted_choice(rng, DELIVERY_METHOD_WEIGHTS)

        forced_pairs: List[Tuple[str, str]] = []
        for _n in range(2):
            if supplier_product_coverage_queue:
                forced_pairs.append(supplier_product_coverage_queue.pop())
        line_count = max(sample_line_count(rng), len(forced_pairs))

        provisional_lines: List[Dict[str, object]] = []
        used_pairs: set[Tuple[str, str]] = set()

        while len(provisional_lines) < line_count:
            if forced_pairs:
                pid, sid = forced_pairs.pop(0)
            else:
                pid = rng.choices(product_ids, weights=[product_popularity[x] for x in product_ids], k=1)[0]
                supplier_pool = supplier_ids_by_product.get(pid, [primary_supplier_by_product[pid]])
                if len(supplier_pool) == 1:
                    sid = supplier_pool[0]
                else:
                    sid = rng.choices(
                        supplier_pool,
                        weights=[8 if x == primary_supplier_by_product[pid] else 2 for x in supplier_pool],
                        k=1,
                    )[0]
            pair = (pid, sid)
            if pair in used_pairs:
                continue
            used_pairs.add(pair)

            qty = int(weighted_choice(rng, LINE_QTY_WEIGHTS))

            provisional_lines.append(
                {
                    "OrderlineID": orderline_id_gen.next(),
                    "OrderID": oid,
                    "ProductID": pid,
                    "SupplierID": sid,
                    "Quantity": str(qty),
                    "DeliveryID": NULL,
                    "ReturnID": NULL,
                    "RatingScore": NULL,
                    "RatingCreatedAt": NULL,
                    "Comment": NULL,
                }
            )

        subtotal, min_margin_pct = order_pricing_context(
            provisional_lines=provisional_lines,
            product_infos=product_infos,
            unit_cost_by_supplier_product=unit_cost_by_supplier_product,
        )
        shipping = DELIVERY_METHOD_TO_SHIPPING[delivery_method]

        preferred_discount_id: Optional[str] = None
        if discount_coverage_queue:
            preferred_discount_id = discount_coverage_queue.pop()
            if not discount_safe_for_margin(subtotal, preferred_discount_id, min_margin_pct):
                subtotal = top_up_subtotal_to_make_discount_safe(
                    provisional_lines=provisional_lines,
                    subtotal=subtotal,
                    min_margin_pct=min_margin_pct,
                    discount_id=preferred_discount_id,
                    product_infos=product_infos,
                )
                subtotal, min_margin_pct = order_pricing_context(
                    provisional_lines=provisional_lines,
                    product_infos=product_infos,
                    unit_cost_by_supplier_product=unit_cost_by_supplier_product,
                )

        discount_id = pick_safe_discount_id(
            rng=rng,
            subtotal=subtotal,
            min_margin_pct=min_margin_pct,
            preferred_discount_id=preferred_discount_id,
        )

        if target_fulfillment != "cancelled":
            # Split deliveries by warehouse (shipping warehouse == storage warehouse).
            # Each product can only be shipped from the warehouse that holds its stock.
            # Greedy approach: each round, pick the warehouse covering the most remaining
            # products and bundle them into a single Delivery record.
            product_wh_opts: Dict[str, set] = {}
            for line in provisional_lines:
                pid = str(line["ProductID"])
                wh_set = {
                    str(inv.get("Location", ""))
                    for inv in inventory_by_product.get(pid, [])
                    if str(inv.get("Location", "")) in WAREHOUSES
                }
                product_wh_opts[pid] = wh_set if wh_set else set(WAREHOUSES)

            unassigned = list(provisional_lines)
            wh_groups: Dict[str, List] = {}
            while unassigned:
                wh_cov: Counter = Counter()
                for line in unassigned:
                    for wh in product_wh_opts[str(line["ProductID"])]:
                        wh_cov[wh] += 1
                top_cnt = wh_cov.most_common(1)[0][1]
                best_wh = rng.choice([wh for wh, cnt in wh_cov.items() if cnt == top_cnt])
                can_ship = [l for l in unassigned if best_wh in product_wh_opts[str(l["ProductID"])]]
                unassigned = [l for l in unassigned if best_wh not in product_wh_opts[str(l["ProductID"])]]
                wh_groups.setdefault(best_wh, []).extend(can_ship)

            groups_with_wh = list(wh_groups.items())  # [(warehouse, [lines]), ...]
            delivery_count = len(groups_with_wh)

            shipping_split = assign_shipping_to_deliveries(shipping, delivery_count)
            d_statuses = delivery_statuses_for_order(rng, str(target_fulfillment), delivery_count)

            for idx, (warehouse, group) in enumerate(groups_with_wh):
                did = delivery_id_gen.next()
                d_status = d_statuses[idx]

                shipped_date: Optional[date] = None
                delivered_date: Optional[date] = None
                tracking = NULL
                if d_status in {"shipped", "delivered"}:
                    shipped_date = start_date + timedelta(days=rng.randint(1, 4))
                    tracking = generate_tracking_number(rng, delivery_method)
                if d_status == "delivered":
                    delivered_date = (shipped_date or start_date) + timedelta(days=rng.randint(0, 3))
                    delivered_date_by_delivery_id[did] = delivered_date

                deliveries.append(
                    {
                        "DeliveryID": did,
                        "Warehouse": warehouse,
                        "DeliveryMethod": delivery_method,
                        "ShippedDate": shipped_date.isoformat() if shipped_date else NULL,
                        "DeliveredDate": delivered_date.isoformat() if delivered_date else NULL,
                        "DeliveryStatus": d_status,
                        "TrackingNumber": tracking,
                        "ShippedGBP": money_str(shipping_split[idx]),
                    }
                )
                delivery_status_by_id[did] = d_status
                delivery_ids_by_order[oid].append(did)

                for line in group:
                    line["DeliveryID"] = did

        for line in provisional_lines:
            orderlines.append(line)

        derived_fulfillment = derive_fulfillment_status(
            is_cancelled=(target_fulfillment == "cancelled"),
            delivery_statuses=[delivery_status_by_id.get(did, "pending") for did in delivery_ids_by_order.get(oid, [])],
        )
        if derived_fulfillment == "cancelled":
            end_date = start_date + timedelta(days=rng.randint(0, 1))
        elif derived_fulfillment == "delivered":
            d_dates = [
                delivered_date_by_delivery_id.get(did)
                for did in delivery_ids_by_order.get(oid, [])
                if delivered_date_by_delivery_id.get(did) is not None
            ]
            end_date = max(d_dates) if d_dates else start_date + timedelta(days=rng.randint(1, 9))
        else:
            end_date = None

        orders.append(
            {
                "OrderID": oid,
                "StartDate": start_date.isoformat(),
                "CustomerID": cid,
                "EndDate": end_date.isoformat() if end_date else NULL,
                "Platform": platform,
                "DiscountID": discount_id,
                "PaymentMethod": payment,
                "FulfillmentStatus": derived_fulfillment,
                "AfterSalesStatus": "no_return",
            }
        )

    # Returns and ratings
    eligible_return_lines: List[Dict[str, object]] = []
    for ol in orderlines:
        did = str(ol["DeliveryID"])
        if did == NULL:
            continue
        if delivery_status_by_id.get(did) == "delivered":
            eligible_return_lines.append(ol)

    selected_for_return_by_order: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    target_return_lines = int(round(len(eligible_return_lines) * RETURN_RATE))
    target_return_lines = max(0, min(target_return_lines, len(eligible_return_lines)))
    if target_return_lines > 0:
        for ol in rng.sample(eligible_return_lines, k=target_return_lines):
            selected_for_return_by_order[str(ol["OrderID"])].append(ol)

    return_exists_for_orderline: set[str] = set()
    for oid, lines in selected_for_return_by_order.items():
        rng.shuffle(lines)
        idx = 0
        while idx < len(lines):
            group_size = rng.randint(1, 3)
            group = lines[idx: idx + group_size]
            idx += group_size
            if not group:
                continue

            filtered: List[Dict[str, object]] = []
            for candidate in group:
                olid = str(candidate["OrderlineID"])
                if olid in return_exists_for_orderline:
                    continue
                filtered.append(candidate)
                return_exists_for_orderline.add(olid)
            if not filtered:
                continue

            rid = return_id_gen.next()
            reason = weighted_choice(rng, RETURN_REASON_WEIGHTS)
            rstatus = weighted_choice(rng, RETURN_STATUS_WEIGHTS)
            qty_sum = sum(int(str(x["Quantity"])) for x in filtered)
            latest_delivered = max(
                delivered_date_by_delivery_id.get(str(x["DeliveryID"]), today)
                for x in filtered
            )
            returns.append(
                {
                    "ReturnID": rid,
                    "ReturnReason": reason,
                    "ReturnQty": str(qty_sum),
                    "ReturnDate": (latest_delivered + timedelta(days=rng.randint(1, 20))).isoformat(),
                    "ReturnStatus": rstatus,
                }
            )
            for g in filtered:
                g["ReturnID"] = rid

    for ol in orderlines:
        oid = str(ol["OrderID"])
        order_row = next(x for x in orders if x["OrderID"] == oid)
        fulfillment = str(order_row["FulfillmentStatus"])
        olid = str(ol["OrderlineID"])
        did = str(ol["DeliveryID"])

        if fulfillment not in {"delivered", "partially_delivered"}:
            continue
        if olid in return_exists_for_orderline:
            continue
        if rng.random() > RATING_COVERAGE_ON_ELIGIBLE:
            continue

        if did == NULL:
            continue
        d_status = delivery_status_by_id.get(did)
        if d_status != "delivered":
            continue

        band = pick_score_band(rng)
        score = pick_score_in_band(rng, band)
        comment: str = NULL
        if rng.random() < COMMENT_PROB_BY_BAND[band]:
            comment = rng.choice(COMMENT_BANK[band])

        delivery_dt = delivered_date_by_delivery_id.get(did, today)
        created_at = delivery_dt + timedelta(days=rng.randint(0, 21))

        ol["RatingScore"] = str(score)
        ol["RatingCreatedAt"] = created_at.isoformat()
        ol["Comment"] = comment

    # EndDate (for delivered orders) should cover latest completed event:
    # delivered timestamp, return timestamp, and rating timestamp.
    order_by_id = {str(x["OrderID"]): x for x in orders}
    return_date_by_id = {
        str(x["ReturnID"]): date.fromisoformat(str(x["ReturnDate"]))
        for x in returns
    }
    latest_event_by_order: Dict[str, date] = {}
    for oid, order in order_by_id.items():
        if str(order["FulfillmentStatus"]) != "delivered":
            continue
        end_raw = str(order["EndDate"])
        if end_raw != NULL:
            latest_event_by_order[oid] = date.fromisoformat(end_raw)

    for ol in orderlines:
        oid = str(ol["OrderID"])
        if oid not in latest_event_by_order:
            continue
        rating_raw = str(ol["RatingCreatedAt"])
        if rating_raw != NULL:
            rdt = date.fromisoformat(rating_raw)
            if rdt > latest_event_by_order[oid]:
                latest_event_by_order[oid] = rdt
        rid = str(ol["ReturnID"])
        if rid != NULL and rid in return_date_by_id:
            rrdt = return_date_by_id[rid]
            if rrdt > latest_event_by_order[oid]:
                latest_event_by_order[oid] = rrdt

    for oid, dtv in latest_event_by_order.items():
        order_by_id[oid]["EndDate"] = dtv.isoformat()

    return_by_id = {str(x["ReturnID"]): x for x in returns}
    returns_by_order: Dict[str, List[str]] = defaultdict(list)
    for ol in orderlines:
        rid = str(ol["ReturnID"]) if ol.get("ReturnID") != NULL else None
        if rid is None:
            continue
        oid = str(ol["OrderID"])
        r = return_by_id.get(rid)
        if r is not None:
            returns_by_order[oid].append(str(r["ReturnStatus"]))

    for order in orders:
        oid = str(order["OrderID"])
        order["AfterSalesStatus"] = derive_aftersales_status(returns_by_order.get(oid, []))

    discounts = [
        {
            "DiscountID": did,
            "DiscountType": dtype,
            "DiscountMethod": method,
            "DiscountValue": decimal_token(value),
        }
        for did, dtype, method, value in DISCOUNT_DICTIONARY
    ]

    flatview = build_flatview(
        customers_by_id=customers_by_id,
        products_by_id=products_by_id,
        suppliers_by_id=suppliers_by_id,
        supplier_products_by_pair={
            (str(x["SupplierID"]), str(x["ProductID"])): x for x in supplier_products
        },
        discounts_by_id={str(x["DiscountID"]): x for x in discounts},
        inventory_by_product=inventory_by_product,
        orders=orders,
        orderlines=orderlines,
        deliveries=deliveries,
        returns=returns,
    )

    return {
        "OrderTable": orders,
        "Customer": customers,
        "Product": products,
        "Orderline": orderlines,
        "Supplier": suppliers,
        "SupplierProduct": supplier_products,
        "Discount": discounts,
        "Inventory": inventory,
        "Delivery": deliveries,
        "Return": returns,
        "FlatView": flatview,
    }


def write_workbook(path: Path, tables: Dict[str, List[Dict[str, object]]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    wb = Workbook()
    first_sheet = True

    for sheet_name in SHEET_ORDER:
        rows = tables[sheet_name]
        if first_sheet:
            ws = wb.active
            ws.title = sheet_name
            first_sheet = False
        else:
            ws = wb.create_sheet(title=sheet_name)

        if not rows:
            ws.append(["EMPTY"])
            continue

        headers = list(rows[0].keys())
        ws.append(headers)
        for row in rows:
            ws.append([value_or_null(row.get(h)) for h in headers])

    wb.save(path)


def write_csv_sheets(output_dir: Path, tables: Dict[str, List[Dict[str, object]]]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for sheet_name in SHEET_ORDER:
        rows = tables[sheet_name]
        csv_path = output_dir / f"{sheet_name}.csv"
        with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            if not rows:
                writer.writerow(["EMPTY"])
                continue
            headers = list(rows[0].keys())
            writer.writerow(headers)
            for row in rows:
                writer.writerow([value_or_null(row.get(h)) for h in headers])


def summarize_tables(tables: Dict[str, List[Dict[str, object]]]) -> List[Tuple[str, int]]:
    return [(name, len(rows)) for name, rows in tables.items()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic BiroCo 3NF workbook.")
    parser.add_argument("--output",  default=DEFAULT_OUTPUT_XLSX,
                        help="Output XLSX path (default: biroco_3nf_generated.xlsx)")
    parser.add_argument("--csv-dir", default=DEFAULT_OUTPUT_CSV_DIR,
                        help="Output folder for per-sheet CSV files (default: output/)")
    parser.add_argument("--orders",  type=int, default=DEFAULT_ORDER_COUNT,
                        help="Number of orders to generate; must be >= 1 (default: 200)")
    parser.add_argument("--seed",    type=int, default=DEFAULT_SEED,
                        help="Random seed for reproducibility; any integer (default: 20260225)")
    args = parser.parse_args()

    if args.orders < 1:
        parser.error(f"--orders must be >= 1 (got {args.orders})")

    out_path = _res(args.output)
    csv_dir  = _res(args.csv_dir)
    tables = generate_dataset(order_count=args.orders, seed=args.seed)
    write_workbook(out_path, tables)
    write_csv_sheets(csv_dir, tables)

    print("Generated workbook:")
    print(f"- path: {out_path.resolve()}")
    print(f"- csv_dir: {csv_dir.resolve()}")
    for name, cnt in summarize_tables(tables):
        print(f"- {name}: {cnt}")


if __name__ == "__main__":
    main()
