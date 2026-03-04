# BiroCo Generator — Configuration Reference

All numeric parameters, probability weights, and distribution settings for
`data_generator.py` live in `config.json`.  Product/discount/comment/name data
live as CSV files under `resources/` (see below).

`config.json` is validated at **import time**.  If any value is out of range the
generator prints a clear error list and exits before producing any output.

---

## CLI Arguments (`data_generator.py`)

| Argument | Default | Valid range | Description |
|----------|---------|-------------|-------------|
| `--output` | `biroco_3nf_generated.xlsx` | any writable path | Output XLSX workbook |
| `--csv-dir` | `output/` | any writable directory | Folder for per-sheet CSV exports |
| `--orders` | `200` | integer **≥ 1** | Number of orders to generate |
| `--seed` | `20260225` | any integer | Random seed (same seed → identical dataset) |

---

## `time`

| Key | Type | Valid range | Default | Description |
|-----|------|-------------|---------|-------------|
| `window_days` | int | **≥ 1** | 180 | Total date range (days back from today) across which orders are spread |
| `recent_window_days` | int | **≥ 1 and < `window_days`** | 30 | Number of days that count as "recent" |
| `recent_order_ratio` | float | **(0, 1]** | 0.35 | Fraction of orders placed within the recent window |

---

## `order.line_count`

Clipped normal distribution controlling how many Orderlines each Order has.

| Key | Type | Valid range | Default | Description |
|-----|------|-------------|---------|-------------|
| `type` | str | — | `"normal"` | Distribution family (reserved; only `normal` implemented) |
| `mean` | float | **> 0** | 6.0 | Mean lines per order |
| `std` | float | **> 0** | 2.0 | Standard deviation |
| `min` | int | **≥ 1** | 1 | Hard minimum (clip lower tail) |
| `max` | int | **≥ min** | 12 | Hard maximum (clip upper tail) |

---

## `rates`

| Key | Type | Valid range | Default | Description |
|-----|------|-------------|---------|-------------|
| `return_rate` | float | **[0, 1]** | 0.10 | Probability that a delivered Orderline generates a Return |
| `rating_coverage` | float | **[0, 1]** | 0.82 | Fraction of eligible Orderlines that receive a rating |

---

## `pricing`

### `pricing.gross_margin`

Clipped normal distribution for the per-product gross margin used to derive
supplier unit cost from retail price.

| Key | Type | Valid range | Default | Description |
|-----|------|-------------|---------|-------------|
| `type` | str | — | `"normal"` | Distribution family (reserved) |
| `min` | float | **[0, 1)** | 0.30 | Minimum margin (clip lower tail) |
| `max` | float | **(0, 1]** and **> min** | 0.70 | Maximum margin (clip upper tail) |
| `mean` | float | **(0, 1)** | 0.50 | Mean margin |
| `std` | float | **> 0** | 0.09 | Standard deviation |

### `pricing.lead_time_margin`

Adjusts margin tolerance based on supplier lead time.

| Key | Type | Valid range | Default | Description |
|-----|------|-------------|---------|-------------|
| `ref_days` | int | any int | 8 | Reference lead time (days); margins tighten above this |
| `slope` | float | any float | 0.01 | Margin reduction per extra day above `ref_days` |

---

## `id_generation`

Technical parameters for synthetic ID gaps (simulates non-sequential real-world IDs).

| Key | Type | Valid range | Default | Description |
|-----|------|-------------|---------|-------------|
| `gap_event_rate_default` | float | **[0, 1]** | 0.03 | Probability of a gap between consecutive IDs |
| `gap_min` | int | **≥ 1** | 5 | Minimum gap size |
| `gap_max` | int | **≥ gap_min** | 30 | Maximum gap size |
| `gap_event_rate_by_prefix` | object | rates in **[0, 1]** | `{S,P,I: 0}` | Per-prefix overrides (0 = no gaps) |

---

## `weights`

All weight maps are **relative non-negative numbers** (not percentages).
The generator normalises them automatically — they do not need to sum to 100.
**All values must be ≥ 0 and at least one value per map must be > 0.**
`comment_prob_by_band` values are probabilities and must be in **[0, 1]**.

| Key | Description |
|-----|-------------|
| `fulfillment_status` | Distribution of Order fulfilment states |
| `line_qty` | Distribution of quantity per Orderline (keys are integers as strings) |
| `platform` | Order source channel |
| `delivery_method` | Delivery type (`Standard`, `Express`, `Click&Collect`) |
| `payment_method` | Payment instrument |
| `shipping_cost` | Fixed shipping fee (GBP string) per delivery method — **not a weight** |
| `courier_by_delivery_method` | Nested: for each delivery method, courier brand weights |
| `return_reason` | Reason given when a Return is created |
| `return_status` | Lifecycle state of a Return |
| `rating_band` | Which score band (`0`, `1-3`, `4-6`, `7-8`, `9-10`) a rating falls into; weights may be floats; `0` = extreme dissatisfaction (distinct from NULL / no rating) |
| `comment_prob_by_band` | Probability that a rating in that band includes a text comment |

---

## Resource CSV files (`resources/`)

These files define the **data catalogue** rather than generation parameters.
Editing them does **not** require any code changes.

| File | Columns | Notes |
|------|---------|-------|
| `ref_products.csv` | `ProductName, Category, SupplierName, UnitPriceGBP` | Supplier is auto-created from `SupplierName` |
| `ref_discounts.csv` | `DiscountID, DiscountType, DiscountMethod, DiscountValue, PickWeight` | `PickWeight` must be ≥ 0 |
| `ref_comments.csv` | `Band, Comment` | `Band` must be one of `_0`, `_1-3`, `_4-6`, `_7-8`, `_9-10` (leading `_` prevents Excel date auto-conversion; stripped at load time) |
| `ref_names.csv` | `FirstName, LastName` | Duplicate rows increase selection weight (no auto-dedup) |
| `ref_email_domains.csv` | `Domain` | Add rows to expand the email domain pool; duplicates increase pick weight |
| `ref_postcodes.csv` | `Postcode` | Add rows to expand the GB postcode pool; duplicates increase pick weight |

### Adding a new product
Append a row to `ref_products.csv`. The supplier is created automatically if the
`SupplierName` value is new.

### Adding a new discount type
Append a row to `ref_discounts.csv` with a unique `DiscountID`, any `DiscountType`
label, an existing `DiscountMethod` (`fixed` or `percentage`), and a `PickWeight`.
No code changes needed.

### Adding a new discount **method**
Requires code changes in `apply_discount()`, `discount_effective_pct()`, and the
SQL `CHECK` constraint in `csv_to_sql.py`.
