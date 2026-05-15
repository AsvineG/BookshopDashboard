import requests, csv, hashlib, os, sys, time, re, json
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.utils import parsedate_to_datetime

STORE_HASH   = os.environ['BC_STORE_HASH']
CLIENT_ID    = os.environ['BC_CLIENT_ID']
ACCESS_TOKEN = os.environ['BC_ACCESS_TOKEN']
FULL_REFRESH = os.environ.get('FULL_REFRESH','false').lower() == 'true'

BASE_V2 = f'https://api.bigcommerce.com/stores/{STORE_HASH}/v2'
BASE_V3 = f'https://api.bigcommerce.com/stores/{STORE_HASH}/v3'
HEADERS = {
    'X-Auth-Token': ACCESS_TOKEN,
    'X-Auth-Client': CLIENT_ID,
    'Content-Type': 'application/json',
    'Accept': 'application/json',
}

# ── Country → channel mapping ─────────────────────────────────────────────────
COUNTRY_CHANNEL = {
    'united states': 'Reading Eggs USA', 'usa': 'Reading Eggs USA',
    'us': 'Reading Eggs USA', 'canada': 'Reading Eggs USA',
    'united kingdom': 'Reading Eggs UK', 'uk': 'Reading Eggs UK',
    'ireland': 'Reading Eggs UK', 'england': 'Reading Eggs UK',
    'scotland': 'Reading Eggs UK', 'wales': 'Reading Eggs UK',
    'germany': 'Reading Eggs UK', 'france': 'Reading Eggs UK',
    'spain': 'Reading Eggs UK', 'italy': 'Reading Eggs UK',
    'netherlands': 'Reading Eggs UK', 'sweden': 'Reading Eggs UK',
    'norway': 'Reading Eggs UK', 'denmark': 'Reading Eggs UK',
    'finland': 'Reading Eggs UK', 'switzerland': 'Reading Eggs UK',
    'austria': 'Reading Eggs UK', 'belgium': 'Reading Eggs UK',
    'portugal': 'Reading Eggs UK', 'poland': 'Reading Eggs UK',
    'australia': 'Reading Eggs AU', 'new zealand': 'Reading Eggs AU',
    'singapore': 'Reading Eggs AU', 'hong kong': 'Reading Eggs AU',
    'malaysia': 'Reading Eggs AU', 'south africa': 'Reading Eggs AU',
    'india': 'Reading Eggs AU', 'philippines': 'Reading Eggs AU',
}

# ── AUD fallback rates (used only if BC rate field missing) ──────────────────
# Primary: store_default_to_transactional_exchange_rate from BC order object
# Fallback: these hardcoded rates for edge cases (e.g. AUD orders = 1.0)
AUD_RATES = {
    'AUD': 1.0, 'GBP': 1.88, 'USD': 1.40, 'EUR': 1.58,
    'NZD': 0.90, 'CAD': 1.08, 'SGD': 1.05, 'HKD': 0.18,
    'ZAR': 0.075, 'INR': 0.017,
}

def derive_channel(country, channel_id):
    return COUNTRY_CHANNEL.get((country or '').lower().strip(), 'Reading Eggs AU')

def sha256(v):
    if not v: return ''
    return hashlib.sha256(str(v).strip().lower().encode()).hexdigest()[:16]

AEST = timezone(timedelta(hours=10))

def parse_bc_date(s):
    if not s: return None
    s = str(s).strip()
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00'))
    except:
        pass
    try:
        return parsedate_to_datetime(s)
    except:
        pass
    return None

def fmt_date(s):
    dt = parse_bc_date(s)
    if not dt: return ''
    if dt.tzinfo is not None:
        dt = dt.astimezone(AEST)
    return dt.strftime('%d/%m/%Y')

def fmt_time(s):
    dt = parse_bc_date(s)
    if not dt: return ''
    if dt.tzinfo is not None:
        dt = dt.astimezone(AEST)
    return dt.strftime('%H:%M:%S')

def safe_get(url, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            if r.status_code == 429:
                wait = int(r.headers.get('X-Rate-Limit-Time-Reset-Ms', 10000)) / 1000
                print(f'  Rate limited, waiting {wait:.0f}s...')
                time.sleep(wait + 1)
                continue
            if r.status_code == 204 or not r.text.strip():
                return None
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.json()
        except requests.exceptions.JSONDecodeError:
            return None
        except Exception:
            if attempt == retries - 1: raise
            time.sleep(2 ** attempt)
    return None

def bc_get_all_v2(endpoint, params=None, stop_at_id=None):
    results = []
    page = 1
    while True:
        p = dict(params or {})
        p.update({'page': page, 'limit': 250})
        data = safe_get(f'{BASE_V2}{endpoint}', p)
        if not data: break
        batch = data if isinstance(data, list) else []
        if not batch: break
        if stop_at_id:
            filtered = []
            done = False
            for item in batch:
                if int(item.get('id', 0)) <= stop_at_id:
                    done = True
                    break
                filtered.append(item)
            results.extend(filtered)
            print(f'  Page {page}: {len(batch)} fetched, {len(filtered)} new (total: {len(results)})')
            if done:
                break
        else:
            results.extend(batch)
            print(f'  Page {page}: {len(batch)} records (total {len(results)})')
        if len(batch) < 250: break
        page += 1
        time.sleep(0.1)
    return results

def bc_get_all_v3(endpoint, params=None):
    results = []
    page = 1
    while True:
        p = dict(params or {})
        p.update({'page': page, 'limit': 250})
        data = safe_get(f'{BASE_V3}{endpoint}', p)
        if not data: break
        batch = data.get('data', [])
        results.extend(batch)
        print(f'  Page {page}: {len(batch)} records (total {len(results)})')
        if page >= data.get('meta', {}).get('pagination', {}).get('total_pages', 1): break
        page += 1
        time.sleep(0.1)
    return results

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')

def validate_no_pii(rows, filename):
    for i, row in enumerate(rows):
        for k, v in row.items():
            if v and EMAIL_RE.search(str(v)):
                print(f'ERROR: PII in {filename} row {i} field {k}')
                sys.exit(1)
    print(f'  ✅ {filename}: no PII ({len(rows)} rows)')

def write_csv(filename, rows, reference_rows=None):
    if not rows:
        print(f'  ⚠️  {filename}: no rows')
        return
    all_keys = list(rows[0].keys())
    if reference_rows:
        for k in reference_rows[0].keys():
            if k not in all_keys:
                all_keys.append(k)
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=all_keys, extrasaction='ignore')
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, '') for k in all_keys})
    print(f'  ✅ {filename} written ({len(rows)} rows)')

def read_csv(filename):
    if not os.path.exists(filename): return []
    with open(filename, 'r', encoding='utf-8') as f:
        return list(csv.DictReader(f))

# ── Channel map ───────────────────────────────────────────────────────────────
CHANNEL_MAP = {
    1: 'Reading Eggs AU', 2: 'Reading Eggs UK', 3: 'Reading Eggs USA',
    1692460: 'Reading Eggs USA', 1692461: 'Reading Eggs UK',
    1692941: 'Reading Eggs USA', 1707575: 'Reading Eggs AU',
    1707576: 'Reading Eggs AU', 1723482: 'Reading Eggs AU',
    1724452: 'Reading Eggs AU', 1747993: 'Reading Eggs USA',
    1747994: 'Reading Eggs UK', 1823086: 'Reading Eggs AU',
    1835881: 'Reading Eggs AU', 1837891: 'Reading Eggs USA',
    1837898: 'Reading Eggs UK', 1837908: 'Reading Eggs USA',
}
try:
    ch_data = safe_get(f'{BASE_V3}/channels', {'limit': 250})
    if ch_data and ch_data.get('data'):
        for ch in ch_data['data']:
            ch_id = ch.get('id')
            ch_name = ch.get('name', '') or ch.get('type', '')
            if ch_id and ch_name and ch_id not in (1, 2, 3):
                CHANNEL_MAP[ch_id] = ch_name
except Exception as e:
    print(f'  Channel lookup failed: {e}')

# ── Existing data ─────────────────────────────────────────────────────────────
existing_orders = read_csv('orders.csv')
print(f'\n📋 Existing orders.csv: {len(existing_orders)} rows')

max_existing_id = 0
stop_at_id = None

if FULL_REFRESH:
    print('🔄 Full refresh — fetching all orders')
elif existing_orders:
    for row in existing_orders:
        try:
            oid = int(row.get('Order ID', 0))
            if oid > max_existing_id:
                max_existing_id = oid
        except:
            pass
    if max_existing_id > 0:
        cutoff_date = datetime.now() - timedelta(days=7)
        recent_cutoff_id = 0
        for row in existing_orders:
            try:
                row_date = datetime.strptime(row.get('Order Date', ''), '%d/%m/%Y')
                if row_date >= cutoff_date:
                    oid = int(row.get('Order ID', 0))
                    if oid < recent_cutoff_id or recent_cutoff_id == 0:
                        recent_cutoff_id = oid
            except:
                pass
        stop_at_id = (recent_cutoff_id - 1) if recent_cutoff_id > 0 else max_existing_id
        print(f'⚡ Incremental — fetching orders with ID > {stop_at_id}')
else:
    print('📋 No existing data — first run')

# ── 1. Orders ─────────────────────────────────────────────────────────────────
print('\n📦 Fetching orders...')
new_orders = bc_get_all_v2('/orders', {'sort': 'id:desc', 'is_deleted': 'false'}, stop_at_id=stop_at_id)
print(f'  Fetched {len(new_orders)} orders')

if new_orders or FULL_REFRESH:
    existing_details = {}
    if existing_orders and not FULL_REFRESH:
        for row in existing_orders:
            oid = str(row.get('Order ID',''))
            pd  = row.get('Product Details','')
            if oid and pd and 'Product Name:' in pd:
                existing_details[oid] = pd

    orders_needing_items = [o for o in new_orders if str(o['id']) not in existing_details]
    orders_with_details  = [o for o in new_orders if str(o['id']) in existing_details]

    print(f'  Fetching line items for {len(orders_needing_items)} orders...')
    order_items = {}
    for o in orders_with_details:
        order_items[o['id']] = existing_details[str(o['id'])]

    completed = 0

    def fetch_items(oid):
        data = safe_get(f'{BASE_V2}/orders/{oid}/products')
        return oid, (data if isinstance(data, list) else [])

    if orders_needing_items:
        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = {ex.submit(fetch_items, o['id']): o['id'] for o in orders_needing_items}
            for future in as_completed(futures):
                try:
                    oid, items = future.result()
                    order_items[oid] = items
                except:
                    order_items[futures[future]] = []
                completed += 1
                if completed % 500 == 0 or completed == len(orders_needing_items):
                    print(f'    {completed}/{len(orders_needing_items)} done ({completed/len(orders_needing_items)*100:.0f}%)')

    print('  ✅ Line items done')

    # Fetch coupon details for orders with coupon_discount > 0
    orders_with_disc = [o for o in new_orders if float(o.get('coupon_discount',0) or 0) > 0]
    print(f'  Fetching coupon codes for {len(orders_with_disc)} discounted orders...')
    order_coupons = {}

    def fetch_coupons(oid):
        data = safe_get(f'{BASE_V2}/orders/{oid}/coupons')
        return oid, (data if isinstance(data, list) else [])

    if orders_with_disc:
        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = {ex.submit(fetch_coupons, o['id']): o['id'] for o in orders_with_disc}
            for future in as_completed(futures):
                try:
                    oid, coupons = future.result()
                    order_coupons[oid] = coupons
                except:
                    pass
    print(f'  ✅ Coupon codes fetched')

    def _build_coupon_details(o):
        oid = o['id']
        # Use separately fetched coupon data (order API doesn't include by default)
        coupons = order_coupons.get(oid, [])
        if coupons:
            parts = []
            for cp in coupons:
                if isinstance(cp, dict):
                    code = cp.get('code', '')
                    disc = cp.get('discount', 0)
                    ctype = cp.get('type', '')
                    if code:
                        parts.append(f'Coupon Code: {code}, Discount: {disc}, Type: {ctype}')
            if parts:
                return ' | '.join(parts)
        # Fallback: just record the discount amount
        discount = float(o.get('coupon_discount', 0) or 0)
        if discount > 0:
            return f'Coupon Discount: {discount}'
        return ''

    def build_order_row(o):
        oid   = o['id']
        items = order_items.get(oid, [])
        _curr = (o.get('currency_code') or 'AUD').upper()

        # Convert to AUD using BC's stored transactional exchange rate
        # store_default_to_transactional_exchange_rate = how many transaction units per 1 AUD
        # e.g. 0.71 means 1 AUD = 0.71 USD, so USD total / 0.71 = AUD total
        # Fall back to live AUD_RATES if BC rate not present
        _bc_fx = float(o.get('store_default_to_transactional_exchange_rate') or 0)
        if _bc_fx > 0 and _bc_fx != 1.0:
            _rate = 1.0 / _bc_fx  # AUD per 1 foreign unit
        else:
            _rate = AUD_RATES.get(_curr, 1.0)
        def _to_aud(v): return str(round(float(v or 0) * _rate, 2))
        billing    = o.get('billing_address') or {}
        ship_addrs = o.get('shipping_addresses') or []
        if isinstance(ship_addrs, dict):
            ship_addrs = list(ship_addrs.values())
        ship_method = ''
        if ship_addrs and isinstance(ship_addrs[0], dict):
            ship_method = ship_addrs[0].get('shipping_method', '')
        if isinstance(items, str):
            product_details_str = items
        else:
            parts = []
            for it in items:
                if not isinstance(it, dict): continue
                name  = str(it.get('name', '')).replace('|', '/').replace(',', ' ')
                sku   = str(it.get('sku', '')).replace(',', ' ')
                qty   = it.get('quantity', 1)
                price = float(it.get('price_inc_tax', it.get('base_price', 0)) or 0) * _rate
                total_price = round(price * int(qty), 2)
                parts.append(f"Product Name: {name}, Product SKU: {sku}, Product Qty: {qty}, Product Total Price: {total_price}")
            product_details_str = ' | '.join(parts)
        ch_id   = o.get('channel_id', 1)
        ch_name = CHANNEL_MAP.get(ch_id, derive_channel(billing.get('country', ''), ch_id))
        return {
            'Order ID':               str(oid),
            'Order Date':             fmt_date(o.get('date_created', '')),
            'Order Status':           o.get('status', ''),
            'Channel Name':           ch_name,
            'Order Total (inc tax)':  _to_aud(o.get('total_inc_tax','0')),
            'Order Total (ex tax)':   _to_aud(o.get('total_ex_tax','0')),
            'Exchange Rate':          str(round(_rate, 6)),
            'Order Total (AUD)':      _to_aud(o.get('total_inc_tax','0')),
            'Tax Total':              _to_aud(o.get('total_tax','0')),
            'Shipping Cost (ex tax)': _to_aud(o.get('shipping_cost_ex_tax','0')),
            'Coupon Discount':        _to_aud(o.get('coupon_discount','0')),
            'Coupon Details':         _build_coupon_details(o),
            'Payment Method':         o.get('payment_method', ''),
            'Product Details':        product_details_str,
            'Billing Country':        billing.get('country', ''),
            'Billing State':          billing.get('state', ''),
            'Billing Suburb':         billing.get('city', ''),
            'Order Source':           o.get('order_source') or o.get('external_source', ''),
            'Order Time':             fmt_time(o.get('date_created', '')),
            'Ship Method':            ship_method,
            'Date Shipped':           fmt_date(o.get('date_shipped', '')),
            'Total Shipped':          o.get('items_shipped', 0),
            'Customer ID':            sha256(billing.get('email', '')),
            'Order Currency Code':    o.get('currency_code', 'AUD'),
            'Subtotal (ex tax)':      _to_aud(o.get('subtotal_ex_tax', o.get('total_ex_tax','0'))),
            'Total Quantity':         sum(int(it.get('quantity', 1)) for it in items if isinstance(it, dict)),
        }

    new_rows = [build_order_row(o) for o in new_orders]

    if existing_orders and not FULL_REFRESH:
        new_ids = {str(r['Order ID']) for r in new_rows}
        kept = [r for r in existing_orders if str(r.get('Order ID', '')) not in new_ids]
        clean_orders = new_rows + kept
        try: clean_orders.sort(key=lambda r: int(r.get('Order ID', 0)), reverse=True)
        except: pass
        print(f'  Merged: {len(new_rows)} new + {len(kept)} kept = {len(clean_orders)} total')
    else:
        try: new_rows.sort(key=lambda r: int(r.get('Order ID', 0)), reverse=True)
        except: pass
        clean_orders = new_rows

    validate_no_pii(clean_orders, 'orders.csv')
    write_csv('orders.csv', clean_orders, reference_rows=existing_orders if existing_orders else None)

    print('\n📊 Rebuilding sources.csv...')
    clean_sources = [{'order_id': r['Order ID'], 'order_date': r['Order Date'],
                      'channel_id': r['Channel Name'],
                      'attribution': r.get('Order Source', '') or 'direct',
                      'total': r['Order Total (inc tax)']} for r in clean_orders]
    validate_no_pii(clean_sources, 'sources.csv')
    write_csv('sources.csv', clean_sources)
else:
    print('  No new orders — data already up to date.')

# ── 2. Categories (fetch first so we can map IDs → names in products) ─────────
print('\n🗂️  Fetching categories...')
categories_raw = bc_get_all_v3('/catalog/categories')
category_map = {}  # id → name
for cat in categories_raw:
    category_map[cat.get('id')] = cat.get('name', '')
print(f'  {len(category_map)} categories loaded')

# ── 3. Products (enriched) ────────────────────────────────────────────────────
print('\n📚 Fetching products (enriched)...')
products = bc_get_all_v3('/catalog/products', {
    'include': 'variants,custom_fields,images',
})

# Fetch channel assignments in bulk (single call, much faster)
# Correct endpoint: /v3/catalog/products/channel-assignments (NOT per-product)
print('  Fetching channel assignments (bulk)...')
prod_channels = {}
try:
    page = 1
    while True:
        data = safe_get(f'{BASE_V3}/catalog/products/channel-assignments', {'limit': 250, 'page': page})
        if not data or not data.get('data'):
            break
        for a in data['data']:
            pid = a.get('product_id')
            ch  = a.get('channel_id')
            if pid and ch:
                if pid not in prod_channels:
                    prod_channels[pid] = []
                prod_channels[pid].append(ch)
        total_pages = data.get('meta', {}).get('pagination', {}).get('total_pages', 1)
        print(f'    Page {page}/{total_pages}: {len(data["data"])} assignments')
        if page >= total_pages:
            break
        page += 1
    print(f'  ✅ Channel assignments loaded for {len(prod_channels)} products')
except Exception as e:
    print(f'  ⚠️  Channel assignments failed ({e}) — defaulting to all storefronts')
    prod_channels = {}

clean_products = []
for p in products:
    pid = p.get('id', '')

    # Category names (resolve IDs → names)
    cat_ids  = p.get('categories') or []
    cat_names = ','.join(category_map.get(cid, str(cid)) for cid in cat_ids)

    # Custom fields (flatten to key:value string)
    custom_fields = p.get('custom_fields') or []
    custom_str = ' | '.join(f"{cf.get('name','')}: {cf.get('value','')}" for cf in custom_fields if cf.get('name'))

    # Channel assignments → which storefronts
    channels = prod_channels.get(pid, [])
    in_au = any(CHANNEL_MAP.get(c,'').endswith('AU')  for c in channels) or 1 in channels
    in_uk = any(CHANNEL_MAP.get(c,'').endswith('UK')  for c in channels) or 2 in channels
    in_us = any(CHANNEL_MAP.get(c,'').endswith('USA') for c in channels) or 3 in channels

    # Primary image URL
    images = p.get('images') or []
    primary_img = next((img.get('url_thumbnail','') for img in images if img.get('is_thumbnail')), '')
    if not primary_img and images:
        primary_img = images[0].get('url_thumbnail','')

    # Variant count and price range
    variants    = p.get('variants') or []
    variant_skus = ','.join(v.get('sku','') for v in variants if v.get('sku'))
    min_var_price = min((float(v.get('price') or p.get('price') or 0) for v in variants), default=0) if variants else 0
    max_var_price = max((float(v.get('price') or p.get('price') or 0) for v in variants), default=0) if variants else 0

    clean_products.append({
        # ── Identity ──────────────────────────────────────────────────
        'Product ID':              pid,
        'Product Name':            p.get('name', ''),
        'SKU':                     p.get('sku', ''),
        'Type':                    p.get('type', ''),
        'Brand ID':                p.get('brand_id', ''),
        'Condition':               p.get('condition', ''),

        # ── Pricing ───────────────────────────────────────────────────
        'Price':                   p.get('price', ''),
        'Sale Price':              p.get('sale_price', '') or '',    # current sale price if on promo
        'Cost Price':              p.get('cost_price', '') or '',    # for real margin calc
        'Retail Price':            p.get('retail_price', '') or '',
        'Calculated Price':        p.get('calculated_price', '') or '',  # price after rules

        # ── Sales & traffic ───────────────────────────────────────────
        'Total Sold':              p.get('total_sold', 0),           # lifetime units sold from BC
        'View Count':              p.get('view_count', 0),           # storefront page views

        # ── Inventory ─────────────────────────────────────────────────
        'Inventory Level':         p.get('inventory_level', 0),
        'Inventory Warning Level': p.get('inventory_warning_level', 0),
        'Is Visible':              p.get('is_visible', True),        # hidden products won't sell
        'Availability':            p.get('availability', ''),
        'Weight':                  p.get('weight', ''),

        # ── Catalogue ─────────────────────────────────────────────────
        'Categories':              cat_names,                        # human-readable names now
        'Category IDs':            ','.join(str(c) for c in cat_ids),
        'Is Featured':             p.get('is_featured', False),
        'Custom Fields':           custom_str,

        # ── Channel / storefront assignments ─────────────────────────
        'In AU Storefront':        'Y' if in_au else 'N',           # KEY: explains regional gaps
        'In UK Storefront':        'Y' if in_uk else 'N',
        'In USA Storefront':       'Y' if in_us else 'N',
        'Assigned Channel IDs':    ','.join(str(c) for c in channels),

        # ── Variants ──────────────────────────────────────────────────
        'Variant Count':           len(variants),
        'Variant SKUs':            variant_skus,
        'Min Variant Price':       round(min_var_price, 2) if min_var_price else '',
        'Max Variant Price':       round(max_var_price, 2) if max_var_price else '',

        # ── SEO & content ─────────────────────────────────────────────
        'Page Title':              p.get('page_title', ''),
        'Meta Description':        p.get('meta_description', '') or '',
        'Custom URL':              (p.get('custom_url') or {}).get('url', ''),
        'Primary Image':           primary_img,

        # ── Dates ─────────────────────────────────────────────────────
        'Date Created':            fmt_date(p.get('date_created', '')),
        'Date Modified':           fmt_date(p.get('date_modified', '')),
    })

validate_no_pii(clean_products, 'products.csv')
write_csv('products.csv', clean_products)
print(f'  Products enriched with: total_sold, view_count, sale_price, cost_price,')
print(f'    is_visible, channel_assignments, categories (names), custom_fields, variants')

# ── 4. Customers ──────────────────────────────────────────────────────────────
print('\n👥 Fetching customers...')
customers = bc_get_all_v3('/customers')
clean_customers = []
for c in customers:
    credits    = c.get('store_credit_amounts') or []
    credit_amt = credits[0].get('amount', 0) if credits else 0
    channel_ids = c.get('channel_ids') or [1]
    clean_customers.append({
        'Customer ID':                           sha256(c.get('email', '')),
        'Date Joined':                           fmt_date(c.get('date_created', '')),
        'Date Modified':                         fmt_date(c.get('date_modified', '')),
        'Store Credit':                          credit_amt,
        'Total Orders':                          c.get('orders_count', 0),
        'Channel ID':                            channel_ids[0] if channel_ids else 1,
        'Receive Review/Abandoned Cart Emails?': 'Y' if c.get('accepts_product_review_abandoned_cart_emails', True) else 'N',
        'First Name':                            '',
        'Last Name':                             '',
        'Email':                                 sha256(c.get('email', '')),
    })
validate_no_pii(clean_customers, 'customers.csv')
write_csv('customers.csv', clean_customers)

# ── 5. Abandoned carts ────────────────────────────────────────────────────────
print('\n🛒 Fetching abandoned carts...')
try:
    carts = bc_get_all_v2('/customers/abandoned_carts')
    clean_carts = []
    for c in carts:
        if not isinstance(c, dict): continue
        line_items = c.get('line_items') or {}
        if isinstance(line_items, dict):
            item_count = len(line_items.get('physical_items', [])) + len(line_items.get('digital_items', []))
        else:
            item_count = 0
        clean_carts.append({
            'cart_id':        c.get('id', ''),
            'date_created':   fmt_date(c.get('date_created', '')),
            'date_modified':  fmt_date(c.get('date_modified', '')),
            'customer_email': sha256(c.get('customer_email', '')),
            'cart_amount':    c.get('cart_amount', '0'),
            'channel_id':     c.get('channel_id', 1),
            'line_items':     item_count,
        })
    if clean_carts:
        validate_no_pii(clean_carts, 'carts.csv')
        write_csv('carts.csv', clean_carts)
    else:
        print('  ⚠️  No abandoned carts found')
except Exception as e:
    print(f'  ⚠️  Abandoned carts skipped: {e}')

# ── 6. Price Lists ────────────────────────────────────────────────────────────
print('\n💰 Fetching price lists...')
try:
    price_lists_raw = bc_get_all_v3('/pricelists')
    print(f'  Found {len(price_lists_raw)} price lists')

    if price_lists_raw:
        # Write the price lists index (name, currency, status)
        clean_pricelists = []
        for pl in price_lists_raw:
            clean_pricelists.append({
                'Price List ID':   pl.get('id', ''),
                'Name':            pl.get('name', ''),
                'Currency Code':   pl.get('currency_code', ''),
                'Active':          'Y' if pl.get('active', True) else 'N',
                'Date Created':    fmt_date(pl.get('date_created', '')),
                'Date Modified':   fmt_date(pl.get('date_modified', '')),
            })
        write_csv('pricelists.csv', clean_pricelists)

        # ── Price list records (per-SKU prices for each list) ─────────────
        print('  Fetching price list records (per-SKU prices)...')
        all_records = []

        def fetch_pl_records(pl_id, pl_name, currency):
            """Fetch all price records for one price list."""
            # Build product_id → SKU map from already-fetched products
            pid_sku_map = {str(p.get('id','')): (p.get('sku','') or '') for p in products}

            records = []
            page = 1
            while True:
                data = safe_get(f'{BASE_V3}/pricelists/{pl_id}/records', {
                    'page': page, 'limit': 250, 'include': 'currency'
                })
                if not data: break
                batch = data.get('data', [])
                if not batch: break
                for rec in batch:
                    # Currency is per-record in BC (AUD/USD/GBP/NZD/CAD)
                    rec_currency = rec.get('currency', '') or currency or ''
                    # Resolve SKU from product_id (BC doesn't return SKU directly)
                    pid = str(rec.get('product_id', ''))
                    sku = rec.get('sku', '') or pid_sku_map.get(pid, '')
                    records.append({
                        'Price List ID':   pl_id,
                        'Price List Name': pl_name,
                        'Currency':        rec_currency,
                        'SKU':             sku,
                        'Product ID':      pid,
                        'Variant ID':      rec.get('variant_id', ''),
                        'Price':           rec.get('price', ''),
                        'Sale Price':      rec.get('sale_price', '') or '',
                        'Retail Price':    rec.get('retail_price', '') or '',
                        'Bulk Pricing':    str(rec.get('bulk_pricing_tiers', '') or ''),
                    })
                if page >= data.get('meta', {}).get('pagination', {}).get('total_pages', 1):
                    break
                page += 1
                time.sleep(0.05)
            return records

        # Fetch all price list records in parallel
        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = {
                ex.submit(fetch_pl_records, pl['id'], pl.get('name',''), pl.get('currency_code','')): pl['id']
                for pl in price_lists_raw
            }
            for future in as_completed(futures):
                try:
                    records = future.result()
                    all_records.extend(records)
                    print(f'    Price list {futures[future]}: {len(records)} records')
                except Exception as e:
                    print(f'    Price list {futures[future]} records failed: {e}')

        if all_records:
            validate_no_pii(all_records, 'pricelist_records.csv')
            write_csv('pricelist_records.csv', all_records)
            print(f'  ✅ {len(all_records)} total price list records across {len(price_lists_raw)} lists')
        else:
            print('  ⚠️  No price list records found')

        # ── Price list assignments (which channel/customer group each list applies to) ──
        print('  Fetching price list assignments...')
        try:
            assignments_raw = bc_get_all_v3('/pricelists/assignments')
            clean_assignments = []
            for a in assignments_raw:
                # Build readable description of what this list is assigned to
                channel_id   = a.get('channel_id', '')
                customer_grp = a.get('customer_group_id', '')
                pl_id        = a.get('price_list_id', '')
                pl_name      = next((p.get('name','') for p in price_lists_raw if p.get('id')==pl_id), '')
                channel_name = CHANNEL_MAP.get(channel_id, str(channel_id)) if channel_id else 'All channels'
                clean_assignments.append({
                    'Price List ID':        pl_id,
                    'Price List Name':      pl_name,
                    'Channel ID':           channel_id,
                    'Channel Name':         channel_name,
                    'Customer Group ID':    customer_grp,
                    'Customer Group Name':  a.get('customer_group', {}).get('name', '') if isinstance(a.get('customer_group'), dict) else '',
                })
            if clean_assignments:
                write_csv('pricelist_assignments.csv', clean_assignments)
                print(f'  ✅ {len(clean_assignments)} price list assignments')
            else:
                print('  ℹ️  No explicit assignments (price lists may be applied via customer groups in BC admin)')
        except Exception as e:
            print(f'  ⚠️  Price list assignments failed: {e}')

except Exception as e:
    print(f'  ⚠️  Price lists skipped: {e}')

# ── 7. Product Modifiers (bundle compositions) ────────────────────────────────
# Modifiers define the pick-list options (Book 1, Book 2...) for each bundle.
# This tells us exactly which individual books are selectable in each bundle product.
print('\n📖 Fetching product modifiers (bundle compositions)...')
try:
    # Only fetch modifiers for non-ISBN products (bundles/digital that have pick lists)
    bundle_products = [p for p in products
                       if not str(p.get('sku','')).strip().isdigit()
                       or len(str(p.get('sku','')).strip()) != 13]

    print(f'  Fetching modifiers for {len(bundle_products)} non-ISBN products...')

    def fetch_modifiers(pid):
        data = safe_get(f'{BASE_V3}/catalog/products/{pid}/modifiers')
        if data and data.get('data'):
            return pid, data['data']
        return pid, []

    all_modifiers = []
    done = 0

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(fetch_modifiers, p['id']): p for p in bundle_products}
        for future in as_completed(futures):
            p = futures[future]
            try:
                pid, mods = future.result()
                prod_name = p.get('name', '')
                prod_sku  = p.get('sku', '')
                for mod in mods:
                    mod_name = mod.get('display_name', '') or mod.get('name', '')
                    mod_type = mod.get('type', '')
                    # Get option values (the actual book titles in the pick list)
                    option_values = mod.get('option_values', [])
                    if option_values:
                        for ov in option_values:
                            all_modifiers.append({
                                'Product ID':       pid,
                                'Product Name':     prod_name,
                                'Product SKU':      prod_sku,
                                'Modifier ID':      mod.get('id', ''),
                                'Modifier Name':    mod_name,   # e.g. "Book 1", "Book 2"
                                'Modifier Type':    mod_type,   # e.g. "pick_list"
                                'Option ID':        ov.get('id', ''),
                                'Option Label':     ov.get('label', ''),  # e.g. book title
                                'Is Default':       'Y' if ov.get('is_default', False) else 'N',
                                'Sort Order':       ov.get('sort_order', 0),
                                'Value Data':       json.dumps(ov.get('value_data', '') or ''),
                            })
                    else:
                        # Modifier with no values (e.g. text field, file upload) — still record it
                        all_modifiers.append({
                            'Product ID':       pid,
                            'Product Name':     prod_name,
                            'Product SKU':      prod_sku,
                            'Modifier ID':      mod.get('id', ''),
                            'Modifier Name':    mod_name,
                            'Modifier Type':    mod_type,
                            'Option ID':        '',
                            'Option Label':     '',
                            'Is Default':       '',
                            'Sort Order':       '',
                            'Value Data':       '',
                        })
            except Exception as e:
                pass
            done += 1
            if done % 50 == 0 or done == len(bundle_products):
                print(f'    {done}/{len(bundle_products)} products checked')

    if all_modifiers:
        write_csv('product_modifiers.csv', all_modifiers)
        # Summary: how many products have modifiers
        prods_with_mods = len(set(m['Product ID'] for m in all_modifiers if m['Option Label']))
        print(f'  ✅ {len(all_modifiers)} modifier options across {prods_with_mods} products')
        print(f'  These are your bundle compositions — which books are in each bundle pick list')
    else:
        print('  ℹ️  No product modifiers found')

except Exception as e:
    print(f'  ⚠️  Product modifiers skipped: {e}')

# ── 8. Customer Groups (needed to name price list assignments) ────────────────
print('\n👤 Fetching customer groups...')
try:
    cust_groups_raw = bc_get_all_v2('/customer_groups')
    clean_groups = []
    for g in cust_groups_raw:
        clean_groups.append({
            'Group ID':           g.get('id', ''),
            'Group Name':         g.get('name', ''),
            'Is Default':         'Y' if g.get('is_default', False) else 'N',
            'Category Access':    g.get('category_access', {}).get('type', ''),
            'Discount Amount':    g.get('discount_rules', [{}])[0].get('amount', '') if g.get('discount_rules') else '',
            'Discount Type':      g.get('discount_rules', [{}])[0].get('type', '') if g.get('discount_rules') else '',
        })
    if clean_groups:
        write_csv('customer_groups.csv', clean_groups)
        print(f'  ✅ {len(clean_groups)} customer groups')
        # Now enrich pricelist_assignments.csv with group names if we have both
        if clean_assignments:
            grp_map = {str(g['Group ID']): g['Group Name'] for g in clean_groups}
            for a in clean_assignments:
                if a['Customer Group ID']:
                    a['Customer Group Name'] = grp_map.get(str(a['Customer Group ID']), '')
            write_csv('pricelist_assignments.csv', clean_assignments)
            print('  ✅ Price list assignments enriched with group names')
    else:
        print('  ℹ️  No customer groups found')
except Exception as e:
    print(f'  ⚠️  Customer groups skipped: {e}')

print('\n✅ All done.')
print('\nFiles written:')
print('  orders.csv          — all orders with line items')
print('  products.csv        — full product catalogue with inventory, views, channel assignments')
print('  customers.csv       — customer list (PII hashed)')
print('  sources.csv         — order attribution')
print('  carts.csv           — abandoned carts (if enabled)')
print('  pricelists.csv      — your BC price lists')
print('  pricelist_records.csv    — per-SKU prices for each price list')
print('  pricelist_assignments.csv — which channels/groups each price list applies to')
print('  product_modifiers.csv    — bundle compositions (pick list options per product)')
print('  customer_groups.csv — customer segment definitions')
