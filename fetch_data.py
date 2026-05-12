import requests, csv, hashlib, os, sys, time, re
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# ── Country to channel mapping ────────────────────────────────────────────────
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

def derive_channel(country, channel_id):
    return COUNTRY_CHANNEL.get((country or '').lower().strip(), 'Reading Eggs AU')

def sha256(v):
    if not v: return ''
    return hashlib.sha256(str(v).strip().lower().encode()).hexdigest()[:16]

def parse_bc_date(s):
    if not s: return None
    s = str(s).strip()
    # Try ISO format first (V3 API): "2026-05-11T08:30:00+00:00"
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00'))
    except:
        pass
    # Try RFC 2822 format (V2 API): "Mon, 11 May 2026 08:30:00 +0000"
    try:
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(s)
    except:
        pass
    return None

def fmt_date(s):
    dt = parse_bc_date(s)
    if dt: return dt.strftime('%d/%m/%Y')
    return ''

def fmt_time(s):
    dt = parse_bc_date(s)
    if dt: return dt.strftime('%H:%M:%S')
    return ''

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
                item_id = int(item.get('id', 0))
                if item_id <= stop_at_id:
                    done = True
                    break
                filtered.append(item)
            results.extend(filtered)
            print(f'  Page {page}: {len(batch)} fetched, {len(filtered)} new (total: {len(results)})')
            if done:
                print(f'  Reached existing orders — stopping.')
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

# ── Channel map from BC API ───────────────────────────────────────────────────
CHANNEL_MAP = {1: 'Reading Eggs AU', 2: 'Reading Eggs UK', 3: 'Reading Eggs USA'}
try:
    ch_data = safe_get(f'{BASE_V3}/channels', {'limit': 250})
    if ch_data and ch_data.get('data'):
        for ch in ch_data['data']:
            ch_id = ch.get('id')
            ch_name = ch.get('name', '') or ch.get('type', '')
            if ch_id and ch_name:
                CHANNEL_MAP[ch_id] = ch_name
        print(f'  Channels: {CHANNEL_MAP}')
except Exception as e:
    print(f'  Channel lookup failed: {e}')

# ── Read existing data ────────────────────────────────────────────────────────
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
        # Also re-fetch last 7 days to catch status updates
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
    print('📋 No existing data — first run, fetching all orders')

# ── 1. Orders ─────────────────────────────────────────────────────────────────
print('\n📦 Fetching orders from BigCommerce...')
new_orders = bc_get_all_v2('/orders', {'sort': 'id:desc', 'is_deleted': 'false'}, stop_at_id=stop_at_id)
print(f'  Fetched {len(new_orders)} orders')

if new_orders or FULL_REFRESH:
    # Fetch line items in parallel
    print(f'  Fetching line items ({len(new_orders)} orders, 10 workers)...')
    order_items = {}
    completed = 0

    def fetch_items(oid):
        data = safe_get(f'{BASE_V2}/orders/{oid}/products')
        return oid, (data if isinstance(data, list) else [])

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(fetch_items, o['id']): o['id'] for o in new_orders}
        for future in as_completed(futures):
            try:
                oid, items = future.result()
                order_items[oid] = items
            except:
                order_items[futures[future]] = []
            completed += 1
            if completed % 500 == 0 or completed == len(new_orders):
                print(f'    {completed}/{len(new_orders)} done ({completed/len(new_orders)*100:.0f}%)')

    print('  ✅ Line items done')

    def build_order_row(o):
        oid = o['id']
        items = order_items.get(oid, [])
        billing = o.get('billing_address') or {}
        ship_addrs = o.get('shipping_addresses') or []
        if isinstance(ship_addrs, dict):
            ship_addrs = list(ship_addrs.values())
        ship_method = ''
        if ship_addrs and isinstance(ship_addrs[0], dict):
            ship_method = ship_addrs[0].get('shipping_method', '')
        parts = []
        for it in items:
            if not isinstance(it, dict): continue
            name = str(it.get('name', '')).replace('|', '/').replace(',', ' ')
            sku  = str(it.get('sku', '')).replace(',', ' ')
            qty  = it.get('quantity', 1)
            rate = float(o.get('currency_exchange_rate', 1) or 1)
            price = float(it.get('price_inc_tax', it.get('base_price', 0)) or 0) * rate
            total_price = round(price * int(qty), 2)
            parts.append(f"Product Name: {name}, Product SKU: {sku}, Product Qty: {qty}, Product Total Price: {total_price}")
        ch_id = o.get('channel_id', 1)
        ch_name = CHANNEL_MAP.get(ch_id, derive_channel(billing.get('country', ''), ch_id))
        return {
            'Order ID':               str(oid),
            'Order Date':             fmt_date(o.get('date_created', '')),
            'Order Status':           o.get('status', ''),
            'Channel Name':           ch_name,
            # Pre-convert all monetary values to AUD
            # BC exchange_rate = AUD per 1 unit of order currency
            # e.g. GBP order: rate=1.95, total=50 GBP → 50 * 1.95 = 97.50 AUD
            'Order Total (inc tax)':  str(round(float(o.get('total_inc_tax','0') or 0) * float(o.get('currency_exchange_rate','1') or 1), 2)),
            'Order Total (ex tax)':   str(round(float(o.get('total_ex_tax','0') or 0) * float(o.get('currency_exchange_rate','1') or 1), 2)),
            'Exchange Rate':          '1',
            'Tax Total':              str(round(float(o.get('total_tax','0') or 0) * float(o.get('currency_exchange_rate','1') or 1), 2)),
            'Shipping Cost (ex tax)': str(round(float(o.get('shipping_cost_ex_tax','0') or 0) * float(o.get('currency_exchange_rate','1') or 1), 2)),
            'Coupon Discount':        str(round(float(o.get('coupon_discount','0') or 0) * float(o.get('currency_exchange_rate','1') or 1), 2)),
            'Payment Method':         o.get('payment_method', ''),
            'Product Details':        ' | '.join(parts),
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
            'Subtotal (ex tax)':      str(round(float(o.get('subtotal_ex_tax', o.get('total_ex_tax','0')) or 0) * float(o.get('currency_exchange_rate','1') or 1), 2)),
            'Total Quantity':         sum(int(it.get('quantity', 1)) for it in items if isinstance(it, dict)),
        }

    new_rows = [build_order_row(o) for o in new_orders]

    # Merge with existing
    if existing_orders and not FULL_REFRESH:
        new_ids = {str(r['Order ID']) for r in new_rows}
        kept = [r for r in existing_orders if str(r.get('Order ID', '')) not in new_ids]
        clean_orders = new_rows + kept
        try: clean_orders.sort(key=lambda r: int(r.get('Order ID', 0)), reverse=True)
        except: pass
        print(f'  Merged: {len(new_rows)} new/updated + {len(kept)} kept = {len(clean_orders)} total')
    else:
        try: new_rows.sort(key=lambda r: int(r.get('Order ID', 0)), reverse=True)
        except: pass
        clean_orders = new_rows

    validate_no_pii(clean_orders, 'orders.csv')
    write_csv('orders.csv', clean_orders, reference_rows=existing_orders if existing_orders else None)

    # Rebuild sources from merged orders
    print('\n📊 Rebuilding sources.csv...')
    clean_sources = [{'order_id': r['Order ID'], 'order_date': r['Order Date'],
                      'channel_id': r['Channel Name'],
                      'attribution': r.get('Order Source', '') or 'direct',
                      'total': r['Order Total (inc tax)']} for r in clean_orders]
    validate_no_pii(clean_sources, 'sources.csv')
    write_csv('sources.csv', clean_sources)
else:
    print('  No new orders — data already up to date.')

# ── 2. Products ───────────────────────────────────────────────────────────────
print('\n📚 Fetching products...')
products = bc_get_all_v3('/catalog/products', {'include': 'variants'})
clean_products = []
for p in products:
    clean_products.append({
        'Product ID':              p.get('id', ''),
        'Product Name':            p.get('name', ''),
        'SKU':                     p.get('sku', ''),
        'Price':                   p.get('price', ''),
        'Cost Price':              p.get('cost_price', ''),
        'Retail Price':            p.get('retail_price', ''),
        'Type':                    p.get('type', ''),
        'Inventory Level':         p.get('inventory_level', 0),
        'Inventory Warning Level': p.get('inventory_warning_level', 0),
        'Categories':              ','.join(str(c) for c in (p.get('categories') or [])),
        'Weight':                  p.get('weight', ''),
        'Availability':            p.get('availability', ''),
        'Is Featured':             p.get('is_featured', False),
        'Date Created':            fmt_date(p.get('date_created', '')),
        'Date Modified':           fmt_date(p.get('date_modified', '')),
    })
validate_no_pii(clean_products, 'products.csv')
write_csv('products.csv', clean_products)

# ── 3. Customers ──────────────────────────────────────────────────────────────
print('\n👥 Fetching customers...')
customers = bc_get_all_v3('/customers')
clean_customers = []
for c in customers:
    credits = c.get('store_credit_amounts') or []
    credit_amt = credits[0].get('amount', 0) if credits else 0
    channel_ids = c.get('channel_ids') or [1]
    clean_customers.append({
        'Customer ID':       sha256(c.get('email', '')),
        'Date Created':      fmt_date(c.get('date_created', '')),
        'Date Modified':     fmt_date(c.get('date_modified', '')),
        'Store Credit':      credit_amt,
        'Total Orders':      c.get('orders_count', 0),
        'Channel ID':        channel_ids[0] if channel_ids else 1,
        'Accepts Marketing': c.get('accepts_product_review_abandoned_cart_emails', False),
    })
validate_no_pii(clean_customers, 'customers.csv')
write_csv('customers.csv', clean_customers)

# ── 4. Abandoned carts ────────────────────────────────────────────────────────
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

print('\n✅ All done.')
