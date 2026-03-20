"""
streamlit_ar.py — Accounts Receivable management app for HubSpot.
Reads closed-won deals + line items, allows partial invoicing, creates
HubSpot Commerce Hub invoices via the API.

Run: streamlit run streamlit_ar.py
"""
import os
import json
import hashlib
import urllib.parse
import requests
from pathlib import Path
from datetime import date, timedelta

import streamlit as st
from dotenv import load_dotenv

# ── Setup ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR   = Path(__file__).parent
STATE_FILE   = SCRIPT_DIR / "ar_state.json"

load_dotenv(SCRIPT_DIR / ".env", override=True)

# ── DeepWalk Auth0 config ─────────────────────────────────────────────────────
DW_AUTH0_CLIENT_ID     = os.environ.get("DEEPWALK_AUTH0_CLIENT_ID", "")
DW_AUTH0_CLIENT_SECRET = os.environ.get("DEEPWALK_AUTH0_CLIENT_SECRET", "")
DW_AUTH0_DOMAIN        = os.environ.get("DEEPWALK_AUTH0_DOMAIN", "")
DW_AUTH0_AUDIENCE      = os.environ.get("DEEPWALK_AUTH0_AUDIENCE", "")

HUBSPOT_TOKEN  = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
BASE_URL       = "https://api.hubapi.com"
HEADERS        = {"Authorization": f"Bearer {HUBSPOT_TOKEN}", "Content-Type": "application/json"}

# ── OAuth config (required for hosted/multi-user deployment) ──────────────────
# Leave CLIENT_ID empty to run in dev mode (no login gate, uses HUBSPOT_TOKEN).
OAUTH_CLIENT_ID     = os.environ.get("HUBSPOT_CLIENT_ID", "")
OAUTH_CLIENT_SECRET = os.environ.get("HUBSPOT_CLIENT_SECRET", "")
OAUTH_REDIRECT_URI  = os.environ.get("HUBSPOT_REDIRECT_URI", "http://localhost:8501")
# Restrict login to a single HubSpot portal. Get yours from:
# HubSpot → Settings → Account Management → Account Details → Hub ID
ALLOWED_PORTAL_ID   = os.environ.get("HUBSPOT_PORTAL_ID", "")
# Minimal scope — only used to verify the user is in your org
OAUTH_SCOPES        = "crm.objects.contacts.read"

LINE_ITEM_TYPES = ["Standard", "Mileage", "Web Subscription", "Data Collection"]
TYPE_KEY_MAP    = {"Standard": "standard", "Mileage": "mileage",
                   "Web Subscription": "subscription", "Data Collection": "data"}
TYPE_DISPLAY    = {v: k for k, v in TYPE_KEY_MAP.items()}

OPEN_STATUSES    = {"OPEN", "OVERDUE"}
PAID_STATUSES    = {"PAID"}
EXCLUDE_STATUSES = {"VOID", "DRAFT"}

# ── State persistence ──────────────────────────────────────────────────────────
def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"line_item_types": {}, "miles_history": {}}

def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")

# ── OAuth helpers ──────────────────────────────────────────────────────────────
def _oauth_state() -> str:
    """Deterministic state token derived from the client secret — survives server restarts."""
    return hashlib.sha256(f"ar-oauth-{OAUTH_CLIENT_SECRET}".encode()).hexdigest()[:32]

def oauth_auth_url() -> str:
    return "https://app.hubspot.com/oauth/authorize?" + urllib.parse.urlencode({
        "client_id":    OAUTH_CLIENT_ID,
        "redirect_uri": OAUTH_REDIRECT_URI,
        "scope":        OAUTH_SCOPES,
        "state":        _oauth_state(),
    })

def oauth_exchange_code(code: str) -> dict:
    """Exchange an authorization code for access + refresh tokens."""
    r = requests.post(
        "https://api.hubapi.com/oauth/v1/token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type":    "authorization_code",
            "client_id":     OAUTH_CLIENT_ID,
            "client_secret": OAUTH_CLIENT_SECRET,
            "redirect_uri":  OAUTH_REDIRECT_URI,
            "code":          code,
        },
    )
    r.raise_for_status()
    return r.json()

def oauth_get_user_info(access_token: str) -> dict:
    """Returns {hub_id, user (email), hub_domain, scopes, ...}"""
    r = requests.get(f"https://api.hubapi.com/oauth/v1/access-tokens/{access_token}")
    r.raise_for_status()
    return r.json()

def render_login_page():
    st.set_page_config(page_title="Sign In — AR", page_icon="💰", layout="centered")
    st.title("💰 Accounts Receivable")
    st.markdown("Sign in with your HubSpot account to continue.")
    st.divider()

    st.link_button("Sign in with HubSpot", oauth_auth_url(), type="primary")
    st.caption("You'll be redirected to HubSpot to authorize access.")

# ── DeepWalk client-credentials token fetch ───────────────────────────────────
def fetch_dw_token() -> str:
    """Obtain a DeepWalk API token via client credentials (no browser redirect)."""
    r = requests.post(
        f"https://{DW_AUTH0_DOMAIN}/oauth/token",
        json={
            "grant_type":    "client_credentials",
            "client_id":     DW_AUTH0_CLIENT_ID,
            "client_secret": DW_AUTH0_CLIENT_SECRET,
            "audience":      DW_AUTH0_AUDIENCE,
        },
    )
    r.raise_for_status()
    return r.json()["access_token"]

# ── HubSpot helpers ────────────────────────────────────────────────────────────
def raise_for_status(resp, context=""):
    if not resp.ok:
        try:
            body = resp.json()
        except Exception:
            body = resp.text
        raise requests.HTTPError(
            f"HTTP {resp.status_code} [{context}]: {body}", response=resp
        )

@st.cache_data(ttl=300)
def fetch_closed_won_deals() -> list[dict]:
    url   = f"{BASE_URL}/crm/v3/objects/deals/search"
    props = ["dealname", "dealstage", "pipeline", "amount", "closedate", "createdate", "description", "project_id"]
    all_deals, after, page = [], None, 1
    while True:
        body = {
            "properties": props,
            "limit": 100,
            "filterGroups": [{"filters": [
                {"propertyName": "dealstage", "operator": "EQ", "value": "closedwon"}
            ]}],
        }
        if after:
            body["after"] = after
        resp = requests.post(url, headers=HEADERS, json=body)
        raise_for_status(resp, f"fetch_deals page {page}")
        data    = resp.json()
        results = data.get("results", [])
        all_deals.extend(results)
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
        page += 1
    return all_deals

@st.cache_data(ttl=300)
def fetch_line_items_for_deal(deal_id: str) -> list[dict]:
    # Step 1: get line item IDs associated to the deal
    assoc_url = f"{BASE_URL}/crm/v4/associations/deals/line_items/batch/read"
    resp = requests.post(assoc_url, headers=HEADERS,
                         json={"inputs": [{"id": deal_id}]})
    raise_for_status(resp, f"deal→line_items assoc {deal_id}")
    li_ids = [
        item["toObjectId"]
        for result in resp.json().get("results", [])
        for item in result.get("to", [])
    ]
    if not li_ids:
        return []

    # Step 2: batch read line item properties
    read_url = f"{BASE_URL}/crm/v3/objects/line_items/batch/read"
    props    = ["name", "quantity", "price", "amount", "hs_unit_price", "description",
                "ar_li_type"]
    resp = requests.post(read_url, headers=HEADERS, json={
        "inputs":     [{"id": str(x)} for x in li_ids],
        "properties": props,
    })
    raise_for_status(resp, "batch/read line_items")
    return resp.json().get("results", [])

@st.cache_data(ttl=300)
def fetch_invoices_for_deal(deal_id: str) -> list[dict]:
    # Get invoice IDs
    assoc_url = f"{BASE_URL}/crm/v4/associations/deals/invoices/batch/read"
    resp = requests.post(assoc_url, headers=HEADERS,
                         json={"inputs": [{"id": deal_id}]})
    raise_for_status(resp, f"deal→invoices assoc {deal_id}")
    inv_ids = [
        item["toObjectId"]
        for result in resp.json().get("results", [])
        for item in result.get("to", [])
    ]
    if not inv_ids:
        return []

    # Batch read invoices
    read_url = f"{BASE_URL}/crm/v3/objects/invoices/batch/read"
    props    = ["hs_invoice_status", "hs_amount_billed", "hs_createdate",
                "hs_due_date", "hs_number", "hs_invoice_link"]
    all_inv  = []
    for i in range(0, len(inv_ids), 100):
        chunk = inv_ids[i:i+100]
        r = requests.post(read_url, headers=HEADERS, json={
            "inputs":     [{"id": str(x)} for x in chunk],
            "properties": props,
        })
        raise_for_status(r, "batch/read invoices")
        all_inv.extend(r.json().get("results", []))
    return all_inv

@st.cache_data(ttl=300)
def fetch_invoiced_amounts_for_deal(deal_id: str) -> dict:
    """
    Returns {line_item_name: {"amount": float, "quantity": float, "count": int}}
    summed across all non-void/draft invoices on the deal.
    Used as the source of truth for 'previously billed' per line item.
    """
    invoices = fetch_invoices_for_deal(deal_id)
    valid_inv_ids = [
        inv["id"] for inv in invoices
        if (inv.get("properties", {}).get("hs_invoice_status") or "").upper()
           not in EXCLUDE_STATUSES
    ]
    if not valid_inv_ids:
        return {}

    # Get line item IDs for every valid invoice
    assoc_resp = requests.post(
        f"{BASE_URL}/crm/v4/associations/invoices/line_items/batch/read",
        headers=HEADERS,
        json={"inputs": [{"id": inv_id} for inv_id in valid_inv_ids]},
    )
    raise_for_status(assoc_resp, "invoices→line_items assoc")
    li_ids = [
        item["toObjectId"]
        for result in assoc_resp.json().get("results", [])
        for item in result.get("to", [])
    ]
    if not li_ids:
        return {}

    # Batch-read those line items
    all_li = []
    for i in range(0, len(li_ids), 100):
        chunk = li_ids[i:i+100]
        r = requests.post(
            f"{BASE_URL}/crm/v3/objects/line_items/batch/read",
            headers=HEADERS,
            json={"inputs": [{"id": str(x)} for x in chunk],
                  "properties": ["name", "quantity", "amount"]},
        )
        raise_for_status(r, "batch/read invoice line_items")
        all_li.extend(r.json().get("results", []))

    totals: dict = {}
    for li in all_li:
        p    = li.get("properties", {})
        name = (p.get("name") or "").strip()
        amt  = float(p.get("amount")   or 0)
        qty  = float(p.get("quantity") or 0)
        if name not in totals:
            totals[name] = {"amount": 0.0, "quantity": 0.0, "count": 0}
        totals[name]["amount"]   += amt
        totals[name]["quantity"] += qty
        totals[name]["count"]    += 1
    return totals

def get_unit_price(props: dict, quantity: float) -> float:
    """Try hs_unit_price, then price, then amount/quantity."""
    for field in ("hs_unit_price", "price"):
        v = props.get(field)
        if v not in (None, "", "0", 0):
            try:
                return float(v)
            except (ValueError, TypeError):
                pass
    amount = float(props.get("amount") or 0)
    if quantity and quantity != 0:
        return amount / quantity
    return 0.0

@st.cache_data(ttl=3600)
def fetch_portal_id() -> str:
    """Returns the HubSpot portal (hub) ID for constructing CRM URLs."""
    resp = requests.get(f"{BASE_URL}/integrations/v1/me", headers=HEADERS)
    if resp.ok:
        return str(resp.json().get("portalId", ""))
    return ""


def aggregate_invoices(invoices: list[dict]) -> tuple[float, float, float, int]:
    open_t = paid_t = billed_t = 0.0
    count  = 0
    for inv in invoices:
        p      = inv.get("properties", {})
        status = (p.get("hs_invoice_status") or "").upper()
        if status in EXCLUDE_STATUSES:
            continue
        amt = float(p.get("hs_amount_billed") or 0)
        if status in OPEN_STATUSES:
            open_t   += amt
            billed_t += amt
        elif status in PAID_STATUSES:
            paid_t   += amt
            billed_t += amt
        # Unknown statuses are counted but not added to billed totals
        count += 1
    return open_t, paid_t, billed_t, count

def count_draft_invoices(invoices: list[dict]) -> int:
    return sum(
        1 for inv in invoices
        if (inv.get("properties", {}).get("hs_invoice_status") or "").upper() == "DRAFT"
    )

@st.cache_data(ttl=300)
def fetch_all_draft_invoices() -> list[dict]:
    """Searches HubSpot for all draft invoices and enriches with deal associations."""
    url   = f"{BASE_URL}/crm/v3/objects/invoices/search"
    props = ["hs_invoice_status", "hs_amount_billed", "hs_due_date",
             "hs_number", "hs_createdate"]
    all_inv, after, page = [], None, 1
    while True:
        body = {
            "properties":   props,
            "limit":        100,
            "filterGroups": [{"filters": [
                {"propertyName": "hs_invoice_status", "operator": "EQ", "value": "draft"}
            ]}],
        }
        if after:
            body["after"] = after
        resp = requests.post(url, headers=HEADERS, json=body)
        raise_for_status(resp, f"search draft invoices page {page}")
        data = resp.json()
        all_inv.extend(data.get("results", []))
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
        page += 1

    if not all_inv:
        return []

    # Batch-get deal associations for all draft invoices
    inv_ids    = [inv["id"] for inv in all_inv]
    assoc_resp = requests.post(
        f"{BASE_URL}/crm/v4/associations/invoices/deals/batch/read",
        headers=HEADERS,
        json={"inputs": [{"id": x} for x in inv_ids]},
    )
    raise_for_status(assoc_resp, "draft invoices→deals assoc")
    deal_map: dict[str, str] = {}
    for result in assoc_resp.json().get("results", []):
        inv_id   = str(result.get("from", {}).get("id", ""))
        deal_ids = [str(item["toObjectId"]) for item in result.get("to", [])]
        if deal_ids:
            deal_map[inv_id] = deal_ids[0]

    enriched = []
    for inv in all_inv:
        p = inv.get("properties", {})
        enriched.append({
            "id":       inv["id"],
            "number":   p.get("hs_number", ""),
            "amount":   float(p.get("hs_amount_billed") or 0),
            "due_date": (p.get("hs_due_date") or "")[:10],
            "created":  (p.get("hs_createdate") or "")[:10],
            "deal_id":  deal_map.get(inv["id"], ""),
        })
    return enriched

@st.cache_data(ttl=300)
def fetch_invoice_line_items(invoice_id: str) -> list[dict]:
    """Returns the line items attached to a single invoice."""
    assoc_resp = requests.post(
        f"{BASE_URL}/crm/v4/associations/invoices/line_items/batch/read",
        headers=HEADERS,
        json={"inputs": [{"id": invoice_id}]},
    )
    raise_for_status(assoc_resp, f"invoice {invoice_id}→line_items")
    li_ids = [
        item["toObjectId"]
        for result in assoc_resp.json().get("results", [])
        for item in result.get("to", [])
    ]
    if not li_ids:
        return []
    r = requests.post(
        f"{BASE_URL}/crm/v3/objects/line_items/batch/read",
        headers=HEADERS,
        json={"inputs":     [{"id": str(x)} for x in li_ids],
              "properties": ["name", "quantity", "price", "amount"]},
    )
    raise_for_status(r, "batch/read invoice line_items")
    return r.json().get("results", [])

# ── Pipeline stages ────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def fetch_deal_stages() -> list[dict]:
    """Returns list of {label, stageId, pipelineLabel} for all deal stages."""
    resp = requests.get(f"{BASE_URL}/crm/v3/pipelines/deals", headers=HEADERS)
    raise_for_status(resp, "fetch pipelines")
    stages = []
    for pipeline in resp.json().get("results", []):
        pipe_label = pipeline.get("label", "")
        for stage in pipeline.get("stages", []):
            stages.append({
                "label":         stage.get("label", ""),
                "stageId":       stage.get("id", ""),
                "pipelineLabel": pipe_label,
            })
    return stages

def patch_deal_stage(deal_id: str, stage_id: str):
    resp = requests.patch(
        f"{BASE_URL}/crm/v3/objects/deals/{deal_id}",
        headers=HEADERS,
        json={"properties": {"dealstage": stage_id}},
    )
    raise_for_status(resp, f"patch dealstage {deal_id}")

# ── Invoice creation ───────────────────────────────────────────────────────────
def create_invoice_in_hubspot(deal_id: str, line_items_to_bill: list[dict],
                               due_date: date, publish: bool = False,
                               memo: str = "") -> tuple[str, str, str]:
    """
    Creates the invoice, its line items, and associations.
    If publish=True, sets hs_invoice_status='open' and returns the share link.
    Returns: (invoice_id, invoice_link, hs_number) — link is '' when still draft.
    """
    # 1. Create invoice (draft)
    inv_props: dict = {
        "hs_due_date": due_date.isoformat(),
        "hs_currency": "USD",
    }
    if memo:
        inv_props["hs_note"] = memo
    inv_resp = requests.post(
        f"{BASE_URL}/crm/v3/objects/invoices",
        headers=HEADERS,
        json={"properties": inv_props},
    )
    raise_for_status(inv_resp, "create invoice")
    invoice_id = inv_resp.json()["id"]

    # 2. Create each line item
    li_ids = []
    for li in line_items_to_bill:
        li_props = {
            "name":     li["name"],
            "quantity": str(li["quantity"]),
            "price":    str(li["unit_price"]),  # 'amount' is read-only (computed by HS)
        }
        if li.get("description"):
            li_props["description"] = li["description"]
        li_resp = requests.post(
            f"{BASE_URL}/crm/v3/objects/line_items",
            headers=HEADERS,
            json={"properties": li_props},
        )
        raise_for_status(li_resp, f"create line item {li['name']}")
        li_ids.append(li_resp.json()["id"])

    # 3. Associate line items → invoice
    if li_ids:
        assoc_li_resp = requests.post(
            f"{BASE_URL}/crm/v4/associations/line_items/invoices/batch/create",
            headers=HEADERS,
            json={"inputs": [
                {"from": {"id": li_id}, "to": {"id": invoice_id},
                 "types": [{"associationCategory": "HUBSPOT_DEFINED",
                            "associationTypeId": 410}]}
                for li_id in li_ids
            ]},
        )
        raise_for_status(assoc_li_resp, "associate line_items → invoice")

    # 4. Associate invoice → deal
    assoc_deal_resp = requests.post(
        f"{BASE_URL}/crm/v4/associations/invoices/deals/batch/create",
        headers=HEADERS,
        json={"inputs": [
            {"from": {"id": invoice_id}, "to": {"id": deal_id},
             "types": [{"associationCategory": "HUBSPOT_DEFINED",
                        "associationTypeId": 175}]}
        ]},
    )
    raise_for_status(assoc_deal_resp, "associate invoice → deal")

    # 4b. Silently associate deal's contacts → invoice (type 178)
    try:
        contact_assoc_resp = requests.post(
            f"{BASE_URL}/crm/v4/associations/deals/contacts/batch/read",
            headers=HEADERS,
            json={"inputs": [{"id": deal_id}]},
        )
        if contact_assoc_resp.ok:
            contact_ids = [
                item["toObjectId"]
                for result in contact_assoc_resp.json().get("results", [])
                for item in result.get("to", [])
            ]
            if contact_ids:
                requests.post(
                    f"{BASE_URL}/crm/v4/associations/invoices/contacts/batch/create",
                    headers=HEADERS,
                    json={"inputs": [
                        {"from": {"id": invoice_id}, "to": {"id": str(cid)},
                         "types": [{"associationCategory": "HUBSPOT_DEFINED",
                                    "associationTypeId": 178}]}
                        for cid in contact_ids
                    ]},
                )
    except Exception:
        pass  # Non-critical; don't fail invoice creation

    # 5. Optionally publish (open) the invoice and retrieve its share link
    invoice_link = ""
    if publish:
        patch_resp = requests.patch(
            f"{BASE_URL}/crm/v3/objects/invoices/{invoice_id}",
            headers=HEADERS,
            json={"properties": {"hs_invoice_status": "open"}},
        )
        raise_for_status(patch_resp, "publish invoice")

    # 6. Fetch hs_number (and link if published)
    meta_resp = requests.get(
        f"{BASE_URL}/crm/v3/objects/invoices/{invoice_id}",
        headers=HEADERS,
        params={"properties": "hs_invoice_link,hs_number"},
    )
    raise_for_status(meta_resp, "fetch invoice meta")
    meta_props   = meta_resp.json().get("properties", {})
    invoice_link = meta_props.get("hs_invoice_link", "") if publish else ""
    hs_number    = meta_props.get("hs_number", "")

    return invoice_id, invoice_link, hs_number

# ── Deal details inline panel ──────────────────────────────────────────────────
def render_deal_details_panel(deal_data: dict, state: dict):
    """Inline details panel rendered below a summary row."""
    li_types = state["line_item_types"]
    p = deal_data

    # Ensure li_types is seeded from HubSpot for any items not in local state
    try:
        line_items_preview = fetch_line_items_for_deal(p["id"])
        if seed_li_types_from_hubspot(line_items_preview, li_types):
            save_state(state)
    except Exception:
        pass

    meta_parts = []
    if p.get("pipeline"):
        meta_parts.append(f"**Pipeline:** {p['pipeline']}")
    if p.get("closedate"):
        meta_parts.append(f"**Closed:** {p['closedate'][:10]}")
    if p.get("createdate"):
        meta_parts.append(f"**Created:** {p['createdate'][:10]}")
    if meta_parts:
        st.markdown("  ·  ".join(meta_parts))
    if p.get("description"):
        st.caption(p["description"])

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Deal Amount",  f"${p['amount']:,.0f}")
    c2.metric("Billed",       f"${p['billed']:,.0f}")
    c3.metric("Open",         f"${p['open']:,.0f}")
    c4.metric("Paid",         f"${p['paid']:,.0f}")
    c5.metric("Remaining",    f"${p['remaining']:,.0f}")

    st.markdown("**Line Items**")
    try:
        line_items    = fetch_line_items_for_deal(p["id"])
        invoiced      = fetch_invoiced_amounts_for_deal(p["id"])
    except Exception as e:
        st.warning(f"Could not load line items: {e}")
        line_items = []
        invoiced   = {}

    if line_items:
        li_rows = []
        for li in line_items:
            props       = li.get("properties", {})
            li_id       = li["id"]
            name        = props.get("name") or li_id
            qty         = float(props.get("quantity") or 1)
            unit_p      = get_unit_price(props, qty)
            contract_total = float(props.get("amount") or unit_p * qty)
            li_type_key = li_types.get(li_id, "standard")
            inv_data    = invoiced.get(name.strip(), {})
            billed_amt  = inv_data.get("amount", 0.0)
            billed_qty  = inv_data.get("quantity", 0.0)
            remaining_amt = contract_total - billed_amt

            if li_type_key == "mileage":
                note = f"{billed_qty:g} / {qty:g} mi billed"
            else:
                note = ""

            li_rows.append({
                "Name":       name,
                "Type":       TYPE_DISPLAY.get(li_type_key, "Standard"),
                "Qty":        f"{qty:g}",
                "Unit Price": f"${unit_p:,.4f}",
                "Contract":   f"${contract_total:,.0f}",
                "Billed":     f"${billed_amt:,.0f}",
                "Remaining":  f"${remaining_amt:,.0f}",
                "Note":       note,
            })
        st.dataframe(li_rows, use_container_width=True, hide_index=True)
    else:
        st.caption("No line items found.")

    # Draft invoices for this deal
    try:
        all_invs   = fetch_invoices_for_deal(p["id"])
        draft_invs = [inv for inv in all_invs
                      if (inv.get("properties", {}).get("hs_invoice_status") or "").upper() == "DRAFT"]
    except Exception:
        draft_invs = []

    if draft_invs:
        st.markdown("**Draft Invoices**")
        draft_rows = []
        for inv in draft_invs:
            ip = inv.get("properties", {})
            draft_rows.append({
                "Invoice ID": inv["id"],
                "Number":     ip.get("hs_number", ""),
                "Due":        (ip.get("hs_due_date") or "")[:10],
                "Created":    (ip.get("hs_createdate") or "")[:10],
            })
        st.dataframe(draft_rows, use_container_width=True, hide_index=True)

    # Change Deal Stage
    st.divider()
    stage_key = f"show_stage_{p['id']}"
    if st.button("Change Deal Stage", key=f"stage_toggle_{p['id']}"):
        st.session_state[stage_key] = not st.session_state.get(stage_key, False)

    if st.session_state.get(stage_key, False):
        try:
            stages = fetch_deal_stages()
        except Exception as e:
            st.error(f"Could not fetch stages: {e}")
            stages = []

        if stages:
            stage_labels = [f"{s['label']}  ({s['pipelineLabel']})" for s in stages]
            chosen_label = st.selectbox("Target stage", stage_labels,
                                        key=f"stage_select_{p['id']}")
            chosen_stage = stages[stage_labels.index(chosen_label)]

            c1, c2 = st.columns([1, 3])
            if c1.button("Apply", key=f"stage_apply_{p['id']}", type="primary"):
                try:
                    patch_deal_stage(p["id"], chosen_stage["stageId"])
                    fetch_closed_won_deals.clear()
                    st.session_state[stage_key] = False
                    st.success(f"Deal moved to **{chosen_stage['label']}**.")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))
            if c2.button("Cancel", key=f"stage_cancel_{p['id']}"):
                st.session_state[stage_key] = False
                st.rerun()


# ── Page: AR Dashboard ─────────────────────────────────────────────────────────
def render_dashboard(deals: list[dict]):
    st.header("AR Dashboard")
    today = date.today()

    # Aggregate across all deals
    total_contract = total_billed = total_open = total_paid = 0.0
    total_inv = total_overdue_count = total_draft_count = 0
    aging = {"Current": 0.0, "1–30 days": 0.0, "31–60 days": 0.0,
             "61–90 days": 0.0, "90+ days": 0.0}
    deal_rows   = []
    recent_invs = []  # (created_str, deal_name, status, amount)

    with st.spinner("Computing dashboard…"):
        for d in deals:
            p       = d.get("properties", {})
            deal_id = d["id"]
            name    = p.get("dealname") or "(unnamed)"
            amount  = float(p.get("amount") or 0)
            total_contract += amount
            try:
                invoices = fetch_invoices_for_deal(deal_id)
                open_t, paid, billed, count = aggregate_invoices(invoices)
                drafts = count_draft_invoices(invoices)
            except Exception:
                open_t = paid = billed = 0.0
                count = drafts = 0
                invoices = []

            total_billed += billed
            total_open   += open_t
            total_paid   += paid
            total_inv    += count
            total_draft_count += drafts

            for inv in invoices:
                ip     = inv.get("properties", {})
                status = (ip.get("hs_invoice_status") or "").upper()
                amt    = float(ip.get("hs_amount_billed") or 0)
                due_s  = (ip.get("hs_due_date") or "")[:10]
                created_s = (ip.get("hs_createdate") or "")[:10]

                # Aging: open invoices only
                if status in OPEN_STATUSES:
                    is_overdue = (status == "OVERDUE") or (
                        due_s and due_s < today.isoformat()
                    )
                    total_overdue_count += (1 if is_overdue else 0)
                    if not due_s:
                        aging["Current"] += amt
                    else:
                        try:
                            due_d    = date.fromisoformat(due_s)
                            days_ago = (today - due_d).days
                        except ValueError:
                            days_ago = 0
                        if days_ago <= 0:
                            aging["Current"] += amt
                        elif days_ago <= 30:
                            aging["1–30 days"] += amt
                        elif days_ago <= 60:
                            aging["31–60 days"] += amt
                        elif days_ago <= 90:
                            aging["61–90 days"] += amt
                        else:
                            aging["90+ days"] += amt

                # Recent invoices (non-draft)
                if status not in ("DRAFT", "") and created_s:
                    recent_invs.append((created_s, name, status, amt,
                                        ip.get("hs_number", "")))

            deal_rows.append({
                "Deal":      name,
                "Contract":  amount,
                "Billed":    billed,
                "Open":      open_t,
                "Paid":      paid,
                "Remaining": amount - paid,
                "#Inv":      count,
                "#Draft":    drafts,
            })

    total_remaining      = total_contract - total_paid
    collection_rate      = (total_paid / total_billed * 100) if total_billed else 0.0
    pct_billed           = (total_billed / total_contract * 100) if total_contract else 0.0

    # ── KPI row ───────────────────────────────────────────────────────────────
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Total Contract",  f"${total_contract:,.0f}")
    k2.metric("Total Billed",    f"${total_billed:,.0f}",
              delta=f"{pct_billed:.0f}% of contract")
    k3.metric("Outstanding",     f"${total_open:,.0f}")
    k4.metric("Total Paid",      f"${total_paid:,.0f}")
    k5.metric("Remaining",       f"${total_remaining:,.0f}")
    k6.metric("Collection Rate", f"{collection_rate:.0f}%")

    st.divider()

    # ── Invoice count badges ──────────────────────────────────────────────────
    ic1, ic2, ic3, ic4 = st.columns(4)
    ic1.metric("Active Invoices", total_inv)
    ic2.metric("Overdue",         total_overdue_count,
               delta="⚠️ needs attention" if total_overdue_count else None,
               delta_color="inverse" if total_overdue_count else "off")
    ic3.metric("Drafts Pending",  total_draft_count)
    ic4.metric("Deals Tracked",   len(deals))

    st.divider()

    # ── AR Aging ─────────────────────────────────────────────────────────────
    st.subheader("AR Aging (Open Invoices)")
    ag_cols = st.columns(len(aging))
    for col, (bucket, val) in zip(ag_cols, aging.items()):
        col.metric(bucket, f"${val:,.0f}")

    if aging["90+ days"] > 0 or aging["61–90 days"] > 0:
        st.warning("You have invoices 60+ days overdue. Consider following up.")

    st.divider()

    # ── Top open balances ─────────────────────────────────────────────────────
    st.subheader("Top Open Balances")
    top_open = sorted(
        [r for r in deal_rows if r["Remaining"] > 0],
        key=lambda r: r["Remaining"], reverse=True
    )[:10]
    if top_open:
        st.dataframe(
            [{"Deal":     r["Deal"],
              "Invoiced (Unpaid)": f"${r['Open']:,.0f}",
              "Unbilled":          f"${max(r['Remaining'] - r['Open'], 0):,.0f}",
              "Total Remaining":   f"${r['Remaining']:,.0f}"}
             for r in top_open],
            use_container_width=True, hide_index=True,
        )
    else:
        st.info("No outstanding balances.")

    # ── Recent invoice activity ───────────────────────────────────────────────
    st.subheader("Recent Invoice Activity")
    recent_sorted = sorted(recent_invs, key=lambda x: x[0], reverse=True)[:15]
    if recent_sorted:
        _STATUS_BADGE = {"OVERDUE": "🔴 OVERDUE", "PAID": "✅ PAID",
                         "OPEN": "📬 OPEN", "VOID": "🚫 VOID"}
        st.dataframe(
            [{"Date":   r[0], "Deal": r[1],
              "Number": r[4], "Status": _STATUS_BADGE.get(r[2], r[2]),
              "Amount": f"${r[3]:,.0f}"}
             for r in recent_sorted],
            use_container_width=True, hide_index=True,
        )
    else:
        st.info("No recent invoice activity.")

    st.divider()

    # ── Full deal breakdown ───────────────────────────────────────────────────
    st.subheader("All Deals")
    st.dataframe(
        [{"Deal":      r["Deal"],
          "Contract":  f"${r['Contract']:,.0f}",
          "Billed":    f"${r['Billed']:,.0f}",
          "Open":      f"${r['Open']:,.0f}",
          "Paid":      f"${r['Paid']:,.0f}",
          "Remaining": f"${r['Remaining']:,.0f}",
          "#Inv":      r["#Inv"],
          "#Draft":    r["#Draft"]}
         for r in sorted(deal_rows, key=lambda r: r["Open"], reverse=True)],
        use_container_width=True, hide_index=True,
    )


# ── Page: No deal selected (summary table) ────────────────────────────────────
def render_summary(deals: list[dict], state: dict):
    import pandas as pd

    st.subheader("Closed-Won Deals Summary")
    if not deals:
        st.info("No closed-won deals found.")
        return

    # Build all deal data
    all_deal_data = []
    paid_off      = []
    with st.spinner("Loading invoice totals..."):
        for d in deals:
            p       = d.get("properties", {})
            deal_id = d["id"]
            name    = p.get("dealname") or "(unnamed)"
            amount  = float(p.get("amount") or 0)
            try:
                invoices = fetch_invoices_for_deal(deal_id)
                open_t, paid, billed, count = aggregate_invoices(invoices)
                drafts = count_draft_invoices(invoices)
            except Exception:
                open_t = paid = billed = 0.0
                count  = 0
                drafts = 0
            remaining = amount - paid
            if remaining <= 0:
                paid_off.append((deal_id, name))
            all_deal_data.append({
                "id":          deal_id,
                "name":        name,
                "amount":      amount,
                "billed":      billed,
                "open":        open_t,
                "paid":        paid,
                "remaining":   remaining,
                "count":       count,
                "drafts":      drafts,
                "pipeline":    p.get("pipeline", ""),
                "closedate":   p.get("closedate", ""),
                "createdate":  p.get("createdate", ""),
                "description": p.get("description", ""),
            })

    # ── Filter controls ───────────────────────────────────────────────────────
    fc1, fc2, fc3 = st.columns([3, 1, 1])
    search       = fc1.text_input("🔍 Search deals...", "")
    only_drafts  = fc2.checkbox("Has drafts")
    only_balance = fc3.checkbox("Has balance")

    search_lower  = search.strip().lower()
    visible_deals = [
        dd for dd in all_deal_data
        if (not search_lower or search_lower in dd["name"].lower())
        and (not only_drafts  or dd["drafts"] > 0)
        and (not only_balance or dd["remaining"] > 0)
    ]

    if not visible_deals:
        st.info("No deals match the current filters.")
        return

    # ── Sortable / filterable dataframe ──────────────────────────────────────
    df = pd.DataFrame([
        {
            "Deal Name":  dd["name"],
            "Amount":     dd["amount"],
            "Billed":     dd["billed"],
            "Open":       dd["open"],
            "Paid":       dd["paid"],
            "Remaining":  dd["remaining"],
            "Invoices":   dd["count"],
            "Drafts":     dd["drafts"],
            "Close Date": dd["closedate"][:10] if dd["closedate"] else "",
        }
        for dd in visible_deals
    ])

    event = st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "Deal Name":  st.column_config.TextColumn("Deal Name",  pinned=True),
            "Amount":     st.column_config.NumberColumn("Amount",     format="$%.0f"),
            "Billed":     st.column_config.NumberColumn("Billed",     format="$%.0f"),
            "Open":       st.column_config.NumberColumn("Open",       format="$%.0f"),
            "Paid":       st.column_config.NumberColumn("Paid",       format="$%.0f"),
            "Remaining":  st.column_config.NumberColumn("Remaining",  format="$%.0f"),
        },
    )

    # ── Row-selection actions ─────────────────────────────────────────────────
    selected_rows = event.selection.rows if event and event.selection else []
    if selected_rows:
        dd = visible_deals[selected_rows[0]]
        st.divider()
        st.markdown(f"**Selected:** {dd['name']}")
        ca, cb = st.columns(2)

        det_key = f"show_det_{dd['id']}"
        if ca.button("Details", key=f"det_btn_{dd['id']}", use_container_width=True):
            st.session_state[det_key] = not st.session_state.get(det_key, False)

        if cb.button("Invoice →", key=f"inv_btn_{dd['id']}",
                     type="primary", use_container_width=True):
            st.session_state["selected_deal_id"]     = dd["id"]
            st.session_state["selected_deal_amount"] = dd["amount"]
            st.session_state["view"]                 = "deal"
            st.session_state["goto_invoice_tab"]     = True
            st.rerun()

        if st.session_state.get(det_key, False):
            with st.container(border=True):
                render_deal_details_panel(dd, state)

    # ── Move fully-paid deals to another stage ────────────────────────────────
    st.divider()
    st.subheader("Move Fully-Paid Deals")
    if not paid_off:
        st.info("No deals with remaining balance ≤ $0.")
        return

    st.write(
        f"**{len(paid_off)} deal(s)** have remaining ≤ $0 and are still in "
        f"*Closed Won*. Select a target stage and move them."
    )
    with st.expander("Deals that will be moved", expanded=False):
        for _, name in paid_off:
            st.write(f"• {name}")

    try:
        stages = fetch_deal_stages()
    except Exception as e:
        st.error(f"Could not fetch deal stages: {e}")
        return

    stage_labels = [f"{s['label']}  ({s['pipelineLabel']})" for s in stages]
    default_idx  = next(
        (i for i, s in enumerate(stages) if "paid" in s["label"].lower()), 0
    )
    chosen_label = st.selectbox("Target Stage", stage_labels, index=default_idx)
    chosen_stage = stages[stage_labels.index(chosen_label)]

    if st.button(
        f"Move {len(paid_off)} deal(s) → {chosen_stage['label']}",
        type="primary",
    ):
        errors = []
        for deal_id, name in paid_off:
            try:
                patch_deal_stage(deal_id, chosen_stage["stageId"])
            except Exception as e:
                errors.append(f"{name}: {e}")
        fetch_closed_won_deals.clear()
        if errors:
            st.error("Some deals failed to update:\n" + "\n".join(errors))
        else:
            st.success(
                f"Moved {len(paid_off)} deal(s) to **{chosen_stage['label']}**. "
                "They will no longer appear here after the next refresh."
            )

# ── Global line-item type config page ─────────────────────────────────────────
def render_global_line_item_config(deals: list[dict], state: dict):
    st.header("Line Item Types")
    st.caption(
        "Set the billing type for each line item name. Changes apply across all "
        "deals and all users. The Configure tab on individual deals is an optional override."
    )

    li_types = state["line_item_types"]

    try:
        ensure_li_type_property()
    except Exception:
        pass

    # Collect all line items across all deals, grouped by name
    name_map: dict[str, dict] = {}  # name → {type, li_ids, deal_names}

    with st.spinner("Loading line items across all deals…"):
        for d in deals:
            deal_id   = d["id"]
            deal_name = d.get("properties", {}).get("dealname") or deal_id
            try:
                items = fetch_line_items_for_deal(deal_id)
            except Exception:
                continue
            for li in items:
                li_id  = li["id"]
                props  = li.get("properties", {})
                name   = (props.get("name") or li_id).strip()
                # Resolve type: local state → HubSpot property → default
                hs_type = (props.get("ar_li_type") or "").strip().lower()
                resolved = li_types.get(li_id) or (hs_type if hs_type in TYPE_KEY_MAP.values() else None)
                entry = name_map.setdefault(name, {"type": None, "mixed": False,
                                                    "li_ids": [], "deal_names": []})
                entry["li_ids"].append(li_id)
                if deal_name not in entry["deal_names"]:
                    entry["deal_names"].append(deal_name)
                if resolved:
                    if entry["type"] is None:
                        entry["type"] = resolved
                    elif entry["type"] != resolved:
                        entry["mixed"] = True

    if not name_map:
        st.info("No line items found across any deals.")
        return

    st.markdown(f"**{len(name_map)} unique line item name(s)** found across {len(deals)} deals.")
    st.divider()

    updated: dict[str, str] = {}  # name → new type key

    for name, entry in sorted(name_map.items()):
        cur_type    = entry["type"] or "standard"
        cur_display = TYPE_DISPLAY.get(cur_type, "Standard")
        mixed_note  = "  *(mixed — override below)*" if entry["mixed"] else ""
        deal_note   = ", ".join(entry["deal_names"][:3])
        if len(entry["deal_names"]) > 3:
            deal_note += f" +{len(entry['deal_names']) - 3} more"

        cols = st.columns([3, 2, 2])
        cols[0].markdown(f"**{name}**{mixed_note}")
        cols[1].caption(deal_note)
        chosen = cols[2].selectbox(
            "Type",
            LINE_ITEM_TYPES,
            index=LINE_ITEM_TYPES.index(cur_display),
            key=f"global_type_{name}",
            label_visibility="collapsed",
        )
        updated[name] = TYPE_KEY_MAP[chosen]

    st.divider()
    if st.button("Save All", type="primary"):
        changed_ids: dict[str, str] = {}  # li_id → type
        for name, new_type in updated.items():
            for li_id in name_map[name]["li_ids"]:
                changed_ids[li_id] = new_type

        save_li_types_to_hubspot(changed_ids)
        li_types.update(changed_ids)
        save_state(state)
        fetch_line_items_for_deal.clear()

        n_names = len(updated)
        n_ids   = len(changed_ids)
        st.success(f"Saved types for {n_names} name(s) across {n_ids} line item(s).")


# ── Line-item type config — HubSpot-backed ─────────────────────────────────────
@st.cache_data(ttl=3600)
def ensure_li_type_property() -> bool:
    """Creates the custom `ar_li_type` property on line_items if it doesn't exist."""
    url  = f"{BASE_URL}/crm/v3/properties/line_items"
    body = {
        "name":      "ar_li_type",
        "label":     "AR Line Item Type",
        "type":      "string",
        "fieldType": "text",
        "groupName": "lineiteminformation",
    }
    resp = requests.post(url, headers=HEADERS, json=body)
    # 409 = already exists — that's fine
    if resp.status_code not in (200, 201, 409):
        raise_for_status(resp, "create ar_li_type property")
    return True


def seed_li_types_from_hubspot(line_items: list[dict], li_types: dict) -> bool:
    """
    Reads `ar_li_type` from fetched line item properties and writes any
    missing entries into `li_types` (in-place).  Returns True if anything changed.
    """
    changed = False
    for li in line_items:
        li_id   = li["id"]
        hs_type = (li.get("properties", {}).get("ar_li_type") or "").strip().lower()
        if li_id not in li_types and hs_type in TYPE_KEY_MAP.values():
            li_types[li_id] = hs_type
            changed = True
    return changed


def save_li_types_to_hubspot(updated_types: dict):
    """PATCHes each line item with its ar_li_type value (fire-and-forget errors)."""
    for li_id, li_type in updated_types.items():
        try:
            r = requests.patch(
                f"{BASE_URL}/crm/v3/objects/line_items/{li_id}",
                headers=HEADERS,
                json={"properties": {"ar_li_type": li_type}},
            )
            raise_for_status(r, f"save ar_li_type for {li_id}")
        except Exception:
            pass  # Non-critical; local state is the fallback


# ── Tab 1: Configure Line Items ────────────────────────────────────────────────
def render_configure_tab(deal_id: str, line_items: list[dict], state: dict):
    li_types = state["line_item_types"]

    # Seed any missing types from the HubSpot ar_li_type property
    try:
        ensure_li_type_property()
    except Exception:
        pass
    if seed_li_types_from_hubspot(line_items, li_types):
        save_state(state)  # keep local cache in sync

    unconfigured = [li for li in line_items if li["id"] not in li_types]
    if unconfigured:
        st.warning(
            f"{len(unconfigured)} line item(s) not yet categorized. "
            "Please set a Type for each below and click **Save Configuration**."
        )

    st.markdown("Set the type for each line item. **Mileage** items are billed "
                "at unit price × miles delivered. **Web Subscription** and "
                "**Data Collection** items can be toggled per invoice.")

    updated_types = {}
    for li in line_items:
        props    = li.get("properties", {})
        li_id    = li["id"]
        name     = props.get("name") or li_id
        qty      = float(props.get("quantity") or 1)
        unit_p   = get_unit_price(props, qty)
        total    = float(props.get("amount") or unit_p * qty)
        cur_type = li_types.get(li_id, "standard")
        cur_display = TYPE_DISPLAY.get(cur_type, "Standard")

        cols = st.columns([3, 1, 1, 1, 2])
        cols[0].write(f"**{name}**")
        cols[1].write(f"${unit_p:,.2f}")
        cols[2].write(f"×{qty:g}")
        cols[3].write(f"${total:,.0f}")
        chosen = cols[4].selectbox(
            "Type", LINE_ITEM_TYPES,
            index=LINE_ITEM_TYPES.index(cur_display),
            key=f"type_{li_id}",
            label_visibility="collapsed",
        )
        updated_types[li_id] = TYPE_KEY_MAP[chosen]

    if st.button("Save Configuration", type="primary"):
        li_types.update(updated_types)
        save_state(state)                         # local cache
        save_li_types_to_hubspot(updated_types)   # shared source of truth
        fetch_line_items_for_deal.clear()         # bust cache so others see it
        st.success("Configuration saved.")

# ── Tab 2: Create Invoice ──────────────────────────────────────────────────────
def render_create_invoice_tab(deal_id: str, line_items: list[dict], state: dict):
    li_types   = state["line_item_types"]
    miles_hist = state["miles_history"]

    # Seed li_types from HubSpot for any items not in local state
    if seed_li_types_from_hubspot(line_items, li_types):
        save_state(state)

    # Fetch deal stats
    try:
        invoices = fetch_invoices_for_deal(deal_id)
        _, paid, billed, inv_count = aggregate_invoices(invoices)
    except Exception as e:
        st.error(f"Could not fetch invoices: {e}")
        paid = billed = 0.0

    # Fetch per-line-item invoiced amounts from HubSpot (source of truth)
    try:
        invoiced = fetch_invoiced_amounts_for_deal(deal_id)
    except Exception as e:
        st.warning(f"Could not load prior invoice line items: {e}")
        invoiced = {}

    deal_amount = st.session_state.get("selected_deal_amount", 0.0)
    remaining   = deal_amount - paid

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Deal Amount",  f"${deal_amount:,.0f}")
    c2.metric("Total Billed", f"${billed:,.0f}")
    c3.metric("Total Paid",   f"${paid:,.0f}")
    c4.metric("Remaining",    f"${remaining:,.0f}")

    st.divider()
    st.subheader("Invoice Builder")

    unconfigured = [li for li in line_items if li["id"] not in li_types]
    if unconfigured:
        st.info("Some line items are uncategorized — they will be treated as Standard. "
                "Go to **Configure Line Items** to set their types.")

    if st.button("Bill All Remaining", key=f"bill_all_btn_{deal_id}"):
        for li in line_items:
            _id   = li["id"]
            _type = li_types.get(_id, "standard")
            if _type == "mileage":
                st.session_state[f"mi_all_{_id}"] = True
            elif _type not in ("subscription", "data"):
                st.session_state[f"std_all_{_id}"] = True
        st.rerun()

    invoice_lines = []

    for li in line_items:
        props   = li.get("properties", {})
        li_id   = li["id"]
        name    = props.get("name") or li_id
        qty     = float(props.get("quantity") or 1)
        unit_p  = get_unit_price(props, qty)
        li_type = li_types.get(li_id, "standard")
        inv_data = invoiced.get(name.strip(), {})

        if li_type == "mileage":
            # Use HubSpot invoice quantities as source of truth for miles billed
            billed_miles = inv_data.get("quantity", 0.0)
            contracted   = qty
            remaining_m  = contracted - billed_miles
            st.markdown(f"**{name}** — Mileage  (${unit_p:,.4f}/mile)")
            st.caption(
                f"Contracted: {contracted:g} mi  ·  "
                f"Billed to date: {billed_miles:g} mi  ·  "
                f"Remaining: {remaining_m:g} mi"
            )
            mi_all = st.checkbox(
                "Bill all remaining",
                value=(remaining_m > 0),
                key=f"mi_all_{li_id}",
            )
            if mi_all:
                miles_input = remaining_m
            else:
                miles_input = st.number_input(
                    "Miles to bill this invoice",
                    min_value=0.0, max_value=float(max(remaining_m, 0)),
                    value=0.0, step=1.0, key=f"miles_{li_id}"
                )
            calc_amount = round(unit_p * miles_input)
            st.write(f"→ {miles_input:g} mi  ·  **${calc_amount:,.0f}**")
            if miles_input > 0:
                invoice_lines.append({
                    "li_id":       li_id,
                    "li_type":     "mileage",
                    "name":        name,
                    "unit_price":  unit_p,
                    "quantity":    miles_input,
                    "amount":      calc_amount,
                    "description": props.get("description", ""),
                    "miles":       miles_input,
                })

        elif li_type in ("subscription", "data"):
            label_type  = "Web Subscription" if li_type == "subscription" else "Data Collection"
            freq_label  = "/yr" if li_type == "subscription" else "/mo"
            prev_count  = inv_data.get("count", 0)
            prev_amt    = inv_data.get("amount", 0.0)
            caption     = (f"Previously invoiced {prev_count}×  ·  ${prev_amt:,.0f} total"
                           if prev_count else "Not yet invoiced")
            include = st.toggle(
                f"Include: **{name}** — ${unit_p:,.2f}{freq_label}  ({label_type})",
                value=True, key=f"toggle_{li_id}"
            )
            st.caption(caption)
            if include:
                invoice_lines.append({
                    "li_id":       li_id,
                    "li_type":     li_type,
                    "name":        name,
                    "unit_price":  unit_p,
                    "quantity":    1,
                    "amount":      unit_p,
                    "description": props.get("description", ""),
                    "miles":       0,
                })

        else:  # standard
            contract_total = float(props.get("amount") or unit_p * qty)
            billed_qty     = inv_data.get("quantity", 0.0)
            prev_amt       = inv_data.get("amount", 0.0)
            remaining_qty  = max(qty - billed_qty, 0.0)
            remaining_amt  = max(contract_total - prev_amt, 0.0)

            st.markdown(f"**{name}** — Standard  (${unit_p:,.4f}/unit)")
            st.caption(
                f"Contract: {qty:g} unit(s) · ${contract_total:,.0f}  ·  "
                f"Billed: {billed_qty:g} · ${prev_amt:,.0f}  ·  "
                f"Remaining: {remaining_qty:g} · ${remaining_amt:,.0f}"
            )

            bill_all = st.checkbox(
                "Bill all remaining",
                value=(remaining_qty > 0),
                key=f"std_all_{li_id}",
            )
            if bill_all:
                bill_qty = remaining_qty
            else:
                bill_qty = st.number_input(
                    "Units to bill",
                    min_value=0.0,
                    max_value=float(max(remaining_qty, 0)),
                    value=0.0,
                    step=1.0,
                    key=f"std_qty_{li_id}",
                )
            bill_amt = round(unit_p * bill_qty)
            st.write(f"→ {bill_qty:g} unit(s)  ·  **${bill_amt:,.0f}**")

            if bill_qty > 0:
                invoice_lines.append({
                    "li_id":       li_id,
                    "li_type":     "standard",
                    "name":        name,
                    "unit_price":  unit_p,
                    "quantity":    bill_qty,
                    "amount":      bill_amt,
                    "description": props.get("description", ""),
                    "miles":       0,
                })

    st.divider()
    invoice_total = sum(ln["amount"] for ln in invoice_lines)
    st.metric("Invoice Total", f"${invoice_total:,.0f}")

    due_date = st.date_input("Due Date", value=date.today() + timedelta(days=30))

    confirm_key = f"invoice_confirm_{deal_id}"

    if not st.session_state.get(confirm_key):
        # ── Step 1: Review button ──────────────────────────────────────────────
        if st.button("Review Invoice →", type="primary", disabled=(not invoice_lines)):
            if invoice_total <= 0:
                st.warning("Invoice total is $0 — add at least one item.")
            else:
                st.session_state[confirm_key] = {
                    "lines":    invoice_lines,
                    "due_date": due_date,
                    "total":    invoice_total,
                }
                st.rerun()
    else:
        # ── Step 2: Confirmation screen ────────────────────────────────────────
        pending = st.session_state[confirm_key]
        st.subheader("Confirm Invoice")

        # Line items summary table
        confirm_rows = []
        for ln in pending["lines"]:
            qty_label = (f"{ln['quantity']:g} mi" if ln["li_type"] == "mileage"
                         else f"{ln['quantity']:g}")
            confirm_rows.append({
                "Line Item":   ln["name"],
                "Type":        TYPE_DISPLAY.get(ln["li_type"], ln["li_type"].title()),
                "Unit Price":  f"${ln['unit_price']:,.4f}",
                "Qty":         qty_label,
                "Amount":      f"${ln['amount']:,.0f}",
            })
        st.dataframe(confirm_rows, use_container_width=True, hide_index=True)

        c1, c2 = st.columns(2)
        c1.metric("Invoice Total", f"${pending['total']:,.0f}")
        c2.metric("Due Date", str(pending["due_date"]))

        memo = st.text_area("Memo / notes", value=pending.get("memo", ""),
                            placeholder="Optional notes to include on the invoice")
        if memo != pending.get("memo", ""):
            pending["memo"] = memo
            st.session_state[confirm_key] = pending

        st.divider()
        publish = st.toggle(
            "Publish & send invoice email",
            value=False,
            help=(
                "Sets the invoice status to Open in HubSpot and returns a "
                "shareable link. Without this the invoice is saved as Draft."
            ),
        )

        col_back, col_confirm = st.columns([1, 2])

        if col_back.button("← Edit"):
            st.session_state.pop(confirm_key, None)
            st.rerun()

        if col_confirm.button("Confirm & Create Invoice", type="primary"):
            hs_lines = [
                {"name": ln["name"], "unit_price": ln["unit_price"],
                 "quantity": ln["quantity"], "description": ln["description"]}
                for ln in pending["lines"]
            ]
            try:
                inv_id, inv_link, hs_number = create_invoice_in_hubspot(
                    deal_id, hs_lines, pending["due_date"], publish=publish,
                    memo=pending.get("memo", "")
                )

                # Record miles history
                today_str = date.today().isoformat()
                for ln in pending["lines"]:
                    if ln["li_type"] == "mileage" and ln["miles"] > 0:
                        deal_hist = miles_hist.setdefault(deal_id, {})
                        deal_hist.setdefault(ln["li_id"], []).append({
                            "invoice_id": inv_id,
                            "miles":      ln["miles"],
                            "date":       today_str,
                            "amount":     ln["amount"],
                        })
                save_state(state)

                fetch_invoices_for_deal.clear()
                fetch_invoiced_amounts_for_deal.clear()
                fetch_closed_won_deals.clear()
                st.session_state.pop(confirm_key, None)

                num_label = f" **{hs_number}**" if hs_number else f" `{inv_id}`"
                if publish and inv_link:
                    st.success(f"Invoice{num_label} published!")
                    st.code(inv_link)
                    st.link_button("Open Invoice", inv_link)
                else:
                    status_note = "published as Open" if publish else "saved as Draft"
                    st.success(f"Invoice{num_label} created ({status_note})!")

            except requests.HTTPError as e:
                st.error(f"HubSpot API error: {e}")
            except Exception as e:
                st.error(f"Unexpected error: {e}")

# ── Tab 3: Invoice History ─────────────────────────────────────────────────────
def render_history_tab(deal_id: str, state: dict):
    try:
        invoices = fetch_invoices_for_deal(deal_id)
    except Exception as e:
        st.error(f"Could not fetch invoices: {e}")
        return

    _STATUS_BADGE = {
        "OVERDUE": "🔴 OVERDUE",
        "PAID":    "✅ PAID",
        "OPEN":    "📬 OPEN",
        "DRAFT":   "📄 DRAFT",
        "VOID":    "🚫 VOID",
    }
    today_str = date.today().isoformat()

    st.subheader("HubSpot Invoices")
    if not invoices:
        st.info("No invoices found for this deal.")
    else:
        rows = []
        for inv in invoices:
            p      = inv.get("properties", {})
            status = (p.get("hs_invoice_status") or "").upper()
            due_date_str = (p.get("hs_due_date") or "")[:10]
            # Flag overdue by date even if status not yet updated
            if status == "OPEN" and due_date_str and due_date_str < today_str:
                display_status = "🔴 OVERDUE"
            else:
                display_status = _STATUS_BADGE.get(status, status) or status
            inv_link = p.get("hs_invoice_link", "")
            rows.append({
                "Invoice ID": inv["id"],
                "Number":     p.get("hs_number", ""),
                "Status":     display_status,
                "Amount":     f"${float(p.get('hs_amount_billed') or 0):,.0f}",
                "Created":    (p.get("hs_createdate") or "")[:10],
                "Due":        due_date_str,
                "Link":       inv_link,
            })
        st.dataframe(
            rows,
            use_container_width=True,
            column_config={"Link": st.column_config.LinkColumn("Link")},
        )

    # Miles history breakdown
    deal_history = state["miles_history"].get(deal_id, {})
    if deal_history:
        st.subheader("Miles Billing History")
        for li_id, entries in deal_history.items():
            if entries:
                st.markdown(f"**Line Item ID: {li_id}**")
                mile_rows = [
                    {"Invoice ID": e["invoice_id"], "Miles": e["miles"],
                     "Amount": f"${e['amount']:,.0f}", "Date": e["date"]}
                    for e in entries
                ]
                st.dataframe(mile_rows, use_container_width=True)

# ── Draft invoice row actions ─────────────────────────────────────────────────
def _render_draft_invoice_row(draft: dict):
    """Renders one draft invoice row with edit / publish / delete controls."""
    inv_id   = draft["id"]
    edit_key = f"draft_edit_{inv_id}"
    del_key  = f"draft_del_{inv_id}"

    # Show persistent link if invoice was just published
    pub_link_key = f"published_link_{inv_id}"
    if st.session_state.get(pub_link_key):
        st.code(st.session_state[pub_link_key])

    today_iso = date.today().isoformat()
    due_display = draft["due_date"] or "—"
    if draft["due_date"] and draft["due_date"] < today_iso:
        due_display += " ⚠️"

    cols = st.columns([1.0, 1.4, 1.0, 1.1, 0.7, 0.85, 0.65])
    cols[0].write(draft["number"] or inv_id[:10])
    cols[1].write(due_display)
    cols[2].write(f"${draft['amount']:,.0f}")
    cols[3].write(draft["created"][:10] if draft["created"] else "—")

    if cols[4].button("Edit", key=f"edit_btn_{inv_id}"):
        st.session_state[edit_key] = not st.session_state.get(edit_key, False)
        st.session_state.pop(del_key, None)

    if cols[5].button("Publish", key=f"pub_btn_{inv_id}"):
        try:
            r = requests.patch(
                f"{BASE_URL}/crm/v3/objects/invoices/{inv_id}",
                headers=HEADERS,
                json={"properties": {"hs_invoice_status": "open"}},
            )
            raise_for_status(r, f"publish invoice {inv_id}")
            # Fetch the share link
            lr = requests.get(
                f"{BASE_URL}/crm/v3/objects/invoices/{inv_id}",
                headers=HEADERS, params={"properties": "hs_invoice_link"},
            )
            link = lr.json().get("properties", {}).get("hs_invoice_link", "")
            fetch_all_draft_invoices.clear()
            fetch_invoices_for_deal.clear()
            fetch_invoiced_amounts_for_deal.clear()
            if link:
                st.session_state[f"published_link_{inv_id}"] = link
            st.rerun()
        except Exception as e:
            st.error(str(e))

    if cols[6].button("Delete", key=f"del_btn_{inv_id}"):
        st.session_state[del_key] = True
        st.session_state.pop(edit_key, None)

    # Line items in collapsible expander
    with st.expander("Line items", expanded=False):
        try:
            li = fetch_invoice_line_items(inv_id)
            if li:
                li_rows = [{
                    "Name":   ln["properties"].get("name", ""),
                    "Qty":    ln["properties"].get("quantity", ""),
                    "Price":  f"${float(ln['properties'].get('price') or 0):,.4f}",
                    "Amount": f"${float(ln['properties'].get('amount') or 0):,.0f}",
                } for ln in li]
                st.dataframe(li_rows, use_container_width=True, hide_index=True)
            else:
                st.caption("No line items.")
        except Exception as e:
            st.caption(f"Error loading line items: {e}")

    # Edit panel
    if st.session_state.get(edit_key):
        with st.container(border=True):
            cur_due = date.today() + timedelta(days=30)
            if draft["due_date"]:
                try:
                    cur_due = date.fromisoformat(draft["due_date"])
                except ValueError:
                    pass
            new_due = st.date_input("New due date", value=cur_due,
                                    key=f"due_input_{inv_id}")
            c1, c2 = st.columns(2)
            if c1.button("Save", key=f"save_btn_{inv_id}", type="primary"):
                try:
                    r = requests.patch(
                        f"{BASE_URL}/crm/v3/objects/invoices/{inv_id}",
                        headers=HEADERS,
                        json={"properties": {"hs_due_date": new_due.isoformat()}},
                    )
                    raise_for_status(r, "update due date")
                    fetch_all_draft_invoices.clear()
                    st.session_state.pop(edit_key, None)
                    st.success("Due date updated.")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))
            if c2.button("Cancel", key=f"cancel_edit_{inv_id}"):
                st.session_state.pop(edit_key, None)
                st.rerun()

    # Delete confirmation
    if st.session_state.get(del_key):
        with st.container(border=True):
            st.warning(
                f"Delete draft **{draft['number'] or inv_id}**? "
                "This cannot be undone."
            )
            c1, c2 = st.columns(2)
            if c1.button("Confirm Delete", key=f"del_confirm_{inv_id}", type="primary"):
                try:
                    r = requests.delete(
                        f"{BASE_URL}/crm/v3/objects/invoices/{inv_id}",
                        headers=HEADERS,
                    )
                    raise_for_status(r, "delete invoice")
                    fetch_all_draft_invoices.clear()
                    fetch_invoices_for_deal.clear()
                    fetch_invoiced_amounts_for_deal.clear()
                    st.session_state.pop(del_key, None)
                    st.success("Invoice deleted.")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))
            if c2.button("Cancel", key=f"cancel_del_{inv_id}"):
                st.session_state.pop(del_key, None)
                st.rerun()

    st.divider()


# ── Page: Draft invoices management ───────────────────────────────────────────
def render_drafts_view(deal_lookup: dict):
    try:
        drafts = fetch_all_draft_invoices()
    except Exception as e:
        st.error(f"Could not load draft invoices: {e}")
        return

    if not drafts:
        st.info("No draft invoices found.")
        return

    st.caption(f"{len(drafts)} draft invoice(s) across all deals")

    if st.button("Publish All Drafts", type="primary"):
        published = 0
        errors    = []
        for draft in drafts:
            try:
                r = requests.patch(
                    f"{BASE_URL}/crm/v3/objects/invoices/{draft['id']}",
                    headers=HEADERS,
                    json={"properties": {"hs_invoice_status": "open"}},
                )
                raise_for_status(r, f"publish {draft['id']}")
                published += 1
            except Exception as e:
                errors.append(str(e))
        fetch_all_draft_invoices.clear()
        fetch_invoices_for_deal.clear()
        fetch_invoiced_amounts_for_deal.clear()
        if errors:
            st.error(f"Published {published}, {len(errors)} failed:\n" + "\n".join(errors))
        else:
            st.success(f"Published {published} invoice(s).")
        st.rerun()

    # Group by deal
    by_deal: dict[str, list] = {}
    for d in drafts:
        by_deal.setdefault(d["deal_id"], []).append(d)

    # Column headers (rendered once per deal group)
    _DRAFT_HEADERS = ["#", "Due Date", "Amount", "Created", "Edit", "Publish", "Delete"]

    for deal_id, deal_drafts in by_deal.items():
        deal_obj  = deal_lookup.get(deal_id, {})
        deal_name = (deal_obj.get("properties", {}).get("dealname")
                     if deal_obj else None) or f"Deal {deal_id}"

        with st.expander(f"**{deal_name}** — {len(deal_drafts)} draft(s)", expanded=True):
            hcols = st.columns([1.0, 1.4, 1.0, 1.1, 0.7, 0.85, 0.65])
            for col, lbl in zip(hcols, _DRAFT_HEADERS):
                col.markdown(f"**{lbl}**")
            st.divider()
            for draft in deal_drafts:
                _render_draft_invoice_row(draft)


# ── DeepWalk API: mileage fetch ────────────────────────────────────────────────
_EXCLUDED_STAGES = {"archive", "rescan"}


def fetch_scan_miles_by_stage(project_id: int) -> dict:
    """Returns {stage: miles} for all scans in the project via DeepWalk API."""
    token = st.session_state.get("dw_token", "")
    resp  = requests.get(
        f"https://app.deepwalkresearch.com/api/project/{project_id}/mileage/detailed",
        headers={"Authorization": f"Bearer {token}"},
    )
    if resp.status_code == 401:
        # Token expired — clear and re-fetch on next run
        st.session_state.pop("dw_token", None)
        st.rerun()
    resp.raise_for_status()
    return {str(k): float(v) for k, v in resp.json().items()}


# ── Page: Scan Progress ────────────────────────────────────────────────────────
def render_scan_progress(deals: list[dict]):
    st.header("Scan Progress")

    if not DW_AUTH0_CLIENT_ID or not DW_AUTH0_CLIENT_SECRET:
        st.warning(
            "DeepWalk API credentials not configured. "
            "Add **DEEPWALK_AUTH0_CLIENT_ID**, **DEEPWALK_AUTH0_CLIENT_SECRET**, **DEEPWALK_AUTH0_AUDIENCE**, and **DEEPWALK_AUTH0_DOMAIN** to your .env file."
        )
        return

    deals_with_project = [
        d for d in deals
        if (d.get("properties", {}).get("project_id") or "").strip()
    ]

    if not deals_with_project:
        st.info("No closed-won deals have a DeepWalk Project ID set.")
        return

    st.caption(f"{len(deals_with_project)} deal(s) with a DeepWalk Project ID")

    for d in deals_with_project:
        p              = d.get("properties", {})
        deal_name      = p.get("dealname") or d["id"]
        project_id_str = (p.get("project_id") or "").strip()

        try:
            project_id = int(project_id_str)
        except (ValueError, TypeError):
            continue

        with st.expander(f"**{deal_name}** — Project #{project_id}", expanded=True):
            try:
                with st.spinner("Loading scan data…"):
                    miles_by_stage = fetch_scan_miles_by_stage(project_id)
            except Exception as e:
                st.warning(f"Could not load scan data: {e}")
                c1, c2, c3 = st.columns(3)
                c1.metric("Collected",  "—")
                c2.metric("In Progress", "—")
                c3.metric("Completed",  "—")
                continue

            if not miles_by_stage:
                st.info("No scan data found for this project.")
                continue

            visible = {k: v for k, v in miles_by_stage.items() if k not in _EXCLUDED_STAGES}
            collected_miles  = sum(visible.values())
            completed_miles  = visible.get("passed", 0.0)
            in_progress_miles = collected_miles - completed_miles

            c1, c2, c3 = st.columns(3)
            c1.metric("Collected",   f"{collected_miles:.2f} mi")
            c2.metric("In Progress", f"{in_progress_miles:.2f} mi")
            pct = f"{completed_miles / collected_miles * 100:.0f}% of collected" if collected_miles else None
            c3.metric("Completed",   f"{completed_miles:.2f} mi", delta=pct)

            with st.expander("Stage breakdown", expanded=False):
                rows = [
                    {
                        "Stage":    stage,
                        "Miles":    f"{miles:.2f}",
                        "Category": "Completed" if stage == "passed" else "In Progress",
                    }
                    for stage, miles in sorted(visible.items(), key=lambda x: -x[1])
                ]
                st.dataframe(rows, use_container_width=True, hide_index=True)


# ── Main app ───────────────────────────────────────────────────────────────────
def main():
    st.set_page_config(page_title="Accounts Receivable", page_icon="💰", layout="wide")

    if not HUBSPOT_TOKEN:
        st.error("HUBSPOT_ACCESS_TOKEN not set in .env — cannot connect to HubSpot.")
        st.stop()

    # ── Step 1: DeepWalk token (client credentials — no browser redirect) ────────
    if DW_AUTH0_CLIENT_ID and not st.session_state.get("dw_token"):
        try:
            st.session_state["dw_token"] = fetch_dw_token()
        except Exception as e:
            st.error(f"Could not obtain DeepWalk API token: {e}")
            st.stop()

    # ── Step 2: HubSpot OAuth gate ────────────────────────────────────────────
    if OAUTH_CLIENT_ID:
        params = st.query_params

        # Handle redirect back from HubSpot with auth code
        if "code" in params and not params.get("state", "").startswith("dw-") \
                and not st.session_state.get("authenticated"):
            code           = params["code"]
            returned_state = params.get("state", "")

            if returned_state != _oauth_state():
                st.error("Invalid state parameter — possible CSRF attempt. Please try again.")
                st.query_params.clear()
                st.stop()

            try:
                with st.spinner("Signing you in…"):
                    tokens    = oauth_exchange_code(code)
                    user_info = oauth_get_user_info(tokens["access_token"])

                hub_id = str(user_info.get("hub_id", ""))
                if ALLOWED_PORTAL_ID and hub_id != ALLOWED_PORTAL_ID:
                    st.error(
                        f"Access denied: your HubSpot portal ({hub_id}) is not "
                        f"authorized for this app (expected {ALLOWED_PORTAL_ID})."
                    )
                    st.stop()

                st.session_state["authenticated"] = True
                st.session_state["user_email"]    = user_info.get("user", "")
                st.session_state["hub_id"]        = hub_id
                st.query_params.clear()
                st.rerun()

            except Exception as e:
                st.error(f"Authentication failed: {e}")
                st.query_params.clear()
                st.stop()

        if not st.session_state.get("authenticated"):
            render_login_page()
            st.stop()
    # Dev mode: no CLIENT_IDs set → skip auth (local use with .env token only)

    state = load_state()

    # ── Sidebar ──────────────────────────────────────────────────────────────
    with st.sidebar:
        st.title("💰 Accounts Receivable")
        if OAUTH_CLIENT_ID and st.session_state.get("user_email"):
            st.caption(f"Signed in as {st.session_state['user_email']}")
            if st.button("Sign out"):
                for k in ("authenticated", "user_email", "hub_id", "dw_token"):
                    st.session_state.pop(k, None)
                st.rerun()
        else:
            st.caption("HubSpot Closed-Won Deals")
        st.divider()

        if st.button("🔄 Refresh from HubSpot"):
            fetch_closed_won_deals.clear()
            fetch_line_items_for_deal.clear()
            fetch_invoices_for_deal.clear()
            fetch_invoiced_amounts_for_deal.clear()
            fetch_all_draft_invoices.clear()
            fetch_invoice_line_items.clear()
            st.rerun()

        st.divider()

        current_view = st.session_state.get("view", "summary")

        if current_view == "deal":
            if st.button("← Back to all deals"):
                st.session_state.pop("selected_deal_id", None)
                st.session_state.pop("selected_deal_amount", None)
                st.session_state["view"] = "summary"
                st.rerun()

        # Nav buttons — highlight the active view
        if st.button("📊 Dashboard", type="primary" if current_view == "dashboard" else "secondary"):
            st.session_state["view"] = "dashboard"
            st.session_state.pop("selected_deal_id", None)
            st.rerun()

        if st.button("📋 Deals", type="primary" if current_view in ("summary","deal") else "secondary"):
            st.session_state["view"] = "summary"
            st.session_state.pop("selected_deal_id", None)
            st.rerun()

        # Show draft count badge on the button
        try:
            draft_count = len(fetch_all_draft_invoices())
        except Exception:
            draft_count = 0
        draft_label = f"📝 Drafts ({draft_count})" if draft_count else "📝 Drafts"
        if st.button(draft_label, type="primary" if current_view == "drafts" else "secondary"):
            st.session_state["view"] = "drafts"
            st.session_state.pop("selected_deal_id", None)
            st.rerun()

        if st.button("⚙️ Line Item Types",
                     type="primary" if current_view == "li_config" else "secondary"):
            st.session_state["view"] = "li_config"
            st.session_state.pop("selected_deal_id", None)
            st.rerun()

        if st.button("🗺️ Scan Progress",
                     type="primary" if current_view == "scan_progress" else "secondary"):
            st.session_state["view"] = "scan_progress"
            st.session_state.pop("selected_deal_id", None)
            st.rerun()

    with st.spinner("Loading deals..."):
        try:
            deals = fetch_closed_won_deals()
        except Exception as e:
            st.error(f"Failed to load deals: {e}")
            deals = []

    deal_lookup = {d["id"]: d for d in deals}

    # ── Main area ─────────────────────────────────────────────────────────────
    view        = st.session_state.get("view", "summary")
    selected_id = st.session_state.get("selected_deal_id")

    # Sync view state with selected deal
    if selected_id and view != "deal":
        st.session_state["view"] = "deal"
        view = "deal"

    if view == "dashboard":
        render_dashboard(deals)
        return

    if view == "li_config":
        render_global_line_item_config(deals, state)
        return

    if view == "scan_progress":
        render_scan_progress(deals)
        return

    if view == "drafts":
        st.header("Draft Invoices")
        render_drafts_view(deal_lookup)
        return

    if not selected_id or view == "summary":
        render_summary(deals, state)
        return

    # Validate the deal still exists in the fetched list
    if selected_id not in deal_lookup:
        st.session_state.pop("selected_deal_id", None)
        st.session_state["view"] = "summary"
        render_summary(deals, state)
        return

    d = deal_lookup[selected_id]
    deal_name = d.get("properties", {}).get("dealname") or f"Deal {selected_id}"
    st.header(f"Deal: {deal_name}")

    st.session_state.pop("goto_invoice_tab", False)  # consumed; Create Invoice is now default tab

    with st.spinner("Loading line items..."):
        try:
            line_items = fetch_line_items_for_deal(selected_id)
        except Exception as e:
            st.error(f"Failed to load line items: {e}")
            line_items = []

    if not line_items:
        st.warning("No line items found for this deal.")

    tab1, tab2, tab3 = st.tabs(["Create Invoice", "Invoice History", "Configure Line Items"])

    with tab1:
        if line_items:
            render_create_invoice_tab(selected_id, line_items, state)
        else:
            st.info("No line items found. Cannot create invoice.")

    with tab2:
        render_history_tab(selected_id, state)

    with tab3:
        st.caption("Override the globally-configured type for individual line items on this deal.")
        if line_items:
            render_configure_tab(selected_id, line_items, state)
        else:
            st.info("No line items to configure.")


if __name__ == "__main__":
    main()
