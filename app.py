import io
from datetime import date

import pandas as pd
import streamlit as st

# ---------- CONFIG ----------
MRC_RATE = 1.42

# Adjust these to match your actual column names in each file
COLS = {
    "iccid": "ICCID",
    "vin": "VIN",
    "meid": "MEID",
    "mdn": "MDN",
    "brand": "BRAND",
    "use_purpose": "USE_PURPOSE",
    "device_group": "DEVICE_GROUP_NAME",  # if exists
    # Usage file columns:
    "data_iccid": "ICCID",
    "data_roaming": "Roaming Zone",
    "data_volume": "Data Volume (MB)",
    "sms_iccid": "ICCID",
    "sms_roaming": "Roaming Zone",
    "sms_volume": "SMS Volume (msg)",
    "voice_iccid": "ICCID",
    "voice_roaming": "Roaming Zone",
    "voice_volume": "Voice Volume",  # e.g. "12:00"
}

CANDIDATES = {
    "iccid": [
        "ICCID",
        "ICCID Number",
        "SIM ICCID",
    ],
    "roaming": [
        "Roaming Zone",
        "Roaming",
        "Roam",
        "Domestic/International",
    ],
    "data_volume": [
        "Data Volume (MB)",
        "Data Volume",
        "Data Usage",
        "Usage (MB)",
        "Total Data (MB)",
    ],
    "sms_volume": [
        "SMS Volume (msg)",
        "SMS Volume",
        "SMS Usage",
        "Messages",
        "Total Messages",
    ],
    "voice_volume": [
        "Voice Volume (m:ss)",
        "Included Voice (m:ss)",
        "Voice Volume",
        "Voice Usage",
        "Voice Minutes",
        "Minutes",
        "Total Minutes",
        "Usage (Min)",
        "Voice",
        "Total Voice",
    ],
}

def pick_first_existing_col(df: pd.DataFrame, candidates: list[str], label: str) -> str:
    cols = list(df.columns)
    colset = set(cols)
    for c in candidates:
        if c in colset:
            return c
    raise KeyError(f"Could not find {label} column. Available columns: {cols}")


# ---------- HELPER FUNCTIONS ----------

def apply_common_device_filters(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Keep USE_PURPOSE in (1, 2)
    - Brand rules:
        - 'G' / 'Genesis' -> Genesis
        - 'H' / 'Hyundai' or blank -> Hyundai
    - Drop rows with no ICCID
    - Drop test devices if DEVICE_GROUP_NAME contains TEST
    """
    df = df.copy()

    # Only valid ICCID rows
    df = df[df[COLS["iccid"]].notna()]

    # USE_PURPOSE filter
    if COLS["use_purpose"] in df.columns:
        df = df[df[COLS["use_purpose"]].isin([1, 2])]

    # Brand mapping
    brand = (
        df[COLS["brand"]]
        .fillna("H")
        .astype(str)
        .str.strip()
        .str.upper()
    )
    service = brand.replace({
        "G": "Genesis",
        "GENESIS": "Genesis",
        "H": "Hyundai",
        "HYUNDAI": "Hyundai",
        "": "Hyundai",
    })
    df["Service"] = service

    # Filter out test devices if column exists
    if COLS["device_group"] in df.columns:
        mask = ~df[COLS["device_group"]].astype(str).str.upper().str.contains("TEST")
        df = df[mask]

    return df


def normalize_sms_usage(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    col = COLS["sms_volume"]
    df[col] = (
        df[col]
        .astype(str)
        .str.replace(",", "")
        .str.strip()
        .replace("", "0")
        .astype(float)
    )
    return df


def normalize_voice_usage(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # pick correct columns from the real file
    iccid_col = COLS["voice_iccid"] if COLS["voice_iccid"] in df.columns else pick_first_existing_col(
        df, CANDIDATES["iccid"], "ICCID (voice usage)"
    )
    roaming_col = COLS["voice_roaming"] if COLS["voice_roaming"] in df.columns else pick_first_existing_col(
        df, CANDIDATES["roaming"], "Roaming (voice usage)"
    )
    vol_col = COLS["voice_volume"] if COLS["voice_volume"] in df.columns else pick_first_existing_col(
        df, CANDIDATES["voice_volume"], "Voice volume/minutes"
    )

    df[COLS["voice_iccid"]] = df[iccid_col]
    df[COLS["voice_roaming"]] = df[roaming_col]

    s = df[vol_col].astype(str).str.strip()

    # Convert m:ss to decimal minutes (12:30 -> 12.5)
    has_colon = s.str.contains(":", na=False)

    minutes_plain = pd.to_numeric(s.where(~has_colon), errors="coerce").fillna(0)

    parts = s.where(has_colon, "")
    mm = pd.to_numeric(parts.str.split(":", expand=True)[0], errors="coerce").fillna(0)
    ss = pd.to_numeric(parts.str.split(":", expand=True)[1], errors="coerce").fillna(0)
    minutes_colon = mm + (ss / 60.0)

    df[COLS["voice_volume"]] = minutes_plain.where(~has_colon, minutes_colon).astype(float)
    return df



def normalize_data_usage(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    iccid_col = COLS["data_iccid"] if COLS["data_iccid"] in df.columns else pick_first_existing_col(
        df, CANDIDATES["iccid"], "ICCID (data usage)"
    )
    roaming_col = COLS["data_roaming"] if COLS["data_roaming"] in df.columns else pick_first_existing_col(
        df, CANDIDATES["roaming"], "Roaming (data usage)"
    )
    vol_col = COLS["data_volume"] if COLS["data_volume"] in df.columns else pick_first_existing_col(
        df, CANDIDATES["data_volume"], "Data volume"
    )

    df[COLS["data_iccid"]] = df[iccid_col]
    df[COLS["data_roaming"]] = df[roaming_col]

    df[COLS["data_volume"]] = (
        df[vol_col]
        .astype(str)
        .str.replace(",", "")
        .str.strip()
        .replace("", "0")
        .astype(float)
    )

    return df



def build_setup_devices(hacc_excel: pd.ExcelFile, pre_rdr_check_df: pd.DataFrame) -> pd.DataFrame:
    # TODO: adjust sheet name if your Pre-RDR tab is named differently
    pre_rdr_hacc = pd.read_excel(hacc_excel, sheet_name="Pre-RDR")
    pre_rdr_hacc = apply_common_device_filters(pre_rdr_hacc)
    pre_rdr_check_df = apply_common_device_filters(pre_rdr_check_df)

    merged = pre_rdr_hacc.merge(
        pre_rdr_check_df[[COLS["iccid"]]].drop_duplicates(),
        on=COLS["iccid"],
        how="inner",
    )
    return merged


def build_ppu_devices(hacc_excel: pd.ExcelFile, enrolled_ppu_check_df: pd.DataFrame) -> pd.DataFrame:
    # TODO: adjust sheet name if your Enrolled-PPU tab is named differently
    ppu_hacc = pd.read_excel(hacc_excel, sheet_name="Enrolled-PPU")
    ppu_hacc = apply_common_device_filters(ppu_hacc)
    enrolled_ppu_check_df = apply_common_device_filters(enrolled_ppu_check_df)

    merged = ppu_hacc.merge(
        enrolled_ppu_check_df[[COLS["iccid"]]].drop_duplicates(),
        on=COLS["iccid"],
        how="inner",
    )
    return merged


def build_mrc_devices(hacc_excel: pd.ExcelFile) -> pd.DataFrame:
    # TODO: adjust sheet name if your MRC tab is named differently
    mrc_hacc = pd.read_excel(hacc_excel, sheet_name="MRC(H,G)-Enrolled(5,10,30,50,1)")
    mrc_hacc = apply_common_device_filters(mrc_hacc)
    return mrc_hacc


def build_tms_service_setup_rows(setup_devices: pd.DataFrame, bill_start, bill_end) -> pd.DataFrame:
    if setup_devices.empty:
        return pd.DataFrame()

    df = setup_devices.copy()
    bill = pd.DataFrame()
    bill["ICCID"] = df[COLS["iccid"]]
    bill["VIN"] = df[COLS["vin"]]
    bill["MEID/IMEI"] = df[COLS["meid"]]
    bill["MDN/MSISDN"] = df[COLS["mdn"]]
    bill["Service"] = df["Service"]
    bill["Record Type"] = "TMS Service Setup"
    bill["Bill Start Date"] = bill_start
    bill["Bill End Date"] = bill_end

    bill["Voice Roaming Zone"] = ""
    bill["SMS Roaming Zone"] = ""
    bill["Data Roaming Zone"] = ""
    bill["Voice Usage (Min)"] = 0
    bill["SMS Usage"] = 0
    bill["Data Usage (MB)"] = 0
    bill["Subscription Plan"] = ""
    bill["Subscription Charges"] = 0

    return bill


def build_usage_rows(
    base_devices: pd.DataFrame,
    usage_df: pd.DataFrame,
    iccid_col: str,
    roaming_col: str,
    usage_col: str,
    record_type_label: str,
    bill_start,
    bill_end,
    usage_type: str,
) -> pd.DataFrame:
    """
    Build rows for usage breakdown (Setup or PPU), split by roaming Yes/No.
    usage_type: "data", "voice", or "sms"
    """
    if base_devices.empty or usage_df.empty:
        return pd.DataFrame()

    usage = (
        usage_df
        .groupby([iccid_col, roaming_col], as_index=False)[usage_col]
        .sum()
    )

    merged = base_devices.merge(
        usage,
        left_on=COLS["iccid"],
        right_on=iccid_col,
        how="inner",
    )

    def roaming_flag(z):
        z = str(z).upper()
        if z in ["R", "ROAM", "ROAMING", "YES", "Y"]:
            return "Yes"
        return "No"

    merged["RoamingFlag"] = merged[roaming_col].apply(roaming_flag)

    rows = []
    for flag in ["No", "Yes"]:   # process NO first, then YES
        sub = merged[merged["RoamingFlag"] == flag]
        if sub.empty:
            continue

        tmp = pd.DataFrame()
        tmp["ICCID"] = sub[COLS["iccid"]]
        tmp["VIN"] = sub[COLS["vin"]]
        tmp["MEID/IMEI"] = sub[COLS["meid"]]
        tmp["MDN/MSISDN"] = sub[COLS["mdn"]]
        tmp["Service"] = sub["Service"]
        tmp["Record Type"] = record_type_label
        tmp["Bill Start Date"] = bill_start
        tmp["Bill End Date"] = bill_end

        tmp["Voice Roaming Zone"] = flag if usage_type == "voice" else ""
        tmp["SMS Roaming Zone"] = flag if usage_type == "sms" else ""
        tmp["Data Roaming Zone"] = flag if usage_type == "data" else ""

        tmp["Voice Usage (Min)"] = 0
        tmp["SMS Usage"] = 0
        tmp["Data Usage (MB)"] = 0

        if usage_type == "data":
            tmp["Data Usage (MB)"] = sub[usage_col].values
        elif usage_type == "voice":
            tmp["Voice Usage (Min)"] = sub[usage_col].values
        elif usage_type == "sms":
            tmp["SMS Usage"] = sub[usage_col].values

        tmp["Subscription Plan"] = ""
        tmp["Subscription Charges"] = 0

        rows.append(tmp)

    if rows:
        return pd.concat(rows, ignore_index=True)
    return pd.DataFrame()


def build_mrc_rows(mrc_devices: pd.DataFrame, bill_start, bill_end) -> pd.DataFrame:
    if mrc_devices.empty:
        return pd.DataFrame()

    df = mrc_devices.copy()
    bill = pd.DataFrame()
    bill["ICCID"] = df[COLS["iccid"]]
    bill["VIN"] = df[COLS["vin"]]
    bill["MEID/IMEI"] = df[COLS["meid"]]
    bill["MDN/MSISDN"] = df[COLS["mdn"]]
    bill["Service"] = df["Service"]
    bill["Record Type"] = "TMS Service Enrolled - MRC"
    bill["Bill Start Date"] = bill_start
    bill["Bill End Date"] = bill_end

    bill["Voice Roaming Zone"] = ""
    bill["SMS Roaming Zone"] = ""
    bill["Data Roaming Zone"] = ""
    bill["Voice Usage (Min)"] = 0
    bill["SMS Usage"] = 0
    bill["Data Usage (MB)"] = 0
    bill["Subscription Plan"] = ""
    bill["Subscription Charges"] = MRC_RATE

    return bill


def build_ppu_subscription_rows(ppu_devices: pd.DataFrame, bill_start, bill_end) -> pd.DataFrame:
    if ppu_devices.empty:
        return pd.DataFrame()

    df = ppu_devices.copy()
    bill = pd.DataFrame()
    bill["ICCID"] = df[COLS["iccid"]]
    bill["VIN"] = df[COLS["vin"]]
    bill["MEID/IMEI"] = df[COLS["meid"]]
    bill["MDN/MSISDN"] = df[COLS["mdn"]]
    bill["Service"] = df["Service"]
    bill["Record Type"] = "TMS Service Enrolled - Subscription"
    bill["Bill Start Date"] = bill_start
    bill["Bill End Date"] = bill_end

    bill["Voice Roaming Zone"] = ""
    bill["SMS Roaming Zone"] = ""
    bill["Data Roaming Zone"] = ""
    bill["Voice Usage (Min)"] = 0
    bill["SMS Usage"] = 0
    bill["Data Usage (MB)"] = 0
    bill["Subscription Plan"] = ""
    bill["Subscription Charges"] = 0

    return bill


def build_invoice_summary(bill_detail_df: pd.DataFrame) -> pd.DataFrame:
    if bill_detail_df.empty:
        return pd.DataFrame()

    df = bill_detail_df.copy()

    # MRC
    mrc_rows = df[df["Record Type"] == "TMS Service Enrolled - MRC"]
    mrc_count = len(mrc_rows)
    mrc_total = mrc_rows["Subscription Charges"].sum()

    # Usage breakdown rows (Setup + PPU)
    usage_rows = df[
        df["Record Type"].isin([
            "TMS Service Setup - Usage Breakdown",
            "TMS Service Enrolled (PPU) - Usage breakdown",
        ])
    ]

    total_data = usage_rows["Data Usage (MB)"].sum()
    total_voice = usage_rows["Voice Usage (Min)"].sum()
    total_sms = usage_rows["SMS Usage"].sum()

    summary = pd.DataFrame([
        {"Item": "MRC Devices", "Count/Amount": mrc_count},
        {"Item": "MRC Total Charges", "Count/Amount": mrc_total},
        {"Item": "Total Data Usage (MB)", "Count/Amount": total_data},
        {"Item": "Total Voice Usage (Min)", "Count/Amount": total_voice},
        {"Item": "Total SMS Usage", "Count/Amount": total_sms},
    ])

    return summary

def build_invoice_summary_from_bill_detail(bill_detail_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build invoice-summary numbers ONLY from Bill Detail.

    Rules:
    - # of Lines = unique ICCID count from base rows:
        * Setup: Record Type == "TMS Service Setup"
        * Enrolled (PPU): Record Type == "TMS Service Enrolled - Subscription"
        * Enrolled (MRC): Record Type == "TMS Service Enrolled - MRC"
    - Usage comes from usage breakdown rows:
        * Setup usage: "TMS Service Setup - Usage Breakdown"
        * PPU usage: "TMS Service Enrolled (PPU) - Usage breakdown"
      Split by roaming zone Yes/No.
    - Charges:
        * Setup data non-roaming charge = MB * 0.0026
        * Setup data roaming charge     = MB * 0.0053
        * Setup voice/sms charge = 0
        * PPU all usage charges = 0
        * MRC total charges = sum(Subscription Charges)
    """
    if bill_detail_df is None or bill_detail_df.empty:
        return pd.DataFrame()

    df = bill_detail_df.copy()

    # ---- Helpers ----
    def safe_series(s):
        return s.fillna("").astype(str).str.strip()

    def nunique_iccid(sub):
        if sub.empty:
            return 0
        return sub["ICCID"].nunique()

    def sum_usage(sub, col):
        if sub.empty or col not in sub.columns:
            return 0.0
        return float(pd.to_numeric(sub[col], errors="coerce").fillna(0).sum())

    # Normalize Service labels a bit
    if "Service" in df.columns:
        df["Service"] = safe_series(df["Service"]).replace({"H": "Hyundai", "G": "Genesis"})
    else:
        df["Service"] = "Hyundai"

    services = ["Hyundai", "Genesis"]

    # ---- Base rows for line counts ----
    setup_base = df[df["Record Type"] == "TMS Service Setup"]
    ppu_base   = df[df["Record Type"] == "TMS Service Enrolled - Subscription"]
    mrc_base   = df[df["Record Type"] == "TMS Service Enrolled - MRC"]

    # ---- Usage breakdown rows ----
    setup_usage = df[df["Record Type"] == "TMS Service Setup - Usage Breakdown"]
    ppu_usage   = df[df["Record Type"] == "TMS Service Enrolled (PPU) - Usage breakdown"]

    # ---- Rates ----
    SETUP_NR_DATA_RATE = 0.0026
    SETUP_R_DATA_RATE  = 0.0053

    rows = []

    for svc in services:
        # Line counts (unique ICCIDs)
        setup_lines = nunique_iccid(setup_base[setup_base["Service"] == svc])
        ppu_lines   = nunique_iccid(ppu_base[ppu_base["Service"] == svc])
        mrc_lines   = nunique_iccid(mrc_base[mrc_base["Service"] == svc])

        # MRC charges (subscription)
        mrc_sub = mrc_base[mrc_base["Service"] == svc]
        mrc_total_charges = float(pd.to_numeric(mrc_sub.get("Subscription Charges", 0), errors="coerce").fillna(0).sum())

        # ---- Setup usage splits ----
        su = setup_usage[setup_usage["Service"] == svc]

        su_data_nr = su[su.get("Data Roaming Zone", "") == "No"]
        su_data_r  = su[su.get("Data Roaming Zone", "") == "Yes"]
        su_sms_nr  = su[su.get("SMS Roaming Zone", "") == "No"]
        su_sms_r   = su[su.get("SMS Roaming Zone", "") == "Yes"]
        su_voice_nr= su[su.get("Voice Roaming Zone", "") == "No"]
        su_voice_r = su[su.get("Voice Roaming Zone", "") == "Yes"]

        setup_data_nr_mb = sum_usage(su_data_nr, "Data Usage (MB)")
        setup_data_r_mb  = sum_usage(su_data_r,  "Data Usage (MB)")
        setup_sms_nr_cnt = sum_usage(su_sms_nr,  "SMS Usage")
        setup_sms_r_cnt  = sum_usage(su_sms_r,   "SMS Usage")
        setup_voice_nr_m = sum_usage(su_voice_nr,"Voice Usage (Min)")
        setup_voice_r_m  = sum_usage(su_voice_r, "Voice Usage (Min)")

        setup_data_nr_charge = setup_data_nr_mb * SETUP_NR_DATA_RATE
        setup_data_r_charge  = setup_data_r_mb  * SETUP_R_DATA_RATE

        # ---- PPU usage splits (charges always 0) ----
        pu = ppu_usage[ppu_usage["Service"] == svc]

        pu_data_nr = pu[pu.get("Data Roaming Zone", "") == "No"]
        pu_data_r  = pu[pu.get("Data Roaming Zone", "") == "Yes"]
        pu_sms_nr  = pu[pu.get("SMS Roaming Zone", "") == "No"]
        pu_sms_r   = pu[pu.get("SMS Roaming Zone", "") == "Yes"]
        pu_voice_nr= pu[pu.get("Voice Roaming Zone", "") == "No"]
        pu_voice_r = pu[pu.get("Voice Roaming Zone", "") == "Yes"]

        ppu_data_nr_mb = sum_usage(pu_data_nr, "Data Usage (MB)")
        ppu_data_r_mb  = sum_usage(pu_data_r,  "Data Usage (MB)")
        ppu_sms_nr_cnt = sum_usage(pu_sms_nr,  "SMS Usage")
        ppu_sms_r_cnt  = sum_usage(pu_sms_r,   "SMS Usage")
        ppu_voice_nr_m = sum_usage(pu_voice_nr,"Voice Usage (Min)")
        ppu_voice_r_m  = sum_usage(pu_voice_r, "Voice Usage (Min)")

        # charges always 0 for PPU usage
        ppu_data_nr_charge = 0.0
        ppu_data_r_charge  = 0.0

        # ---- Build rows in the style of your Summary (Record Type + Record Description) ----
        # Setup - Data/SMS/Voice
        rows.append({
            "Service": svc,
            "Bill Cycle Month": df.get("Bill Cycle Month", pd.Series([""])).iloc[0] if "Bill Cycle Month" in df.columns else "",
            "Record Type": "TMS Service Setup",
            "Record Description": "Data (MB)",
            "# of Lines": setup_lines,
            "Non-Roaming Usage": setup_data_nr_mb,
            "Non-Roaming Charge": setup_data_nr_charge,
            "Roaming Usage": setup_data_r_mb,
            "Roaming Usage Charge": setup_data_r_charge,
            "Total Charges": setup_data_nr_charge + setup_data_r_charge,
        })
        rows.append({
            "Service": svc,
            "Bill Cycle Month": df.get("Bill Cycle Month", pd.Series([""])).iloc[0] if "Bill Cycle Month" in df.columns else "",
            "Record Type": "TMS Service Setup",
            "Record Description": "SMS",
            "# of Lines": setup_lines,
            "Non-Roaming Usage": setup_sms_nr_cnt,
            "Non-Roaming Charge": 0.0,
            "Roaming Usage": setup_sms_r_cnt,
            "Roaming Usage Charge": 0.0,
            "Total Charges": 0.0,
        })
        rows.append({
            "Service": svc,
            "Bill Cycle Month": df.get("Bill Cycle Month", pd.Series([""])).iloc[0] if "Bill Cycle Month" in df.columns else "",
            "Record Type": "TMS Service Setup",
            "Record Description": "Voice",
            "# of Lines": setup_lines,
            "Non-Roaming Usage": setup_voice_nr_m,
            "Non-Roaming Charge": 0.0,
            "Roaming Usage": setup_voice_r_m,
            "Roaming Usage Charge": 0.0,
            "Total Charges": 0.0,
        })

        # Enrolled (MRC) - Subscription Charge
        rows.append({
            "Service": svc,
            "Bill Cycle Month": df.get("Bill Cycle Month", pd.Series([""])).iloc[0] if "Bill Cycle Month" in df.columns else "",
            "Record Type": "TMS Service Enrolled (MRC)",
            "Record Description": "Subscription Charge",
            "# of Lines": mrc_lines,
            "Non-Roaming Usage": "",
            "Non-Roaming Charge": "",
            "Roaming Usage": "",
            "Roaming Usage Charge": "",
            "Total Charges": mrc_total_charges,
        })

        # Enrolled (PPU) - Data/SMS/Voice (charges always 0)
        rows.append({
            "Service": svc,
            "Bill Cycle Month": df.get("Bill Cycle Month", pd.Series([""])).iloc[0] if "Bill Cycle Month" in df.columns else "",
            "Record Type": "TMS Service Enrolled (PPU)",
            "Record Description": "Data (MB)",
            "# of Lines": ppu_lines,
            "Non-Roaming Usage": ppu_data_nr_mb,
            "Non-Roaming Charge": ppu_data_nr_charge,
            "Roaming Usage": ppu_data_r_mb,
            "Roaming Usage Charge": ppu_data_r_charge,
            "Total Charges": 0.0,
        })
        rows.append({
            "Service": svc,
            "Bill Cycle Month": df.get("Bill Cycle Month", pd.Series([""])).iloc[0] if "Bill Cycle Month" in df.columns else "",
            "Record Type": "TMS Service Enrolled (PPU)",
            "Record Description": "SMS",
            "# of Lines": ppu_lines,
            "Non-Roaming Usage": ppu_sms_nr_cnt,
            "Non-Roaming Charge": 0.0,
            "Roaming Usage": ppu_sms_r_cnt,
            "Roaming Usage Charge": 0.0,
            "Total Charges": 0.0,
        })
        rows.append({
            "Service": svc,
            "Bill Cycle Month": df.get("Bill Cycle Month", pd.Series([""])).iloc[0] if "Bill Cycle Month" in df.columns else "",
            "Record Type": "TMS Service Enrolled (PPU)",
            "Record Description": "Voice",
            "# of Lines": ppu_lines,
            "Non-Roaming Usage": ppu_voice_nr_m,
            "Non-Roaming Charge": 0.0,
            "Roaming Usage": ppu_voice_r_m,
            "Roaming Usage Charge": 0.0,
            "Total Charges": 0.0,
        })

    # Return as DataFrame (you can write this into the invoice template Summary tab)
    out = pd.DataFrame(rows)

    # Optional: ensure numeric columns are numeric where applicable
    for c in ["# of Lines", "Non-Roaming Usage", "Non-Roaming Charge", "Roaming Usage", "Roaming Usage Charge", "Total Charges"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="ignore")

    return out

from openpyxl import load_workbook

def write_invoice_from_bill_detail_to_template(
    bill_detail_df: pd.DataFrame,
    invoice_template_file,
    summary_sheet_name: str = "Summary",
) -> io.BytesIO:
    """
    Fill Invoice Summary tab using Bill Detail.
    Writes ONLY input cells (counts + usage).
    Leaves Excel formulas intact.
    """

    df = bill_detail_df.copy()

    # Normalize service
    df["Service"] = (
        df.get("Service", "Hyundai")
        .fillna("Hyundai")
        .astype(str).str.strip()
        .replace({"H": "Hyundai", "G": "Genesis"})
    )

    def uniq_lines(sub):
        return int(sub["ICCID"].nunique()) if not sub.empty else 0

    def sum_col(sub, col):
        if sub.empty or col not in sub.columns:
            return 0
        return float(pd.to_numeric(sub[col], errors="coerce").fillna(0).sum())

    setup_base = df[df["Record Type"] == "TMS Service Setup"]
    mrc_base   = df[df["Record Type"] == "TMS Service Enrolled - MRC"]
    ppu_base   = df[df["Record Type"] == "TMS Service Enrolled - Subscription"]

    setup_usage = df[df["Record Type"] == "TMS Service Setup - Usage Breakdown"]
    ppu_usage   = df[df["Record Type"] == "TMS Service Enrolled (PPU) - Usage breakdown"]

    def usage(svc, base, col, roam_col, flag):
        sub = base[(base["Service"] == svc) & (base[roam_col] == flag)]
        return sum_col(sub, col)

    wb = load_workbook(invoice_template_file)
    ws = wb[summary_sheet_name]

    def setc(cell, val):
        ws[cell].value = val

    # ---------------- HYUNDAI ----------------
    h_setup = uniq_lines(setup_base[setup_base["Service"] == "Hyundai"])
    for c in ["E4", "E5", "E6"]:
        setc(c, h_setup)

    setc("H4", usage("Hyundai", setup_usage, "Data Usage (MB)", "Data Roaming Zone", "No"))
    setc("K4", usage("Hyundai", setup_usage, "Data Usage (MB)", "Data Roaming Zone", "Yes"))
    setc("H5", usage("Hyundai", setup_usage, "SMS Usage", "SMS Roaming Zone", "No"))
    setc("K5", usage("Hyundai", setup_usage, "SMS Usage", "SMS Roaming Zone", "Yes"))
    setc("H6", usage("Hyundai", setup_usage, "Voice Usage (Min)", "Voice Roaming Zone", "No"))
    setc("K6", usage("Hyundai", setup_usage, "Voice Usage (Min)", "Voice Roaming Zone", "Yes"))

    setc("E7", uniq_lines(mrc_base[mrc_base["Service"] == "Hyundai"]))

    h_ppu = uniq_lines(ppu_base[ppu_base["Service"] == "Hyundai"])
    for c in ["E8", "E9", "E10"]:
        setc(c, h_ppu)

    setc("H8", usage("Hyundai", ppu_usage, "Data Usage (MB)", "Data Roaming Zone", "No"))
    setc("K8", usage("Hyundai", ppu_usage, "Data Usage (MB)", "Data Roaming Zone", "Yes"))
    setc("H9", usage("Hyundai", ppu_usage, "SMS Usage", "SMS Roaming Zone", "No"))
    setc("K9", usage("Hyundai", ppu_usage, "SMS Usage", "SMS Roaming Zone", "Yes"))
    setc("H10", usage("Hyundai", ppu_usage, "Voice Usage (Min)", "Voice Roaming Zone", "No"))
    setc("K10", usage("Hyundai", ppu_usage, "Voice Usage (Min)", "Voice Roaming Zone", "Yes"))

    # ---------------- GENESIS ----------------
    g_setup = uniq_lines(setup_base[setup_base["Service"] == "Genesis"])
    for c in ["E14", "E15", "E16"]:
        setc(c, g_setup)

    setc("H14", usage("Genesis", setup_usage, "Data Usage (MB)", "Data Roaming Zone", "No"))
    setc("K14", usage("Genesis", setup_usage, "Data Usage (MB)", "Data Roaming Zone", "Yes"))
    setc("H15", usage("Genesis", setup_usage, "SMS Usage", "SMS Roaming Zone", "No"))
    setc("K15", usage("Genesis", setup_usage, "SMS Usage", "SMS Roaming Zone", "Yes"))
    setc("H16", usage("Genesis", setup_usage, "Voice Usage (Min)", "Voice Roaming Zone", "No"))
    setc("K16", usage("Genesis", setup_usage, "Voice Usage (Min)", "Voice Roaming Zone", "Yes"))

    setc("E17", uniq_lines(mrc_base[mrc_base["Service"] == "Genesis"]))

    g_ppu = uniq_lines(ppu_base[ppu_base["Service"] == "Genesis"])
    for c in ["E18", "E19", "E20"]:
        setc(c, g_ppu)

    setc("H18", usage("Genesis", ppu_usage, "Data Usage (MB)", "Data Roaming Zone", "No"))
    setc("K18", usage("Genesis", ppu_usage, "Data Usage (MB)", "Data Roaming Zone", "Yes"))
    setc("H19", usage("Genesis", ppu_usage, "SMS Usage", "SMS Roaming Zone", "No"))
    setc("K19", usage("Genesis", ppu_usage, "SMS Usage", "SMS Roaming Zone", "Yes"))
    setc("H20", usage("Genesis", ppu_usage, "Voice Usage (Min)", "Voice Roaming Zone", "No"))
    setc("K20", usage("Genesis", ppu_usage, "Voice Usage (Min)", "Voice Roaming Zone", "Yes"))

    # Force Excel to recalc formulas on open
    wb.calculation.fullCalcOnLoad = True

    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    return out


def process_hacc_billing(
    hacc_file,
    pre_rdr_check_file,
    enrolled_ppu_check_file,
    data_usage_file,
    sms_usage_file,
    voice_usage_file,
    bill_start_date,
    bill_end_date,
):
    hacc_excel = pd.ExcelFile(hacc_file)
    pre_rdr_check = pd.read_excel(pre_rdr_check_file)
    enrolled_ppu_check = pd.read_excel(enrolled_ppu_check_file)

    setup_devices = build_setup_devices(hacc_excel, pre_rdr_check)
    ppu_devices = build_ppu_devices(hacc_excel, enrolled_ppu_check)
    mrc_devices = build_mrc_devices(hacc_excel)

    data_usage_raw = pd.read_excel(data_usage_file)
    sms_usage_raw = pd.read_excel(sms_usage_file)
    voice_usage_raw = pd.read_excel(voice_usage_file)

    data_usage = normalize_data_usage(data_usage_raw)
    sms_usage = normalize_sms_usage(sms_usage_raw)
    voice_usage = normalize_voice_usage(voice_usage_raw)

    # Setup base
    setup_rows = build_tms_service_setup_rows(setup_devices, bill_start_date, bill_end_date)

    # Setup Usage Breakdown
    setup_data_usage = build_usage_rows(
        setup_devices, data_usage,
        COLS["data_iccid"], COLS["data_roaming"], COLS["data_volume"],
        "TMS Service Setup - Usage Breakdown",
        bill_start_date, bill_end_date,
        usage_type="data",
    )
    setup_sms_usage = build_usage_rows(
        setup_devices, sms_usage,
        COLS["sms_iccid"], COLS["sms_roaming"], COLS["sms_volume"],
        "TMS Service Setup - Usage Breakdown",
        bill_start_date, bill_end_date,
        usage_type="sms",
    )
    setup_voice_usage = build_usage_rows(
        setup_devices, voice_usage,
        COLS["voice_iccid"], COLS["voice_roaming"], COLS["voice_volume"],
        "TMS Service Setup - Usage Breakdown",
        bill_start_date, bill_end_date,
        usage_type="voice",
    )

    # PPU Subscription base
    ppu_sub_rows = build_ppu_subscription_rows(ppu_devices, bill_start_date, bill_end_date)

    # PPU Usage Breakdown
    ppu_data_usage = build_usage_rows(
        ppu_devices, data_usage,
        COLS["data_iccid"], COLS["data_roaming"], COLS["data_volume"],
        "TMS Service Enrolled (PPU) - Usage breakdown",
        bill_start_date, bill_end_date,
        usage_type="data",
    )
    ppu_sms_usage = build_usage_rows(
        ppu_devices, sms_usage,
        COLS["sms_iccid"], COLS["sms_roaming"], COLS["sms_volume"],
        "TMS Service Enrolled (PPU) - Usage breakdown",
        bill_start_date, bill_end_date,
        usage_type="sms",
    )
    ppu_voice_usage = build_usage_rows(
        ppu_devices, voice_usage,
        COLS["voice_iccid"], COLS["voice_roaming"], COLS["voice_volume"],
        "TMS Service Enrolled (PPU) - Usage breakdown",
        bill_start_date, bill_end_date,
        usage_type="voice",
    )

    # MRC
    mrc_rows = build_mrc_rows(mrc_devices, bill_start_date, bill_end_date)

    bill_detail = pd.concat(
        [
            setup_rows,
            setup_data_usage,
            setup_sms_usage,
            setup_voice_usage,
            ppu_sub_rows,
            ppu_data_usage,
            ppu_sms_usage,
            ppu_voice_usage,
            mrc_rows,
        ],
        ignore_index=True,
    )

    invoice_summary = build_invoice_summary_from_bill_detail(bill_detail)

    return bill_detail, invoice_summary


# ---------- STREAMLIT UI ----------

st.title("HACC Billing Automation (Bill Detail + Invoice)")

st.write("Upload the required files and generate Bill Detail & Invoice Summary automatically.")

hacc_file = st.file_uploader("hacc_Sep25.xlsx", type=["xlsx"])
pre_rdr_check_file = st.file_uploader("pre-rdr_check_Sep25.xlsx", type=["xlsx"])
enrolled_ppu_check_file = st.file_uploader("enrolled-ppu_check_Sep25.xlsx", type=["xlsx"])
data_usage_file = st.file_uploader("HAC_Data.xlsx", type=["xlsx"])
sms_usage_file = st.file_uploader("HAC_SMS.xlsx", type=["xlsx"])
voice_usage_file = st.file_uploader("HAC_Voice.xlsx", type=["xlsx"])
invoice_template = st.file_uploader("Invoice Template (xlsx)", type=["xlsx"])


col1, col2 = st.columns(2)
with col1:
    bill_start = st.date_input("Bill Start Date", value=date(2025, 9, 1))
with col2:
    bill_end = st.date_input("Bill End Date", value=date(2025, 9, 30))

if st.button("Generate Bill Detail + Invoice"):

    missing = [
        name for name, f in [
            ("hacc_Sep25", hacc_file),
            ("pre-rdr_check_Sep25", pre_rdr_check_file),
            ("enrolled-ppu_check_Sep25", enrolled_ppu_check_file),
            ("HAC_Data", data_usage_file),
            ("HAC_SMS", sms_usage_file),
            ("HAC_Voice", voice_usage_file),
        ] if f is None
    ]

    if missing:
        st.error(f"Please upload: {', '.join(missing)}")
    else:
        bill_detail_df, invoice_summary_df = process_hacc_billing(
            hacc_file,
            pre_rdr_check_file,
            enrolled_ppu_check_file,
            data_usage_file,
            sms_usage_file,
            voice_usage_file,
            bill_start,
            bill_end,
        )

if invoice_template is not None:
    invoice_bytes = write_invoice_from_bill_detail_to_template(
        bill_detail_df=bill_detail_df,
        invoice_template_file=invoice_template,
        summary_sheet_name="Summary",
    )

    st.download_button(
        label="Download Invoice Output (Summary filled)",
        data=invoice_bytes,
        file_name="Invoice_Output.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.warning("Upload the Invoice Template to generate the Invoice output.")


        st.success("Processing complete!")

        st.subheader("Preview: Bill Detail (first 20 rows)")
        st.dataframe(bill_detail_df.head(20))

        st.subheader("Preview: Invoice Summary")
        st.dataframe(invoice_summary_df)

        # Export to Excel in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            bill_detail_df.to_excel(writer, sheet_name="Detail Bill", index=False)
            invoice_summary_df.to_excel(writer, sheet_name="Summary", index=False)
        output.seek(0)

        st.download_button(
            label="Download Bill Detail + Summary Excel",
            data=output,
            file_name="HACC_Billing_Output.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
