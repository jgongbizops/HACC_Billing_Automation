import io
from datetime import date

import pandas as pd
import streamlit as st

# ---------- CONFIG ----------
MRC_RATE = 1.42

# Default column names (we auto-detect if the file uses different headers)
COLS = {
    # Device master columns (hacc/pre-rdr/enrolled)
    "iccid": "ICCID",
    "vin": "VIN",
    "meid": "MEID",
    "mdn": "MDN",
    "brand": "BRAND",
    "use_purpose": "USE_PURPOSE",
    "device_group": "DEVICE_GROUP_NAME",  # optional

    # Usage file columns
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

# Candidate column headers (auto-detection)
CANDIDATES = {
    "iccid": ["ICCID", "Iccid", "iccid", "SIM ICCID", "SIM_ICCID"],
    "roaming": ["Roaming Zone", "Roaming", "RoamingZone", "Roaming Flag", "Roaming Indicator"],

    "data_volume": ["Data Volume (MB)", "Data Volume", "Data Usage", "Usage (MB)", "Total MB", "Total Data (MB)", "MB"],
    "sms_volume": ["SMS Volume (msg)", "SMS Volume", "SMS Usage", "Messages", "Total Messages", "Usage (SMS)", "SMS"],
    "voice_volume": ["Voice Volume", "Voice Usage", "Voice Minutes", "Minutes", "Total Minutes", "Usage (Min)", "Usage Minutes", "Voice"],
}

# Candidate sheet names for hacc workbook (auto-detection)
SHEET_CANDIDATES = {
    "pre_rdr": ["Pre-RDR", "Pre RDR", "PreRDR", "Pre Rdr", "pre-rdr", "PRE-RDR"],
    "enrolled_ppu": ["Enrolled-PPU", "Enrolled PPU", "PPU", "Enrolled_PPU", "ENROLLED-PPU"],
    "mrc": ["MRC(H,G)-Enrolled(5,10,30,50,1)", "MRC", "MRC(H,G)", "MRC Enrolled", "MRC_Enrolled"],
}


# ---------- UTILITIES ----------

def pick_first_existing_col(df: pd.DataFrame, candidates: list[str], label: str) -> str:
    """Return the first matching candidate column name from df, else raise KeyError with helpful message."""
    cols = list(df.columns)
    colset = set(cols)
    for c in candidates:
        if c in colset:
            return c
    raise KeyError(f"Could not find {label} column. Available columns: {cols}")


def pick_sheet_name(xls: pd.ExcelFile, candidates: list[str], label: str) -> str:
    """Return the first matching candidate sheet name from xls, else raise KeyError with helpful message."""
    sheets = xls.sheet_names
    sset = set(sheets)
    for c in candidates:
        if c in sset:
            return c
    raise KeyError(f"Could not find {label} sheet. Available sheets: {sheets}")


def safe_get(df: pd.DataFrame, col: str) -> pd.Series:
    """Return df[col] if it exists, else a blank Series of same length."""
    if col in df.columns:
        return df[col]
    return pd.Series([""] * len(df))


# ---------- HELPER FUNCTIONS ----------

def apply_common_device_filters(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Keep USE_PURPOSE in (1, 2) if column exists
    - Brand rules:
        - 'G' / 'Genesis' -> Genesis
        - 'H' / 'Hyundai' or blank -> Hyundai
    - Drop rows with no ICCID
    - Drop test devices if DEVICE_GROUP_NAME contains TEST
    """
    df = df.copy()

    # ICCID column detect (for device files too)
    iccid_col = COLS["iccid"]
    if iccid_col not in df.columns:
        iccid_col = pick_first_existing_col(df, CANDIDATES["iccid"], "ICCID (device list)")
    df = df[df[iccid_col].notna()]
    df[COLS["iccid"]] = df[iccid_col]

    # USE_PURPOSE filter
    if COLS["use_purpose"] in df.columns:
        df = df[df[COLS["use_purpose"]].isin([1, 2])]

    # Brand mapping (default blank -> Hyundai)
    if COLS["brand"] in df.columns:
        brand = (
            df[COLS["brand"]]
            .fillna("H")
            .astype(str)
            .str.strip()
            .str.upper()
        )
    else:
        brand = pd.Series(["H"] * len(df))

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
        mask = ~df[COLS["device_group"]].astype(str).str.upper().str.contains("TEST", na=False)
        df = df[mask]

    return df


def normalize_data_usage(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    iccid_col = COLS["data_iccid"] if COLS["data_iccid"] in df.columns else pick_first_existing_col(df, CANDIDATES["iccid"], "ICCID (data usage)")
    roaming_col = COLS["data_roaming"] if COLS["data_roaming"] in df.columns else pick_first_existing_col(df, CANDIDATES["roaming"], "Roaming (data usage)")
    vol_col = COLS["data_volume"] if COLS["data_volume"] in df.columns else pick_first_existing_col(df, CANDIDATES["data_volume"], "Data volume")

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


def normalize_sms_usage(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    iccid_col = COLS["sms_iccid"] if COLS["sms_iccid"] in df.columns else pick_first_existing_col(df, CANDIDATES["iccid"], "ICCID (sms usage)")
    roaming_col = COLS["sms_roaming"] if COLS["sms_roaming"] in df.columns else pick_first_existing_col(df, CANDIDATES["roaming"], "Roaming (sms usage)")
    vol_col = COLS["sms_volume"] if COLS["sms_volume"] in df.columns else pick_first_existing_col(df, CANDIDATES["sms_volume"], "SMS volume")

    df[COLS["sms_iccid"]] = df[iccid_col]
    df[COLS["sms_roaming"]] = df[roaming_col]
    df[COLS["sms_volume"]] = (
        df[vol_col]
        .astype(str)
        .str.replace(",", "")
        .str.strip()
        .replace("", "0")
        .astype(float)
    )
    return df


def normalize_voice_usage(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    iccid_col = COLS["voice_iccid"] if COLS["voice_iccid"] in df.columns else pick_first_existing_col(df, CANDIDATES["iccid"], "ICCID (voice usage)")
    roaming_col = COLS["voice_roaming"] if COLS["voice_roaming"] in df.columns else pick_first_existing_col(df, CANDIDATES["roaming"], "Roaming (voice usage)")
    vol_col = COLS["voice_volume"] if COLS["voice_volume"] in df.columns else pick_first_existing_col(df, CANDIDATES["voice_volume"], "Voice volume/minutes")

    df[COLS["voice_iccid"]] = df[iccid_col]
    df[COLS["voice_roaming"]] = df[roaming_col]

    # strip ":00" to convert e.g. "12:00" -> "12"
    df[COLS["voice_volume"]] = (
        df[vol_col]
        .astype(str)
        .str.replace(":00", "", regex=False)
        .str.strip()
        .replace("", "0")
        .astype(float)
    )
    return df


def build_setup_devices(hacc_excel: pd.ExcelFile, pre_rdr_check_df: pd.DataFrame) -> pd.DataFrame:
    pre_rdr_sheet = pick_sheet_name(hacc_excel, SHEET_CANDIDATES["pre_rdr"], "Pre-RDR")
    pre_rdr_hacc = pd.read_excel(hacc_excel, sheet_name=pre_rdr_sheet)

    pre_rdr_hacc = apply_common_device_filters(pre_rdr_hacc)
    pre_rdr_check_df = apply_common_device_filters(pre_rdr_check_df)

    merged = pre_rdr_hacc.merge(
        pre_rdr_check_df[[COLS["iccid"]]].drop_duplicates(),
        on=COLS["iccid"],
        how="inner",
    )
    return merged


def build_ppu_devices(hacc_excel: pd.ExcelFile, enrolled_ppu_check_df: pd.DataFrame) -> pd.DataFrame:
    ppu_sheet = pick_sheet_name(hacc_excel, SHEET_CANDIDATES["enrolled_ppu"], "Enrolled-PPU")
    ppu_hacc = pd.read_excel(hacc_excel, sheet_name=ppu_sheet)

    ppu_hacc = apply_common_device_filters(ppu_hacc)
    enrolled_ppu_check_df = apply_common_device_filters(enrolled_ppu_check_df)

    merged = ppu_hacc.merge(
        enrolled_ppu_check_df[[COLS["iccid"]]].drop_duplicates(),
        on=COLS["iccid"],
        how="inner",
    )
    return merged


def build_mrc_devices(hacc_excel: pd.ExcelFile) -> pd.DataFrame:
    mrc_sheet = pick_sheet_name(hacc_excel, SHEET_CANDIDATES["mrc"], "MRC")
    mrc_hacc = pd.read_excel(hacc_excel, sheet_name=mrc_sheet)
    mrc_hacc = apply_common_device_filters(mrc_hacc)
    return mrc_hacc


def build_tms_service_setup_rows(setup_devices: pd.DataFrame, bill_start, bill_end) -> pd.DataFrame:
    if setup_devices.empty:
        return pd.DataFrame()

    df = setup_devices.copy()
    bill = pd.DataFrame()
    bill["ICCID"] = df[COLS["iccid"]]
    bill["VIN"] = safe_get(df, COLS["vin"])
    bill["MEID/IMEI"] = safe_get(df, COLS["meid"])
    bill["MDN/MSISDN"] = safe_get(df, COLS["mdn"])
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
        if z in ["R", "ROAM", "ROAMING", "YES", "Y", "TRUE", "T"]:
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
        tmp["VIN"] = safe_get(sub, COLS["vin"])
        tmp["MEID/IMEI"] = safe_get(sub, COLS["meid"])
        tmp["MDN/MSISDN"] = safe_get(sub, COLS["mdn"])
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
    bill["VIN"] = safe_get(df, COLS["vin"])
    bill["MEID/IMEI"] = safe_get(df, COLS["meid"])
    bill["MDN/MSISDN"] = safe_get(df, COLS["mdn"])
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
    bill["VIN"] = safe_get(df, COLS["vin"])
    bill["MEID/IMEI"] = safe_get(df, COLS["meid"])
    bill["MDN/MSISDN"] = safe_get(df, COLS["mdn"])
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

    invoice_summary = build_invoice_summary(bill_detail)

    return bill_detail, invoice_summary, {
        "hacc_sheets": hacc_excel.sheet_names,
        "data_cols": list(data_usage_raw.columns),
        "sms_cols": list(sms_usage_raw.columns),
        "voice_cols": list(voice_usage_raw.columns),
    }


# ---------- STREAMLIT UI ----------

st.title("HACC Billing Automation (Bill Detail + Invoice)")
st.write("Upload the required files and generate Bill Detail & Invoice Summary automatically.")

hacc_file = st.file_uploader("hacc_SepXX.xlsx", type=["xlsx"])
pre_rdr_check_file = st.file_uploader("pre-rdr_check_SepXX.xlsx", type=["xlsx"])
enrolled_ppu_check_file = st.file_uploader("enrolled-ppu_check_SepXX.xlsx", type=["xlsx"])
data_usage_file = st.file_uploader("HAC_Data.xlsx", type=["xlsx"])
sms_usage_file = st.file_uploader("HAC_SMS.xlsx", type=["xlsx"])
voice_usage_file = st.file_uploader("HAC_Voice.xlsx", type=["xlsx"])

col1, col2 = st.columns(2)
with col1:
    bill_start = st.date_input("Bill Start Date", value=date(2025, 9, 1))
with col2:
    bill_end = st.date_input("Bill End Date", value=date(2025, 9, 30))

if st.button("Generate Bill Detail + Invoice"):
    missing = [
        name for name, f in [
            ("hacc_SepXX", hacc_file),
            ("pre-rdr_check_SepXX", pre_rdr_check_file),
            ("enrolled-ppu_check_SepXX", enrolled_ppu_check_file),
            ("HAC_Data", data_usage_file),
            ("HAC_SMS", sms_usage_file),
            ("HAC_Voice", voice_usage_file),
        ] if f is None
    ]

    if missing:
        st.error(f"Please upload: {', '.join(missing)}")
    else:
        try:
            bill_detail_df, invoice_summary_df, debug = process_hacc_billing(
                hacc_file,
                pre_rdr_check_file,
                enrolled_ppu_check_file,
                data_usage_file,
                sms_usage_file,
                voice_usage_file,
                bill_start,
                bill_end,
            )

            st.success("Processing complete!")

            with st.expander("Debug info (columns & sheet names)"):
                st.write("HACC workbook sheets:", debug["hacc_sheets"])
                st.write("Data usage columns:", debug["data_cols"])
                st.write("SMS usage columns:", debug["sms_cols"])
                st.write("Voice usage columns:", debug["voice_cols"])

            st.subheader("Preview: Bill Detail (first 50 rows)")
            st.dataframe(bill_detail_df.head(50))

            st.subheader("Preview: Invoice Summary")
            st.dataframe(invoice_summary_df)

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

        except Exception as e:
            st.error("Processing failed. Open the Debug info expander (if available) and/or check Streamlit logs.")
            st.exception(e)
