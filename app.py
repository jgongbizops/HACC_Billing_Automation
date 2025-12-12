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
