import hashlib
import datetime


def machine_digest(machine_id: str) -> str:
    """
    Turn the machine ID (from the app's About dialog) into the 6-character
    machine code used in licence keys.
    """
    mid = (machine_id or "").strip().upper()
    digest = hashlib.sha256(mid.encode("utf-8")).hexdigest().upper()
    return digest[:6]


def make_key(machine_id: str, expiry_date: str, suffix: str = "ABCD") -> str:
    """
    machine_id: Machine ID string from the customer's About dialog.
    expiry_date: 'YYYY-MM-DD' or 'YYYYMMDD' (expiry date of the licence).
    suffix: optional suffix for your own tracking (e.g. CLIENT1).
    """
    expiry_date = expiry_date.replace("-", "")
    # Validate the date
    datetime.datetime.strptime(expiry_date, "%Y%m%d")

    mcode = machine_digest(machine_id)
    return f"CHL-{expiry_date}-{mcode}-{suffix}"


if __name__ == "__main__":
    print("=== Companies House Lookup Licence Generator ===\n")

    machine_id = input("Machine ID from customer (as shown in About): ").strip()
    expiry = input("Expiry date (YYYY-MM-DD, e.g. 2025-12-31): ").strip()
    suffix = input("Optional suffix (e.g. CLIENT1, default ABCD): ").strip() or "ABCD"

    try:
        key = make_key(machine_id, expiry, suffix)
    except ValueError:
        print("\n[ERROR] Expiry date format must be YYYY-MM-DD or YYYYMMDD.")
    else:
        print("\nGenerated licence key:")
        print(key)


