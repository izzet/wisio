def convert_bytes_to_unit(bytes: int, unit: str):
    unit = unit.lower()
    if unit == "b" or unit == "bytes":
        return float(bytes)
    elif unit == "kb" or unit == "kilobytes":
        return float(bytes) / 1024
    elif unit == "mb" or unit == "megabytes":
        return float(bytes) / 1024 / 1024
    elif unit == "gb" or unit == "gigabytes":
        return float(bytes) / 1024 / 1024 / 1024
    elif unit == "tb" or unit == "terabytes":
        return float(bytes) / 1024 / 1024 / 1024 / 1024
    else:
        raise ValueError("Invalid unit specified")
