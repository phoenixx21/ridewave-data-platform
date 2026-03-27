import csv

def read_csv(filepath):
    """
    Reads a CSV file using csv.DictReader.
    Returns list of dicts. Handles FileNotFoundError gracefully.
    """
    try:
        with open(filepath, "r", newline="") as f:
            reader = csv.DictReader(f)
            return list(reader)
    except FileNotFoundError:
        print(f"[RideWave] ERROR: File not found → {filepath}")
        return []

def validate_not_null(data, column):
    """
    Checks if any row has None or empty string in given column.
    Returns dict: {'column': col, 'null_count': n, 'valid': bool}
    """
    null_count = sum(
        1 for row in data
        if row.get(column) is None or row.get(column) == ""
    )
    return {
        "column"    : column,
        "null_count": null_count,
        "valid"     : null_count == 0
    }

def count_duplicates(data, key_column):
    """
    Counts duplicate values in key_column.
    Returns count of duplicates (int).
    """
    values = [row[key_column] for row in data if key_column in row]
    return len(values) - len(set(values))

def log_summary(table_name, row_count, null_report, dup_count):
    """
    Prints a formatted quality summary for any table.
    """
    valid_str = "OK" if null_report["valid"] else "HAS NULLS"
    print(
        f"[RideWave] {table_name} "
        f"| rows: {row_count} "
        f"| nulls in {null_report['column']}: {null_report['null_count']} ({valid_str}) "
        f"| duplicates: {dup_count}"
    )

if __name__ == "__main__":
    data       = read_csv("data/rides.csv")
    null_report= validate_not_null(data, "ride_id")
    dup_count  = count_duplicates(data, "ride_id")
    log_summary("rides", len(data), null_report, dup_count)
    print("ridewave_utils.py working correctly!")