import os
import json
import argparse
from serpapi import GoogleSearch
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

def fetch_roundtrip_data_raw(
    from_city: str,
    to_city: str,
    depart_date: str,
    return_date: str,
    api_key: str,
    raw_json_file: str
) -> None:
    """
    Fetch raw round-trip flight data from SerpApi, add a timestamp, and save to JSON.
    """
    params = {
        "engine": "google_flights",
        "departure_id": from_city,
        "arrival_id": to_city,
        "outbound_date": depart_date,
        "return_date": return_date,
        "type": "1",  # round-trip
        "api_key": api_key
    }
    search = GoogleSearch(params)
    results = search.get_dict()

    # Capture local execution timestamp
    fetched_at = datetime.now().isoformat()
    results["_fetched_at"] = fetched_at

    with open(raw_json_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"Saved round-trip JSON to '{raw_json_file}' (fetched_at={fetched_at})")


def process_roundtrip_data(
    raw_json_file: str,
    output_csv: str
) -> pd.DataFrame:
    """
    Load raw round-trip JSON flight data, transform into a DataFrame with timestamp, and save to CSV.
    """
    with open(raw_json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    fetched_at = data.get("_fetched_at", "")
    all_flights = data.get("best_flights", []) + data.get("other_flights", [])
    rows = []
    for entry in all_flights:
        seg = entry["flights"][0]
        rows.append({
            "flight_number": seg.get("flight_number"),
            "airline": seg.get("airline"),
            "airplane": seg.get("airplane", ""),
            "travel_class": seg.get("travel_class", ""),
            "legroom": seg.get("legroom", ""),
            "departure_airport_id": seg["departure_airport"]["id"],
            "departure_airport_name": seg["departure_airport"]["name"],
            "departure_time": seg["departure_airport"]["time"],
            "arrival_airport_id": seg["arrival_airport"]["id"],
            "arrival_airport_name": seg["arrival_airport"]["name"],
            "arrival_time": seg["arrival_airport"]["time"],
            "total_duration_min": entry.get("total_duration"),
            "price_usd": entry.get("price"),
            "trip_type": entry.get("type"),
            "departure_token": entry.get("departure_token"),
            "fetched_at": fetched_at,
        })

    df = pd.DataFrame(rows)
    df.to_csv(output_csv, index=False)
    print(f"Created CSV '{output_csv}' with {len(df)} rows.")
    return df


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Pipeline to fetch and process round-trip flight data via SerpApi."
    )
    parser.add_argument("--from_city", required=True,
                        help="IATA code of departure city (e.g., DEL)")
    parser.add_argument("--to_city", required=True,
                        help="IATA code of arrival city (e.g., BOM)")
    parser.add_argument("--depart_date", required=True,
                        help="Outbound date in YYYY-MM-DD format")
    parser.add_argument("--return_date", required=True,
                        help="Return date in YYYY-MM-DD format")
    parser.add_argument("--raw_json", default=None,
                        help="Filename for JSON. Defaults to '{from}_{to}_{depart}_{return}.json'")
    parser.add_argument("--output_csv", default=None,
                        help="Filename for CSV. Defaults to '{from}_{to}_{depart}_{return}.csv'")
    args = parser.parse_args()

    api_key = os.getenv("SERPAPI_API_KEY")
    if not api_key:
        parser.error("Environment variable SERPAPI_API_KEY not set.")

    raw_json_file = args.raw_json or f"{args.from_city}_{args.to_city}_{args.depart_date}_{args.return_date}.json"
    output_csv_file = args.output_csv or f"{args.from_city}_{args.to_city}_{args.depart_date}_{args.return_date}.csv"

    fetch_roundtrip_data_raw(
        from_city=args.from_city,
        to_city=args.to_city,
        depart_date=args.depart_date,
        return_date=args.return_date,
        api_key=api_key,
        raw_json_file=raw_json_file
    )
    process_roundtrip_data(
        raw_json_file=raw_json_file,
        output_csv=output_csv_file
    )


if __name__ == "__main__":
    main()
