import os
import json
import argparse
from serpapi import GoogleSearch
from dotenv import load_dotenv
import pandas as pd

def fetch_flight_data_raw(from_city: str,
                          to_city: str,
                          depart_date: str,
                          api_key: str,
                          raw_json_file: str) -> None:
    """
    Fetch raw flight data from SerpApi and save to a JSON file.
    """
    params = {
        "engine": "google_flights",
        "departure_id": from_city,
        "arrival_id": to_city,
        "outbound_date": depart_date,
        "type": "2",  # one-way
        "api_key": api_key
    }
    search = GoogleSearch(params)
    results = search.get_dict()

    with open(raw_json_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"Saved raw JSON to '{raw_json_file}'")


def process_flight_data(raw_json_file: str,
                        output_csv: str) -> pd.DataFrame:
    """
    Load raw JSON flight data, transform into a DataFrame, and save to CSV.
    Returns the DataFrame.
    """
    with open(raw_json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

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
            "duration_min": entry.get("total_duration", seg.get("duration")),
            "price_usd": entry.get("price"),
            "booking_token": entry.get("booking_token"),
        })

    df = pd.DataFrame(rows)
    df.to_csv(output_csv, index=False)
    print(f"Created CSV '{output_csv}' with {len(df)} rows.")
    return df


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Pipeline to fetch and process flight data via SerpApi."
    )
    parser.add_argument("--from_city", required=True,
                        help="IATA code of departure city (e.g., DEL)")
    parser.add_argument("--to_city", required=True,
                        help="IATA code of arrival city (e.g., BOM)")
    parser.add_argument("--depart_date", required=True,
                        help="Date of departure in YYYY-MM-DD format")
    parser.add_argument("--raw_json", default=None,
                        help="Filename for saving raw JSON output. Defaults to '{from}_{to}_{date}.json'")
    parser.add_argument("--output_csv", default=None,
                        help="Filename for saving processed CSV data. Defaults to '{from}_{to}_{date}.csv'")
    args = parser.parse_args()

    api_key = os.getenv("SERPAPI_API_KEY")
    if not api_key:
        parser.error("Environment variable SERPAPI_API_KEY not set.")

    raw_json_file = args.raw_json or f"{args.from_city}_{args.to_city}_{args.depart_date}.json"
    output_csv_file = args.output_csv or f"{args.from_city}_{args.to_city}_{args.depart_date}.csv"

    # Step 1: Fetch raw data
    fetch_flight_data_raw(
        from_city=args.from_city,
        to_city=args.to_city,
        depart_date=args.depart_date,
        api_key=api_key,
        raw_json_file=raw_json_file
    )

    # Step 2: Process and save to CSV
    process_flight_data(
        raw_json_file=raw_json_file,
        output_csv=output_csv_file
    )


if __name__ == "__main__":
    main()
