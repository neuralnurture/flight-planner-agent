import os
import argparse
from datetime import datetime
from one_way import fetch_flight_data_raw, process_flight_data
from round_trip import fetch_roundtrip_data_raw, process_roundtrip_data
from dotenv import load_dotenv


def parse_args():
    parser = argparse.ArgumentParser(
        description="Batch fetch & process flight data for lists of cities and dates."
    )
    parser.add_argument(
        "--cities", required=True,
        help="Comma-separated IATA codes of cities (e.g., DEL,BOM,BLR)"
    )
    parser.add_argument(
        "--depart_dates", required=True,
        help="Comma-separated departure dates YYYY-MM-DD"
    )
    parser.add_argument(
        "--return_dates", required=False,
        help="Comma-separated return dates YYYY-MM-DD (for round-trip)"
    )
    parser.add_argument(
        "--mode", choices=["one-way", "round-trip", "both"],
        default="both",
        help="Which flight types to fetch: one-way, round-trip, or both"
    )
    return parser.parse_args()


def iso_dates(date_strings):
    return [datetime.fromisoformat(d).date() for d in date_strings.split(',')]


def main():
    load_dotenv()
    args = parse_args()
    api_key = os.getenv("SERPAPI_API_KEY")
    if not api_key:
        raise RuntimeError("SERPAPI_API_KEY not set in environment.")

    cities = args.cities.split(',')
    depart_dates = iso_dates(args.depart_dates)
    return_dates = iso_dates(args.return_dates) if args.return_dates else []

    # One-way flights
    if args.mode in ("one-way", "both"):
        for from_city in cities:
            for to_city in cities:
                if from_city == to_city:
                    continue
                for depart_date in depart_dates:
                    raw_json = f"{from_city}_{to_city}_{depart_date}.json"
                    csv_file = f"{from_city}_{to_city}_{depart_date}.csv"
                    print(f"Fetching one-way {from_city}->{to_city} on {depart_date}")
                    fetch_flight_data_raw(
                        from_city=from_city,
                        to_city=to_city,
                        depart_date=str(depart_date),
                        api_key=api_key,
                        raw_json_file=raw_json
                    )
                    process_flight_data(
                        raw_json_file=raw_json,
                        output_csv=csv_file
                    )

    # Round-trip flights
    if args.mode in ("round-trip", "both") and return_dates:
        for from_city in cities:
            for to_city in cities:
                if from_city == to_city:
                    continue
                for depart_date in depart_dates:
                    for return_date in return_dates:
                        if return_date <= depart_date:
                            continue
                        raw_json = f"{from_city}_{to_city}_{depart_date}_{return_date}.json"
                        csv_file = f"{from_city}_{to_city}_{depart_date}_{return_date}.csv"
                        print(f"Fetching round-trip {from_city}->{to_city} {depart_date} to {return_date}")
                        fetch_roundtrip_data_raw(
                            from_city=from_city,
                            to_city=to_city,
                            depart_date=str(depart_date),
                            return_date=str(return_date),
                            api_key=api_key,
                            raw_json_file=raw_json
                        )
                        process_roundtrip_data(
                            raw_json_file=raw_json,
                            output_csv=csv_file
                        )


if __name__ == "__main__":
    main()
