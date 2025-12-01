from fetch import fetch_data
from transform import transform_data


def main():
    # Hier kannst du später andere Währungen / Seiten testen
    raw_file = fetch_data(
        vs_currency="usd",
        per_page=100,
        page=1,
    )
    csv_path, parquet_path = transform_data(raw_file)
    print("Saved:", csv_path, parquet_path)


if __name__ == "__main__":
    main()
