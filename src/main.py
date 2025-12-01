from fetch import fetch_data
from transform import transform_data


def main():
    raw_file = fetch_data()
    csv_path, parquet_path = transform_data(raw_file)
    print("Saved:", csv_path, parquet_path)


if __name__ == "__main__":
    main()
