import json
import csv
from pathlib import Path

from flatten_json import flatten


def main():
    p = Path(".")

    csv_dir = Path("./csv_data")

    if not csv_dir.exists():
        csv_dir.mkdir()

    json_files = list(p.glob("**/*.json"))

    for json_file in json_files:
        with open(json_file, encoding = "utf-8") as f_json:
            json_tmp = json.load(f_json)

            flat_json = flatten(json_tmp)

            csv_name = json_file.name.split(".json")[0]

        with open(f"{csv_dir}/{csv_name}.csv", 'w', encoding = "utf-8") as f_csv:
            writer = csv.DictWriter(f_csv, fieldnames = flat_json.keys())

            writer.writeheader()
            
            row = {}
            for k, v in flat_json.items():
                row[k] = v

            writer.writerow(row)


if __name__ == "__main__":
    main()
