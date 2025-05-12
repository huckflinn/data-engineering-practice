import json
import csv
from pathlib import Path

import flatten_json


def main():
    p = Path(".")

    json_files = list(p.glob("**/*.json"))

    for json_file in json_files:
        with open(json_file, encoding = "utf-8") as f:
            json_tmp = json.load(f)

        print(f"{json_file.name}: {json_tmp}")


if __name__ == "__main__":
    main()
