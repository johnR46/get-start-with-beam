import apache_beam as beam
import csv
from apache_beam.runners.direct import DirectRunner


def map_to_dict(elem: str) -> dict:
    # from a line to CSV splitting
    for l in csv.reader(
            [elem],
            delimiter=",",
            quotechar='"',
            quoting=csv.QUOTE_ALL,
            skipinitialspace=True,
    ):
        result = {
            "id": int(l[0]),
            "first_name": l[1],
            "last_name": l[2],
            "gender": l[3],
            "occupation": l[4],
        }
        return result


def map_to_csv_row(elem: dict) -> str:
    # concatenate values of dict to a line with comma separated
    return ",".join(f'"{v}"' for k, v in elem.items())


def run():
    with beam.Pipeline(runner=DirectRunner()) as p:
        (
                p
                | beam.io.ReadFromText("dataset/01-mock.csv", skip_header_lines=1)
                | beam.Map(map_to_dict)
                | beam.Filter(lambda x: x["gender"] == 'F')
                | beam.Map(map_to_csv_row)
                | beam.io.WriteToText("output/01-mock.csv", shard_name_template="")
        )


if __name__ == "__main__":
    run()
