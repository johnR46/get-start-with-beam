import apache_beam as beam

from apache_beam.runners.direct import DirectRunner


def run():
    with beam.Pipeline(runner=DirectRunner()) as p:
        input_from_file = (
                p
                | beam.io.ReadFromText("dataset/01-mock.csv", skip_header_lines=1)
        )

        input_from_file | "print" >> beam.Map(print)


if __name__ == "__main__":
    run()
