import click
import pandas as pd
import time
import zstandard

def get_complex_compression(compression_type):
    if compression_type == "zstd":
        # set up zstd compression options
        params = zstandard.ZstdCompressionParameters.from_level(3, window_log=12, enable_ldm=True, threads=-1)
        complex_compression = {
            'method': "zstd",
            'compression_params': params
        }
        return complex_compression
    else:
        raise RuntimeError(f"Compression type {compression_type} is unsupported")


@click.command()
@click.option('-c', '--compression-type', default='zstd', help='Compression format to use')
@click.option('-l', '--left-input-name', prompt='Name of the left input file',
              help='This should be a <name>.csv.zst file, it will be the left input file for the join/merge.')
@click.option('-r', '--right-input-name', prompt='Name of the right input file',
              help='This should be a <name>.csv.zst file, it will be the right input file for the join/merge.')
@click.option('-o', '--output-name', prompt='Name of the output file',
              help='This should be a <name>.csv.zst file.')
def join(compression_type, left_input_name, right_input_name, output_name):
    print(compression_type, left_input_name, right_input_name, output_name)

    start = time.time()
    right_input = pd.read_csv(right_input_name, dtype={0: "int32[pyarrow]", 1: "category", 2: "category"})
    end = time.time()
    print("small/right dataset loaded into ram")
    print(f"loadtime: {end - start} seconds")
    print(right_input.head())
    print(right_input.info())
    print()

    left_input_dtype = {0: "int32[pyarrow]", 1: "category", 2: "category", 3: "category", 4: "category"}
    start = time.time()
    left_input = pd.read_csv(left_input_name, dtype=left_input_dtype, low_memory=True)
    end = time.time()
    print("big dataset loaded into ram")
    print(f"loadtime: {end - start} seconds")
    print(left_input.head())
    print(left_input.info())
    print()

    start = time.time()
    result = pd.merge(left_input, right_input, on="id", how="outer")
    end = time.time()
    print("result info")
    print(f"runtime: {end - start} seconds")
    print(result.info())
    print()

    complex_compression = get_complex_compression(compression_type)
    print("Saving compressed file to disk")
    start = time.time()
    result.to_csv(output_name, compression=complex_compression, index=False)
    end = time.time()
    print(f"Compression and saving time: {end - start} seconds")

    print("done, exiting")

if __name__ == '__main__':
    join()

