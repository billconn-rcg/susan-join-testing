import click
import pandas as pd
import os
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
@click.option('-i', '--input-name', prompt='Name of the input file',
              help='This should be a <name>.csv.zst file.')
@click.option('-f', '--output-folder', prompt='Folder to store output',
              help='This should be a folder that already exists.')
@click.option('-s', '--output-suffix', prompt='Suffix of the output file',
              help='This should be a _<name>.csv.zst file, where the extracted value from the id column will prepend the filename')
def split(compression_type, input_name, output_folder, output_suffix):
    # Get unique values of the id column
    keys = set()
    start = time.time()
    header = ""
    with zstandard.open(input_name, 'r') as f:
        header = f.readline()
        for line in f:
            column_id = line.split(',')[0].replace('"', '')
            keys.add(column_id)
    keys = sorted(keys)
    end = time.time()
    print(f"unique keys extracted from {input_name}")
    print(f"runtime: {end - start} seconds")
    #print(keys)

    # Create a file handle for each key
    file_handles = {}
    for key in keys:
        file_path = os.path.join(output_folder, str(key) + output_suffix)
        try:
            file_handles[key] = zstandard.open(file_path, 'w')
            # write the header
            file_handles[key].write(header)
        except:
            raise RuntimeError(f"Error opening/writing file {file_path}")

    # write each line into its correct file based on its id
    start = time.time()
    with zstandard.open(input_name, 'r') as f:
        header = f.readline()
        for line in f:
            column_id = line.split(',')[0].replace('"', '')
            file_handles[column_id].write(line)
    end = time.time()
    print(f"time to split files: {end - start} seconds")

    # close out all of our file handles
    for key in keys:
        file_handles[key].close()

    print("done, exiting")

if __name__ == '__main__':
    split()