import click
import io
import os
import time
import zstandard
from zstandard.backend_cffi import ZstdCompressor


class FileListReader(io.BufferedReader):
    def __init__(self, file_names: list, buffer_size=io.DEFAULT_BUFFER_SIZE):
        self.files_to_read = {}
        for file in file_names:
            try:
                self.files_to_read[file] = zstandard.open(file, "r")
            except:
                raise RuntimeError(f"Error opening file: {file}")
        #print(self.files_to_read)
        # Verify all headers are the same
        header = set()
        for file in file_names:
            header.add(self.files_to_read[file].readline())
        if len(header) > 1:
                raise RuntimeError(f"files don't all have the same header")

        # Set up done_reading for later
        self.done_reading = {}
        for file in file_names:
            self.done_reading[file] = False

    def __del__(self):
        self.close()

    def close(self):
        for file in self.files_to_read.keys():
            self.files_to_read[file].close()

    def readline(self, size=-1) -> str:
        if size != -1:
            raise RuntimeError("Size not supported")
        to_ret = self._get_next_lines(1)
        if len(to_ret) > 1:
            raise RuntimeError("get_next_lines(1) returned more than one line")
        if len(to_ret) == 0:
            return ""
        return to_ret[0]

    def readlines(self, hint=10000) -> list:
        if hint == -1:
            raise RuntimeError("Can't return the whole file, it's too large")
        return self._get_next_lines(hint=hint)

    def _get_next_lines(self, hint=1):
        for file in self.files_to_read.keys():
            if self.done_reading[file]:
                pass
            next_line = self.files_to_read[file].readlines(hint)
            if next_line:
                return next_line
            else:
                self.done_reading[file] = True
        return []


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
        raise RuntimeError(f'Compression type {compression_type} is unsupported')


def is_allowed_file_extension(compression_type, file_path: str):
    allowed_extensions = []
    if compression_type == "zstd":
        allowed_extensions.append("csv.zst")
        allowed_extensions.append("csv.zstd")
    else:
        raise RuntimeError(f"Compression type {compression_type} is unsupported")

    for extension in allowed_extensions:
        if file_path.endswith(extension):
            return True

    return False


def get_header(compression_type, file_path):
    if compression_type == "zstd":
        with zstandard.open(file_path, 'r') as f:
            return f.readline()
    else:
        raise RuntimeError(f"Compression type {compression_type} is unsupported")


@click.command()
@click.option('-c', '--compression-type', default='zstd', help='Compression format to use')
@click.option("-r", "--read-lines-count", default=1000000, help="amount of lines to read at a time, can impact memory")
@click.option('-i', '--input-folder', help='folder full of .csv.zst files to combine')
@click.option('-o', '--output-name', prompt='Name of the output file',
              help='This should be a <name>.csv.zst file.')
def join_files(compression_type, read_lines_count, input_folder, output_name):
    print("building list of files to combine")
    # build a list_of_files from the input folder
    list_of_files = []
    folder_contents = os.listdir(input_folder)
    for item in folder_contents:
        item_path = os.path.join(input_folder, item)
        if os.path.isfile(item_path) and is_allowed_file_extension(compression_type, item_path):
            list_of_files.append(os.path.join(input_folder, item))
    # we need this list sorted to preserve the sorted order
    list_of_files = sorted(list_of_files)

    #print(list_of_files)
    print("extracting header")
    file_header = get_header(compression_type, list_of_files[0])

    print("combining files into one")
    start = time.time()
    file_list = FileListReader(list_of_files)
    params = zstandard.ZstdCompressionParameters.from_level(3, window_log=12, enable_ldm=True, threads=-1)
    compressor = ZstdCompressor(compression_params=params)
    with zstandard.open(output_name, 'w', cctx=compressor) as out_file:
        # First write the header
        out_file.write(file_header)

        # then write all the lines
        done = False
        while not done:
            lines_to_write = file_list.readlines(read_lines_count)
            #lines_to_write = file_list.readline()
            #print(f"line to write: {lines_to_write}")
            #time.sleep(1)
            if not lines_to_write:
                done = True
                pass
            out_file.write(''.join(lines_to_write))
            #out_file.write(lines_to_write)
            out_file.flush()
    # Close files
    file_list.close()
    end = time.time()
    print(f"combined files in: {end - start} seconds")

if __name__ == '__main__':
    join_files()