import pandas as pd
import os
import time
import zstandard

# set up variables
input_file="./final_methylation_sorted_with_header_sample.csv"
test_0 = "./uncompressed.csv"
test_1 = "./compression-no-options.csv.zst"
test_2 = "./compression-long-option.csv.zst"
test_3 = "./compression-long-option_threads.csv.zst"

# remove files if they exist
try:
    os.remove(test_0)
    print("removed:", test_0)
except:
    # if we get file not found, just skip
    pass
try:
    os.remove(test_1)
    print("removed:", test_1)
except:
    # if we get file not found, just skip
    pass
try:
    os.remove(test_2)
    print("removed:", test_2)
except:
    # if we get file not found, just skip
    pass
try:
    os.remove(test_3)
    print("removed:", test_3)
except:
    # if we get file not found, just skip
    pass

df = pd.read_csv(input_file)

print("file read in, beginning to write files")

# no specific compression options
start = time.time()
df.to_csv(test_0, index=False)
end = time.time()
print("no options took:", end - start)

# no specific compression options
start = time.time()
df.to_csv(test_1, compression="zstd", index=False)
end = time.time()
print("no options took:", end - start)


# try long option
#params = zstandard.ZstdCompressionParameters(window_log=12, enable_ldm=True)
#params = zstandard.ZstdCompressionParameters(compression_level=22, window_log=12, enable_ldm=True)
params = zstandard.ZstdCompressionParameters.from_level(3, window_log=12, enable_ldm=True)
#params = zstandard.ZstdCompressionParameters.from_level(3, enable_ldm=True)
complex_compression = {
    'method': "zstd",
    'compression_params': params
}
start = time.time()
df.to_csv(test_2, compression=complex_compression, index=False)
end = time.time()
print("no threads took:", end - start)


params = zstandard.ZstdCompressionParameters.from_level(3, window_log=12, enable_ldm=True, threads=-1)
complex_compression = {
    'method': "zstd",
    'compression_params': params
}
start = time.time()
df.to_csv(test_3, compression=complex_compression, index=False)
end = time.time()
print("threads=-1 took:", end - start)
