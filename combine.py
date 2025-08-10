import gc
import pandas as pd
import zstandard

small_dataset_filename = "reshaped_phenotype_sorted_with_header.csv.zst"
big_dataset_filename = "final_methylation_sorted_with_header.csv.zst"

split_suffix = "_methylated.csv.zst"
to_join_suffix = "_join_methylated.csv.zst"

# I pulled these by reading in the big_dataset and then getting a unique list of the id column
#list_of_ids = big_dataset['id'].unique()
#id_list = [
#    61511, 61525, 61667, 62525, 62534, 62603, 62605, 62618, 62642, 62644, 62645, 79328,
#    79513, 79523, 79530, 79532, 79555, 79556, 79561, 79563, 79566, 79567, 79578, 79590,
#    79591, 79594, 79596, 79619, 79620, 79633, 79665, 79676, 79677, 79684, 79685, 79686,
#    79687, 79688, 79690, 79691, 79692
#]
#print(id_list)

# Ignore this, this is way too memory intensive
#big_dataset = pd.read_csv("final_methylation_sorted_with_header.csv.zst", low_memory=True)
small_dataset = pd.read_csv(small_dataset_filename, dtype={0:"int32[pyarrow]",1:"category",2:"category"})
print("small dataset loaded into ram")
print(small_dataset.head())
print()

#print(big_dataset.memory_usage())
#print(small_dataset.memory_usage())


# Actually, lets read it in, split bigger dataset into smaller ones on disk, then free up ram
# from this huge dataset and continue on
big_dataset_dtype = {0:"int32[pyarrow]",1:"category",2:"category",3:"category",4:"category"}
big_dataset = pd.read_csv(big_dataset_filename, dtype=big_dataset_dtype, low_memory=True)
print("big dataset loaded into ram")
print(big_dataset.head())
print(big_dataset.info())
print()

#big_dataset.to_csv("final_methylation_sorted_with_header_test.csv.zst", compression="zstd", index=False)

# lets check memory usage, set some col types and recheck
#print(big_dataset.info())
#big_dataset['id'] = big_dataset['id'].astype('int32')
#big_dataset['Chromosome'] = big_dataset['Chromosome'].astype('category')
#big_dataset['Site'] = big_dataset['Site'].astype('int32')
#big_dataset['MethylatedCounts'] = big_dataset['MethylatedCounts'].astype('category')
#big_dataset['UnmethylatedCounts'] = big_dataset['UnmethylatedCounts'].astype('category')
#print(big_dataset.info())

result = pd.merge(big_dataset, small_dataset, on="id", how="outer")

print("results info")
print(result.info())
print()

# set up zstd compression options
params = zstandard.ZstdCompressionParameters.from_level(3, window_log=12, enable_ldm=True, threads=-1)
complex_compression = {
    'method': "zstd",
    'compression_params': params
}

print("Saving compressed file to disk")
result.to_csv("final_joined_file_small.csv.zst", compression="zstd", index=False)

print("done, exiting")
exit()


id_list = big_dataset['id'].unique()
print("id list")
for col_id in id_list:
    subsection = big_dataset['id'] == col_id
    subsection.to_csv(f"{col_id}{split_suffix}", compression="zstd", index=False)

# free up ram
#del big_dataset
#gc.collect()

# read in every compressed dataset that was split up
for id in id_list:
    partial_dataset = pd.read_csv(f"{id}{split_suffix}")
    result = pd.merge(partial_dataset, small_dataset, on="id", how="outer")
    #print(pd.merge(small_dataset, big_dataset, on="id", how="outer").head())

    result.to_csv(f"{id}{to_join_suffix}", compression="zstd", index=False)

# Now we just need to join everything together again
# This can be memory intensive because pandas copies data frames when you make a new one
# so we're going to do something different
all_data = []
for id in id_list:
    # read each file and turn it into a dictionary
    df = pd.read_csv(f"{id}{to_join_suffix}")
    all_data.extend(df.to_dict(orient='records'))

# Make it a dataframe
combined_df = pd.DataFrame(all_data,copy=False)

# Write it to a compressed file
combined_df.to_csv("final_joined_file.csv.zst", compression="zstd", index=False)