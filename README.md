# susan-join-testing

## info

- naive_split_file
    - This will split a single csv.zst file into one file per unique id
- join_and_compress
    - This will read two csv.zst files in, create dataframes, and left outer join them on the ID column
- slow_low_memory_csv_combining
    - This will combine every .csv.zst in a folder into a single csv.zst file
    - Should probably re-compress afterwards because --long flag doesn't seem to work. (zstdcat tmp.csc.zst | zstd --long > newfile.csv.zst)
