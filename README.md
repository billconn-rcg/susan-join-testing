# susan-join-testing

## info

- split.py
    - This will split a single csv.zst file into one file per unique id
- join_two_files.py
    - This will read two csv.zst files in, create dataframes, and left outer join them on the ID column
- combine.py
    - This will combine every .csv.zst in a folder into a single csv.zst file
    - Should probably re-compress afterwards because --long flag doesn't seem to work. (zstdcat tmp.csc.zst | zstd --long > newfile.csv.zst)
