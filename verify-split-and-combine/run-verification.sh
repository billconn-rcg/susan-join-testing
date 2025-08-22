#!/bin/bash

python_exe="../.venv/bin/python"
input_file="./input.csv.zst"
#input_file="./hand-build-test-file.csv.zst"
tmp_file="./tmpfile.csv.zst"
output_file="./output.csv.zst"
tmpdir="tmpdir/"
split_script="../split.py"
join_script="../combine.py"

# ensure tmpdir exists
mkdir -p $tmpdir

# remove output file if it exists
[ -f "$output_file" ] && rm $output_file

# split file into multiple files within the tmpdir
echo "Splitting file..."
$python_exe $split_script -i $input_file -f $tmpdir -s "_methyl.csv.zst"

# combine split files back into one
echo "Joining files"
$python_exe $join_script -i $tmpdir -o $tmp_file
zstdcat $tmp_file | zstd --long > $output_file
rm $tmp_file

# remove tmpdir
[ "$tmpdir" ] && rm -rf "$tmpdir"

# verify the two are the same with some kind of hash
echo $(zstdcat $input_file | md5sum -) $input_file
echo $(zstdcat $output_file | md5sum -) $output_file
echo $(zstdcat $input_file | sha256sum -) $input_file
echo $(zstdcat $output_file | sha256sum -) $output_file
