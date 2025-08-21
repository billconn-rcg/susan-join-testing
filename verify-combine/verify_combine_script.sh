#!/bin/bash

python_exe="../.venv/bin/python"
combine_script="../join_and_compress_two_files.py"
left_input="61511_small.csv.zst"
right_input="reshaped_phenotype_sorted_with_header_uniq.csv.zst"
output_file="test_out.csv.zst"
expected_output="expected_output.csv.zst"

# remove the old output file if it exists
if [ -f "$output_file" ]; then
    rm "$output_file"
fi

# let's run the combine script, but hide all the std output
"$python_exe" "$combine_script" -l "$left_input" -r "$right_input" -o "$output_file" > /dev/null

# verify the output file is the same as the input file
diff -q <(zstdcat "$output_file") <(zstdcat "$expected_output")
