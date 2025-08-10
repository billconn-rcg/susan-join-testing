from unittest import TestCase
import slow_low_memory_csv_combining
import os


class TestFileListReader(TestCase):
    def setUp(self):
        print(os.getcwd())
        filenames = [
            "./output/79530_joined.csv.zst",
            "./output/79532_joined.csv.zst"
        ]
        self.test_reader = slow_low_memory_csv_combining.FileListReader(filenames)

    def tearDown(self):
        self.test_reader.close()

    def test_readline(self):
        self.assertEqual("61511,,,,,condition,0.10910715657945\n", self.test_reader.readline())

    def test_full_file(self):
        with open("test_output.csv", "r") as f:
            self.assertEqual(f.readline(), self.test_reader.readline())