import os
import re
import csv
import tqdm
import json
import glob
import pprint
import argparse
import constants as co
from shutil import rmtree
from collections import Counter
from multiprocessing import Pool


class AnalyserTemplate:
    """This is the template for all analyses which handles the configuration initialisation and the 
    loading and pre-processing of the resource file(s). 
    """

    def __init__(self, language, resource_type):

        # initialise settings
        self.language = language
        self.resource_type = resource_type
        self.init_configs()

        # load resource
        self.load_resource()

    def init_configs(self):
        """Parses configuration file and loads some frequently used fields to attributes.
        """

        # get current location
        self.script_dir = os.path.dirname(__file__)

        # load configuration file
        with open(os.path.join(self.script_dir, "config.json")) as f:
            self.configs = json.load(f)
        
        # load some configs as attributes
        self.resource_folder = os.path.join(self.script_dir, self.configs["resource_path"], self.resource_type, self.language)
        self.pre_processed_folder = os.path.join(self.resource_folder, self.language + '_' + self.configs["pre_processed_path"])
        self.chunk_size = self.configs["resources"][self.resource_type]["chunk_size"]

    def load_resource(self):
        """Loads the filenames of any pre-processed resource files.

        First, check if pre-processed files exist and execute the preprocessing if not.
        Finally, load the paths to the pre-processed files.
        """

        # check resource folder exists
        if not os.path.isdir(self.resource_folder):
          raise Exception("Resource folder not found...")
        
        # check if pre-processed folder exists
        if not os.path.isdir(self.pre_processed_folder) or not os.listdir(self.pre_processed_folder):
            self.pre_process_resource()

        # get list of matching resource file names
        self.pool = glob.glob(os.path.join(self.pre_processed_folder, self.language + "_part_*"))

    def pre_process_resource(self):
        """Pre-processes the resource file using the multiprocessing library.
        """

        # get list of files matching pattern
        files = glob.glob(os.path.join(self.resource_folder, self.language + ".*"))

        # raise error if no files found
        if not files:
            raise FileNotFoundError("No resource files found...")

        # create preprocessed folder if it doesn't exist
        if not os.path.exists(self.pre_processed_folder):
            os.mkdir(self.pre_processed_folder)

        # record number of chunks from previous files
        chunk_hist = 0

        # open valid files
        for filename in files:

            self.file_size = os.path.getsize(filename)
            self.slice_size = self.file_size / 12

            # check chunk size
            if self.file_size / self.chunk_size > 500:
                print("Warning, this will create {} partitions.".format(int(self.file_size / self.chunk_size)))
                rmtree(self.pre_processed_folder)
                raise ValueError("This file will create more than 500 partitions, consider increasing the chunk size...")

            # process
            pool = Pool(self.configs["n_processors"])
            for _ in tqdm.tqdm(pool.imap(self.pre_process_chunk, self.generate_chunk(filename)), total=int(self.file_size / self.chunk_size)):
                pass
            

    def generate_chunk(self, filename):
        """Loads the raw resource file and yields chunks of the specified size in bytes.

        Arguments:
            filename {str} -- path to the raw resource file

        Yields:
            str -- the chunk of text
        """

        # open resource file in binary
        with open(filename, 'rb') as resource:

            # instantiate chunk start byte and trailing line string
            p = 0
            overlap = ''

            while p <= self.file_size:

                try:
                    if self.file_size - p < self.chunk_size:
                        buffer = overlap + resource.read(self.file_size - p).decode("UTF-8")
                    else:
                        buffer = overlap + resource.read(self.chunk_size).decode("UTF-8")
                except:
                    p += self.chunk_size
                    continue

                # remove and store trailing sentence
                buffer, overlap = buffer.rsplit('\n', maxsplit=1)

                yield buffer

                p += self.chunk_size

    def pre_process_chunk(self, chunk):
        """[summary]

        Arguments:
            chunk {str} -- the chunk of text to be processed
        """
        
        text = ""
        for line in chunk.splitlines():
            text += self.pre_process_line(line) + "\n"

        for i in range(1000):
            fn = os.path.join(self.pre_processed_folder, self.language + "_part_{0:0=3d}".format(i) + ".txt")
            if not os.path.exists(fn):
                with open(fn, 'w', encoding="UTF-8") as chunk_file:
                    chunk_file.write(text)
                break

    def pre_process_line(self, line):
        """Pre-processed one line of text by normalising characters and stripping any metadata.

        Arguments:
            line {str} -- the raw sentence

        Returns:
            str -- the pre-processed sentence
        """

        line = line.lower()
        line = line.translate(co.NORM_TABLE)
        line = line.translate(co.PUNC_TABLE)
        line = line.split()
        line = line[self.configs["resources"][self.resource_type]["lstrip"]:]
        if self.configs["resources"][self.resource_type]["rstrip"]:
            line = line[:-self.configs["resources"][self.resource_type]["rstrip"]]
        return " ".join(line)

    def load_word_freq(self, file_path):
        """Loads the previously calculated phrase frequency data for phrases up to 8 words long.
        
        NOTE: Only 1 word phrases currently in use.

        Arguments:
            file_path {str} -- path to the frequency data

        Returns:
            dict{str:dict{str:str}} -- frequency of each phrase up to 8 words long
        """

        # initialise frequency dict
        # freq = {i: {} for i in range(1,9)}
        freq = {1: {}}

        # read csv file
        with open(file_path, 'r', encoding=co.OUT_ENCODING[self.language]) as f:
            reader = csv.reader(f, delimiter=',')

            # skip header
            next(reader)

            # iterate through csv and add values to dictionary
            for row in reader:
                try:
                    # freq[int(row[3])][row[0]] = int(row[1])
                    freq[1][row[0]] = int(row[1])
                except ValueError:
                    pass

        return freq

    @staticmethod
    def total_count(count):
        """Returns the sum of all the counts in the counter.

        Arguments:
            count {Counter} -- the counter to be summed

        Returns:
            int -- the total count
        """
        return sum(count.values())


if __name__ == "__main__":

    analyser = RepetitioAnalyserTemplate('es', 'subtitles')
 