import os
import csv
import tqdm
import json
import glob
import nltk
import pprint
import argparse
import pandas as pd
import constants as co
from collections import Counter
from multiprocessing import Pool
from AnalyserTemplate import AnalyserTemplate

class Frequency(AnalyserTemplate):
    """This analysis performs a basic frequency count on words or phrases of any length.
    """

    def execute(self):
        """Executes the general collocate frequency analysis.

        Using the multiprocessing library, count the frequency of each word or phrase in the resource file.
        """

        # initialise configurations
        self.task_configs = self.configs["frequency"]
        self.dest_filename = os.path.join(self.results_folder, self.task_configs["dest_filename"].split('.')[0])

        # multiprocess chunk by chunk
        with Pool(self.configs["n_processors"]) as p:
            counts = []
            for count in tqdm.tqdm(p.imap_unordered(self.process, self.pool), total=len(self.pool)):
                counts.append(count)

        # initialise master counter
        self.master_count = Counter()

        # merge counts from multiprocessing
        for count in counts:
            self.master_count += count

        self.save()

    def process(self, file):
        """Processes a single chunk, counting the frequency of each word.

        Arguments:
            file {str} -- path to pre-processed resource file

        Returns:
            dict{str:Counter} -- frequency results
        """

        # get phrase length
        l = self.task_configs["phrase_length"]

        # initialise counter
        count = Counter()

        # iterate over lines in chunk
        with open(file, 'r', encoding="UTF-8") as f:
            for line in f:

                # generate groups and increment counter
                for group in self.generate_groups(line.split(), l):
                    count[group] += 1

        return self.reduce_count(count)

    def save(self):
        """Saves the results to csv at the path specified by the configs.
        """

        # create results folder
        if not os.path.isdir(self.results_folder):
            os.mkdir(self.results_folder)

        # initialise csv writer
        with open(self.dest_filename.format(l=self.language) + '.csv', 'w', encoding=co.OUT_ENCODING[self.language], newline='') as f:
            writer = csv.writer(f)

            # write header
            writer.writerow(['phrase', 'type', 'count', 'relative_frequency'])

            # calculate total count
            total_count = self.total_count(self.master_count)

            # iterate through words and write to file
            for w, c in self.master_count.most_common(self.task_configs["n_most_common"]):    
                writer.writerow([w, nltk.pos_tag([w])[0][1], c, c/total_count])

    def reduce_count(self, cnt):
        """Returns the count, keeping only values whose count exceeds the threshold.
        
        To limit memory usage, some of the least frequently occuring words must be discarded.
        After each chunk has finished processing, this function is called to filter out any very
        low frequency words.

        Arguments:
            cnt {Counter} --  The full count

        Returns:
            Counter -- Only the values whose count was over the threshold
        """
        
        # get threshold from configs
        thresh = self.task_configs["discard_threshold"]

        # filter out counts <= threshold
        count = Counter({key: count for key, count in cnt.items() if count > thresh})
        
        return count

    def generate_groups(self, line, l):
        """Generates valid group of consecutive words in a line.

        For example, ["the", "quick", "brown", "fox"] with l=2 would yield:

            "the quick"
            "quick brown"
            "brown fox"

        Arguments:
            line {list[str]} -- the pre-processed, untagged sentence

        Yields:
            str -- the word or phrase
        """

        # create phrases of l words from sentence string
        for i in range(len(line)-l):
            s = ' '.join(line[i:i+l])
            yield s
