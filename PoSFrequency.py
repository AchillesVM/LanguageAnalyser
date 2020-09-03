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

class PoSFrequency(AnalyserTemplate):
    """This analysis measures how frequently each part of speech appears after another part of speech.
    By tagging and analysing consecutive words, a table can be created to illustrate which PoS most commonly 
    appears before or after another. For example, we may learn that 95% of the time an adverb appears, it comes 
    before a verb.

    """

    def execute(self):
        """Executes the part of speech frequency analysis.

        Using the multiprocessing library, count the frequency of the parts of speech 
        of pairs of consecutive words and save the results to CSV.
        """

        # initialise configurations
        self.task_configs = self.configs["pos_frequency"]
        self.dest_filename = os.path.join(self.results_folder, self.task_configs["dest_filename"].split('.')[0])

        # create results folder
        if not os.path.isdir(self.results_folder):
            os.mkdir(self.results_folder)

        # multiprocess chunk by chunk
        with Pool(self.configs["n_processors"]) as p:
            counts = []
            for count in tqdm.tqdm(p.imap_unordered(self.process, self.pool), total=len(self.pool)):
                counts.append(count)

        # initialise master count 
        self.master_count = {word: Counter() for word in co.POS_TAGS}

        # re-merge counters from multiprocessing
        for count in counts:
            for word in self.master_count:
                self.master_count[word] += count[word]


        self.save()

    def process(self, file):
        """Processes a single chunk, counting the frequency of part of speech pairs.

        Iterating on each pair of consecutive words, record the frequency of the PoS tags.
        For example, "the quick brown fox" has the tags "PRP JJ JJ NN". This would give:

            1 * PRP > JJ 
            1 * JJ > JJ
            1 * JJ > NN 

        Arguments:
            file {str} -- path to pre-processed resource file

        Returns:
            dict{str:Counter} -- part of speech frequency results
        """

        # initialise counter
        count = {tag: Counter() for tag in co.POS_TAGS}

        # iterate over lines in chunk
        with open(file, 'r', encoding=self.configs["encoding"][self.language]) as f:
            for line in f:

                # generate groups and increment counter
                for first, second in self.generate_groups(line.split()):
                    try:
                        count[first][second] += 1
                    except KeyError:
                        pass
        return count

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
            writer.writerow(["PoS"] + list(self.master_count.keys()))

            # iterate through words and write to file
            for first, count in self.master_count.items():    
                total_count = max(self.total_count(count), 1)
                writer.writerow([first] + [count[second]/total_count for second in self.master_count.keys()])

    def generate_groups(self, line):
        """Generates every pair of consecutive words in a line.

        For example, ["the", "quick", "brown", "fox"] would yield:

            "the", "quick"
            "quick", "brown"
            "brown", "fox"

        Arguments:
            line {list[str]} -- the pre-processed, untagged sentence

        Yields:
            str, str -- the first and second word in the pair
        """

        # yield each pair of consecutive words
        line = nltk.pos_tag(line)
        for i in range(len(line)-1):
            yield line[i][1], line[i+1][1]
