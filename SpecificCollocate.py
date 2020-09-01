import os
import csv
import tqdm
import json
import glob
import pprint
import argparse
import constants as co
from collections import Counter
from multiprocessing import Pool
from AnalyserTemplate import AnalyserTemplate

class SpecificCollocate(AnalyserTemplate):
    """This analysis, when given a list of 'words of interest', counts the frequency of each word or phrase appearing before 
    or after the word of interest. For example, if the word of interest was 'the', 'n' was set to 2, and the sentence was
    "the quick brown fox jumps over the lazy dog", we would get:

        1 * "quick brown"
        1 * "lazy dog"

    These frequencies are then measured against the frequency of the word of interest alone to calculate the relative frequency,
    which gives a measure of the strength of the link between a word and it's collocate.

    """
    
    def execute(self):
        """Executes the specific collocate frequency analysis.

        Using the multiprocessing library, count the frequency of word or phrases appearing before or after any of
        the words listed in the text file specified in the configs.
        """

        # initialise configurations
        self.task_configs = self.configs["collocate"]
        self.dest_filename = os.path.join(self.results_folder, self.task_configs["dest_filename"].split('.')[0])
        self.n = self.task_configs["n"]

        # load additional resources
        self.words = self.load_words_of_interest(self.task_configs["words_of_interest_filename"])
        self.freq = self.load_word_freq(os.path.join(self.resource_folder, self.task_configs["frequency_filename"]))

        # multiprocess chunk by chunk
        with Pool(self.configs["n_processors"]) as p:
            counts = []
            for count in tqdm.tqdm(p.imap_unordered(self.process, self.pool), total=len(self.pool)):
                counts.append(count)

        # initialise master count 
        self.master_count = {word: Counter() for word in self.words}

        # re-merge counters from multiprocessing
        for count in counts:
            for word in self.master_count:
                self.master_count[word] += count[word]
        
        self.save()

    def process(self, file):
        """Processes a single chunk, counting the frequency of the collocates.

        Arguments:
            file {str} -- path to pre-processed resource file

        Returns:
            dict{str:Counter} -- collocate frequency results
        """

        # initialise counter
        count = {word: Counter() for word in self.words}

        # iterate over lines in chunk
        with open(file, encoding='UTF-8') as f:
            for line in f:

                # process line
                count = self.process_line(count, line, self.words, self.n)

        return count

    def save(self):
        """Saves the results to csv at the path specified by the configs.
        """

        # create results folder
        if not os.path.isdir(self.results_folder):
            os.mkdir(self.results_folder)

        with open(self.dest_filename.format(l=self.language) + '.csv', 'w', encoding='UTF-8', newline='') as f:

            # instantiate csv writer
            writer = csv.writer(f)

            # create column headings
            headings = ['word', 'count']
            for i in range(1,11):
                headings.append("collocate_{}".format(i)) 
                headings.append("count_{}".format(i)) 
                headings.append("relative_frequency_{}".format(i)) 
            writer.writerow(headings)

            # iterate over words of interest
            for word, cnt in self.master_count.items():

                # build row
                row = [word, cnt['TOTAL']]
                for w, c in cnt.most_common(11)[1:]:
                    row.append(w)
                    row.append(c)
                    try:
                        row.append(c/self.freq[abs(self.n)][w])
                    except KeyError:
                        row.append(0)

                # write row to csv
                writer.writerow(row)

    @staticmethod
    def process_line(count, line, words, n):
        """Processes a single line and updates the frequency count with any matched collocates.

        Arguments:
            count {Counter} -- frequency count
            line {str} -- the sentence to be analysed
            words {list[str]} -- words of interest
            n {int} -- number of words before or after word of interest

        Returns:
            Counter -- updated frequency count
        """
        
        # get word list from string
        line = line.split()

        # iterate over words in sentence
        for i, word in enumerate(line):

            # check if word is of interest
            if word in words:

                # increment total counter
                count[word]['TOTAL'] += 1

                # do forwards and backwards index logic
                neg = n if n < 0 else 1
                pos = n + 1 if n > 0 else 0

                try:

                    # catch outside range limits
                    if i-neg < 0: 
                        raise IndexError()
                    elif i+pos > len(line)-1:
                        raise IndexError()
                    
                    # generate phrase
                    phrase_list = line[i+neg:i+pos]
                    phrase = " ".join(phrase_list)
                    
                    # increment counter
                    count[word][phrase] += 1

                except IndexError:
                    pass

        return count

    @staticmethod
    def load_words_of_interest(words_file):
        """Loads the word of interest and parse into list.

        Arguments:
            words_file {str} -- path to the words of interest file

        Returns:
            list[str] -- the lits of words of interest
        """
        with open(words_file, 'r') as f:
            words = list(f)
        return [word.strip().lower() for word in words]
