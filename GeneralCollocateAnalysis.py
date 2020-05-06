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

class GeneralCollocateAnalysis(AnalyserTemplate):
    """This analysis attempts to find the 'strongest collocates', that is a grouping of words which appears far more
    commonly together than individually. Using the top 1000 most commonly appearing words, their collocates are counted
    and finally each grouping is sorted by their relative frequency to give the 'strongest collocates'.

    """
    
    def execute(self):
        """Executes the general collocate frequency analysis.

        Using the multiprocessing library, count the frequency of word or phrase appearing before or after any of
        the 1000 most frequently occuring words in the resource. Calculate their relative frequencies and return the
        collocates with the highest relative frequency.
        """

        # initialise configurations
        self.task_configs = self.configs["general_collocate_analysis"]
        self.dest_filename = os.path.join(self.resource_folder, self.task_configs["dest_filename"].split('.')[0])
        self.n = self.task_configs["n"]

        # load additional resources
        self.freq = self.load_word_freq(os.path.join(self.resource_folder, self.task_configs["frequency_filename"].format(l=self.language)))
        self.words = list(self.freq[1].keys())

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
                count = self.process_line(count, line, self.words)

        # print([count[key].most_common(5) for key in count])
        return count

    def save(self):
        """Saves the results to csv at the path specified by the configs.
        """

        print("Adding words to dataframe...")
        
        # initialise dataframe
        df = pd.DataFrame(columns=["word", "collocate", "count", "relative_frequency"])

        # iterate over words of interest and their collocate counts
        for word, count in tqdm.tqdm(self.master_count.items(), total=1000):

            # iterate over most common collocates
            for collocate, c in count.most_common(100):

                # calculate relative frequency
                try:
                    rf = c/self.freq[1][word]
                
                # set relative frequency to 0 if not found in frequency analysis
                except KeyError:
                    rf = 0
                
                # add line to df
                df = df.append(pd.Series([word, collocate, c, rf], index=df.columns), ignore_index=True)

        print("Sorting dataframe and saving to csv...")

        # enrich with word types
        df["word_type"] = df["word"].apply(lambda w: self.tag(w))
        df["collocate_type"] = df["collocate"].apply(lambda w: self.tag(w))

        # order by relative frequency and save to csv
        df = df.sort_values(by="relative_frequency", ascending=False)
        df.to_csv(self.dest_filename.format(l=self.language) + '.csv', ',', index=False, encoding=co.OUT_ENCODING[self.language])

    @staticmethod
    def tag(word):
        """Return the PoS tag for the word specified. Returns "N/A" for any words not recognised.

        Arguments:
            word {str} -- the word to be tagged

        Returns:
            str -- the abbreviated part of speech tag
        """
        try:
            tag = nltk.pos_tag([word])[0][1]
        except IndexError:
            tag = "N/A"

        return tag


    @staticmethod
    def process_line(count, line, words):
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

                for n in [-2, -1, 1, 2]:

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

if __name__ == "__main__":

    GeneralCollocateAnalysis("es", "subtitles").execute()
