# Language Analysis Suite
WIP collection of language analysis scripts.

## Description
This analysis suite uses the multiprocessing library to parallel process large collections of sentences. New datasets are first pre-processed to normalise the file structure and split the file into chunks for paralellisation.

## Analysis Types
### Frequency
This analysis counts the frequency of occurence of groups of words, the size of which is defined in config.json. To be counted, the phrases must be fully contained within a single sentence.

### Part-of-Speech Frequency
This analysis measures how frequently each part of speech appears after another part of speech. By tagging and analysing consecutive words, a table can be created to illustrate which PoS most commonly appears before or after another. For example, we may learn that 95% of the time an adverb appears, it comes before a verb.

### Specific Collocate
This analysis, when given a list of 'words of interest', counts the frequency of each word or phrase appearing before or after the word of interest. For example, if the word of interest was 'the', 'n' was set to 2, and the sentence was "the quick brown fox jumps over the lazy dog", we would get:

        1 * "quick brown"
        1 * "lazy dog"

These frequencies are then measured against the frequency of the word of interest alone to calculate the relative frequency, which gives a measure of the strength of the link between a word and it's collocate.

### General Collocate
This analysis attempts to find the 'strongest collocates', that is a grouping of words which appears far more commonly together than individually. Using the top 1000 most commonly appearing words, their collocates are counted and finally each grouping is sorted by their relative frequency to give the 'strongest collocates'.

## Requirements

- multiprocessing>=16.6.0
- tqdm>=4.45.0
- pandas>=1.0.3
- nltk>=3.4.5

## Usage
When using a new dataset, the resource file(s) must me located in /resources/*dataset_name*/*language*/, where the language is represented by the ISO 639-1 code. A new entry will be required in config.json to define the chunk size and the number of columns to remove from the beginning or end of each line (some datasets have appended or prepended indices).

To run an analysis, first confirm the task configurations in config.json, then execute:

    $ python analyse frequency --dataset tatoeba --language en
    $ python analyse general_collocate -d subtitles -l es


## Supported Languages
- English
- Spanish
- French
- Dutch
- German
- Polish
- Italian
- Norwegian
- Portuguese
- Swedish
- Russian