import argparse
from Frequency import Frequency
from PoSFrequency import PoSFrequency
from GeneralCollocate import GeneralCollocate
from SpecificCollocate import SpecificCollocate

analysers = {
    "frequency": Frequency,
    "pos_frequency": PoSFrequency,
    "general_collocate": GeneralCollocate,
    "specific_collocate": SpecificCollocate
    }

def analyse():

    # get arguments
    parser = argparse.ArgumentParser(description="Select the analysis type, language and dataset that you wish to execute...")
    parser.add_argument(dest="type", choices=["frequency", "pos_frequency", "specific_collocate", "general_collocate"])
    parser.add_argument("-l", "--language", dest="language", choices=['nl', 'en', 'es', 'de', 'fr', 'pl', 'it', 'no', 'pt', 'sv', 'ru'], required=True)
    parser.add_argument("-d", "--dataset", dest="dataset", required=True)
    args = parser.parse_args()
    
    # define analyser
    analyser = analysers[args.type]

    # execute analysis
    analyser(args.language, args.dataset).execute()


if __name__ == "__main__":
    
    analyse()
