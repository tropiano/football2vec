"""
Test the data handler for the football2vec project.

This script tests the data handler for the football2vec project by loading event data 
from a specified dataset path and saving it to a dataframe as a CSV file.
"""

from lib.data_handler import load_all_events_data, load_players_metadata
from lib.data_handler_revpool import load_all_events_data as load_all_events_data_revpool

data_file = "../../data/belgium-1.tar.gz"

# this part test the load_all_events_data function
#df = load_all_events_data(verbose=True, dataset_path="data/statsbomb_small/data")
#df.to_csv("../../data/test_statsbomb.csv")

df = load_all_events_data_revpool(verbose=True, data_file=data_file)
df.to_csv("../../data/test_revpool.csv")

# df = load_players_metadata(verbose=True, dataset_path="data/statsbomb_small")
# 

