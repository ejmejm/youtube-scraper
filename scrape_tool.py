import random
from scraping import YTSManager
import argparse
import time

# Defines the following arguements:
#  - search_terms_file: The file containing the search terms to be used
#  - output_file: The file to write the results to
#  - n_threads: The number of threads to use
def parse_args():
  parser = argparse.ArgumentParser(description='Scrapes the YTS website for torrents')
  parser.add_argument('-s', '--search_terms_file', type=str, required=True,
                      help='File containing search terms')
  parser.add_argument('-o', '--output_file', type=str, required=False, default='yt_data.csv',
                      help='File to write results to')
  parser.add_argument('-n', '--n_threads', type=int, required=False, default=8,
                      help='Number of threads to use')
  return parser.parse_args()

def load_search_terms(file_path):
  with open(file_path, 'r') as f:
    lines = f.readlines()
    
  search_terms = [term.strip() for term in lines]
  return search_terms

if __name__ == '__main__':
  args = parse_args()
  try:
    manager = YTSManager()
    search_terms = load_search_terms(args.search_terms_file)

    if args.n_threads > len(search_terms):
        args.n_threads = len(search_terms)

    search_terms = random.sample(search_terms, args.n_threads)
    manager.start_scrape_loops(search_terms)
    print('#' * 80)
    print('Press ctrl+c to quit')
    print('#' * 80)
    while True:
        manager.print_status()
        time.sleep(5)
  except KeyboardInterrupt:
    manager.stop_scraping()
    print('\n\nStopped scraping')
  finally:
    print('Saving data')
    df = manager.get_dataframe()
    df = df.drop_duplicates()
    print('# videos scraped:', str(len(df)))
    df.to_csv(args.output_file)