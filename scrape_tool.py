import os
import random
from scraping import YTSManager
import argparse
import time
import pandas as pd


# Defines the following arguements:
#  - search_terms_file: The file containing the search terms to be used
#  - output_file: The file to write the results to
#  - n_threads: The number of threads to use
def parse_args():
  parser = argparse.ArgumentParser(description='Scrapes the YTS website for torrents')
  parser.add_argument('-s', '--search_terms_file', type=str, default='start_words.txt',
                      help='File containing search terms')
  parser.add_argument('-vo', '--video_output_file', type=str, default='data/yt_video_data.csv',
                      help='File to write video data to')
  parser.add_argument('-co', '--channel_output_file', type=str, default='data/yt_channel_data.csv',
                      help='File to write channel data to')
  parser.add_argument('-n', '--n_threads', type=int, default=4,
                      help='Number of threads to use')
  parser.add_argument('-v', '--scrape_videos', action='store_true',
                      help='Whether to scrape video data')
  parser.add_argument('-c', '--scrape_channel', action='store_true',
                      help='Whether to scrape channel data')
  parser.set_defaults(scrape_videos=False, scrape_channel=False)

  return parser.parse_args()

def load_search_terms(file_path):
  with open(file_path, 'r') as f:
    lines = f.readlines()
    
  search_terms = [term.strip() for term in lines]
  return search_terms

if __name__ == '__main__':
  args = parse_args()
  # Do video searching and scraping
  if args.scrape_videos:
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

      print(os.path.exists(args.video_output_file), os.path.isfile(args.video_output_file))
      if os.path.exists(args.video_output_file) and \
         os.path.isfile(args.video_output_file):
        print('Reading existing data')
        old_df = pd.read_csv(args.video_output_file, index_col=0)

      df = pd.concat([df, old_df], axis=0)
      df = df.drop_duplicates()

      # Reset the index
      df = df.reset_index(drop=True)

      print('# videos scraped:', str(len(df)))
      df.to_csv(args.video_output_file)

  # Do channel scraping
  if args.scrape_channel:
    # Check if the output file exists
    if not os.path.exists(args.video_output_file):
      raise Exception('You must generate a video data file before scraping channels')

    # Load the video data
    df = pd.read_csv(args.video_output_file, index_col=0)
    channel_data = df[['channel_name', 'channel_link']]
    channel_data.drop_duplicates(inplace=True)

    channel_names = channel_data['channel_name'].tolist()
    channel_links = channel_data['channel_link'].tolist()
    # video_page_links = channel_data['channel_link'].apply(lambda x: x + '/videos').tolist()

    manager = YTSManager()
    try:
      manager.start_channel_scrape_loops(channel_names, channel_links, n_workers=args.n_threads)
      while True:
          manager.print_channel_status()
          time.sleep(5)
    except KeyboardInterrupt:
      manager.stop_channel_scraping()
      print('\n\nStopped scraping')
    finally:
      print('Saving data')
      df = manager.get_channel_dataframe()
      df = df.drop_duplicates()
      print('# channels scraped:', str(len(df)))
      df.to_csv(args.channel_output_file)


    