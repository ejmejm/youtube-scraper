import os
import requests
import pandas as pd
from tqdm import tqdm

if __name__ == '__main__':
    # Load the list of YT URLs from the file, loading only the "thumbnail_link" column
    url_list = pd.read_csv('./data/yt_video_data.csv', usecols=['thumbnail_link'])

    # Create folder to store thumbnails
    if not os.path.exists('thumbnails'):
        os.makedirs('thumbnails')

    # Download thumbnails from each link listed in `url_list` if not pd.nan
    # And name them based on their index number
    # Also use a progress bar to show progress
    for index, url in tqdm(url_list.iterrows(), total=url_list.shape[0]):
        if not pd.isnull(url['thumbnail_link']):
            r = requests.get(url['thumbnail_link'])
            with open(f'thumbnails/{index}.jpg', 'wb') as f:
                f.write(r.content)
    
    print('Done!')