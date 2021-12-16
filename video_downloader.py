import os
import requests
import pandas as pd
from tqdm import tqdm
import pytube

if __name__ == '__main__':
    # Load the list of YT URLs from the file, loading only the "video_url" column
    url_list = pd.read_csv('./data/yt_video_data.csv', usecols=['video_url'])

    # Create folder to store videos
    if not os.path.exists('./videos'):
        os.makedirs('./videos')

    # Download video from each link listed in `url_list`
    # if not pd.nan by using pytube
    # And name them based on their index number
    # Also use a progress bar to show progress
    for i, url in tqdm(url_list.iterrows(), total=url_list.shape[0]):
        if os.path.exists('./videos/{}.mp4'.format(i)):
            continue
        try:
            yt = pytube.YouTube(url.video_url)

            # Skip if the video is longer than 20 minutes
            print(yt.length)
            if yt.length > 1200:
                continue

            # Get the video in 240p
            video = yt.streams.filter(res='240p').first()
            video.download('./videos')

            # Rename the file to the index number
            os.rename('./videos/{}'.format(video.default_filename), './videos/{}.mp4'.format(i))
        except Exception as e:
            if isinstance(e, KeyboardInterrupt):
                raise e
            print('Error: {}'.format(url.video_url))
            print(e)
            print()
        
    print('Done!')