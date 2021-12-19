import argparse
import pickle

import numpy as np
import pandas as pd
import torch
from torch.utils.data import DataLoader
import tqdm

from datetime import datetime, timedelta
from data_handling import ImageDataset, img_transform
from models import ImageFeatureExtractor


DEVICE = 'cuda:0'


def parse_args():
  parser = argparse.ArgumentParser(description='Prepares scraped data for use.')
  parser.add_argument('-v', '--video_data_file', type=str, default='data/yt_video_data.csv',
                      help='File to that contains video data')
  parser.add_argument('-c', '--channel_data_file', type=str, default='data/yt_channel_data.csv',
                      help='File to that contains channel data')
  parser.add_argument('-o', '--output_file', type=str, default='data/full_data.csv',
                      help='File to write full data to')
  parser.add_argument('-ot', '--output_thumbnail_features', type=str, default='data/thumbnail_features.pkl',
                      help='Where to write the thumbnail features to')

  return parser.parse_args()

channel_column_changes = {
    'title': 'vid_page_titles',
    'upload_date': 'vid_page_upload_date',
    'view_count': 'vid_page_views',
    'scrape_date': 'channel_scrape_date',
}

def yt_label_to_datetime(label, reference_date=None):
    """Converts YT formatted date strings into datetime objects."""
    if label is None:
        return None

    time_str = label.lower()
    if len(time_str.split(' ')) > 3 and 'premiere' not in time_str:
        time_str = ' '.join(time_str.split(' ')[-3:])

    if reference_date and 'ago' in time_str:
        amt, period, _ = time_str.split(' ')
        amt = int(amt)
        if period[-1] != 's':
            period += 's'
        
        if period == 'months':
            period = 'days'
            amt *= 30
        elif period == 'years':
            period = 'days'
            amt *= 365

        target_time = reference_date - timedelta(**{period: amt})
    elif 'premiere' in time_str:
        target_time = None
    else:
        target_time = datetime.strptime(time_str, '%b %d, %Y')

    return target_time

def format_dates(df, column_name, ref_column_name=None):
    # Dates should be a series
    drop_idxs = df[column_name].apply(lambda x: 'stream' in x.lower())
    df = df[~drop_idxs]
    drop_idxs = df[column_name].apply(lambda x: 'premiere' in x.lower())
    df = df[~drop_idxs]

    if ref_column_name is not None:
        lambda_func = lambda x: yt_label_to_datetime(x[column_name], x[ref_column_name])
        df[column_name] = df.apply(lambda_func, axis=1)
    else:
        lambda_func = lambda x: yt_label_to_datetime(x)
        df[column_name] = df[column_name].apply(lambda_func)
    return df



if __name__ == '__main__':
    args = parse_args()

    # Load the data
    video_df = pd.read_csv('data/yt_video_data.csv', index_col=0)
    channel_df = pd.read_csv('data/yt_channel_data.csv', index_col=0)

    thumbnail_dataset = ImageDataset(root_dir='thumbnails', transform=img_transform)
    thumbnail_loader = DataLoader(thumbnail_dataset, batch_size=64, shuffle=False)

    feature_extractor = ImageFeatureExtractor().to(DEVICE)

    # Generate features for the thumbnails
    thumbnail_feature_idxs = []
    thumbnail_features = []
    print('Generating thumbnail features...')
    for img_names, imgs in tqdm.tqdm(thumbnail_loader):
        imgs = imgs.to(DEVICE)
        with torch.no_grad():
            features = feature_extractor(imgs)
        features = features.cpu().numpy()
        thumbnail_features.append(features)
        feature_idxs = [int(img_name.split('.')[0]) for img_name in img_names]
        thumbnail_feature_idxs.extend(feature_idxs)
    thumbnail_features = np.concatenate(thumbnail_features, axis=0)
            
    # Save the thumbnail features as a pickle
    print('Saving thumbnail features')
    with open(args.output_thumbnail_features, 'wb') as f:
        pickle.dump((thumbnail_feature_idxs, thumbnail_features), f)

    # Change channel df column names to not overlap with the video df
    for old_name, new_name in channel_column_changes.items():
        channel_df[new_name] = channel_df[old_name]
        channel_df.drop(old_name, axis=1, inplace=True)

    # Merge the two dataframes
    full_df = pd.merge(video_df, channel_df, how='left', on=['channel_name', 'channel_link'])

    # Drop NA rows and remove duplicate entries
    full_df = full_df.dropna()
    full_df = full_df.drop_duplicates(subset=['video_url', 'video_title'])

    # Format dates
    full_df = format_dates(full_df, 'scrape_date')
    full_df = format_dates(full_df, 'channel_scrape_date')
    full_df = format_dates(full_df, 'date', 'scrape_date')
    full_df['time_up'] = full_df['scrape_date'] - full_df['date']
    full_df['time_up'] = full_df['time_up'].apply(lambda x: x.total_seconds())
    
    full_df.reset_index(inplace=True)
    full_df = full_df.rename(columns={'index': 'feature_id'})

    # Save the data
    print('Saving {} total entries to {}'.format(len(full_df), args.output_file))
    full_df.to_csv(args.output_file, index=False)