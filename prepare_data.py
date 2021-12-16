import argparse
import pandas as pd
import torch
from torch.utils.data import DataLoader
import tqdm

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

  return parser.parse_args()

channel_column_changes = {
    'title': 'vid_page_titles',
    'upload_date': 'vid_page_upload_date',
    'view_count': 'vid_page_views',
    'scrape_date': 'channel_scrape_date',
}

if __name__ == '__main__':
    args = parse_args()

    # Load the data
    video_df = pd.read_csv('data/yt_video_data.csv', index_col=0)
    channel_df = pd.read_csv('data/yt_channel_data.csv', index_col=0)

    thumbnail_dataset = ImageDataset(root_dir='thumbnails', transform=img_transform)
    thumbnail_loader = DataLoader(thumbnail_dataset, batch_size=64, shuffle=False)

    feature_extractor = ImageFeatureExtractor().to(DEVICE)

    # Generate features for the thumbnails
    thumbnail_features = {}
    print('Generating thumbnail features...')
    for img_names, imgs in tqdm.tqdm(thumbnail_loader):
        imgs = imgs.to(DEVICE)
        with torch.no_grad():
            all_features = feature_extractor(imgs)
        all_features = all_features.cpu().detach().numpy()
        for img_name, img_features in zip(img_names, all_features):
            img_idx = int(img_name.split('.')[0])
            thumbnail_features[img_idx] = img_features

    # Add thumbnail embeddings to the video dataframe
    video_df['thumbnail_embedding'] = video_df.apply(lambda x: thumbnail_features.get(x.name), axis=1)

    # Change channel df column names to not overlap with the video df
    for old_name, new_name in channel_column_changes.items():
        channel_df[new_name] = channel_df[old_name]
        channel_df.drop(old_name, axis=1, inplace=True)

    # Merge the two dataframes
    full_df = pd.merge(video_df, channel_df, how='left', on=['channel_name', 'channel_link'])

    # Drop NA rows and remove duplicate entries
    full_df = full_df.dropna()
    full_df = full_df.drop_duplicates(subset=['video_url', 'video_title'])

    # Save the data
    print('Saving {} total entries to {}'.format(len(full_df), args.output_file))
    full_df.to_csv(args.output_file, index=False)