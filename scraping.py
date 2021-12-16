from datetime import datetime
import os
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.remote.command import Command
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import SessionNotCreatedException
from selenium.common.exceptions import TimeoutException
import numpy as np
import time
import string
import threading
from threading import Lock
import pandas as pd
import warnings
from webdriver_manager.chrome import ChromeDriverManager


YT_SEARCH_URL_TEMPLATE = 'https://www.youtube.com/results?search_query={}'
ACTION_DELAY_SECONDS = 0.5
RETRY_DELAY_SECONDS = 3.0
LOAD_TIMEOUT_SECONDS = 15.0
SELENIUM_WAIT_EXCEPTIONS = (NoSuchElementException, StaleElementReferenceException)

XPATH_PATTERNS = {
  'search_thumbnail': '//a[@id="thumbnail"]',
  'suggested_thumbnail': '//div[@id="related"][contains(@class, "ytd-watch-flexy")]/*/*/*/*/*/a[@id="thumbnail"]',
  'view_count': '//span[contains(@class, "view-count")]', #'//div[@id="count"]/ytd-video-view-count-renderer/span[1]',
  'date': '(//div[@id="date"]|//div[@id="info-strings"])/yt-formatted-string',
  'video_title': '//h1[contains(@class, "title")]/yt-formatted-string',
  'video_description': '//div[@id="description"]/yt-formatted-string',
  'channel_name_link': '//ytd-channel-name[@id="channel-name"]/div/div/yt-formatted-string/a',
  'subscriber_count': '//yt-formatted-string[@id="owner-sub-count"]',
  'likes': '//yt-formatted-string[@id="text"][contains(@aria-label, " likes")][1]',
  # 'dislikes': '//yt-formatted-string[@id="text"][contains(@aria-label, " dislikes")][1]',
  'video_page_views': '//*[@id="metadata-line"]/span[1]',
  'video_page_upload_dates': '//*[@id="metadata-line"]/span[2]',
  'video_page_titles': '//*[@id="video-title"]'
}

os.environ['Path'] = os.environ['Path'] + ';.\\chromedriver'


def action_wait():
  time.sleep(ACTION_DELAY_SECONDS)

def yt_time_ago_to_datetime(time_ago):
  # Source: https://stackoverflow.com/questions/12566152/python-x-days-ago-to-datetime
  parsed_str = [time_ago.split()[:2]]
  time_dict = dict((fmt, float(amount)) for amount, fmt in parsed_str)
  dt = datetime.timedelta(**time_dict)
  past_time = datetime.datetime.now() - dt
  return past_time

def yt_label_to_num(label):
  """Converts YT formatted numbers with added text into integers."""
  if label is None:
      return None
  num_str = '0'
  multiplier = 1
  for c in label.lower():
    if c in (string.digits + '.'):
      num_str += c
    elif c in 'kmb':
      if c == 'k':
        multiplier = 1e3
      elif c == 'm':
        multiplier = 1e6
      if c == 'b':
        multiplier = 1e9
      break
    elif c == ',':
      continue
    else:
      break
  
  return int(float(num_str) * multiplier)

def yt_label_to_datetime(label):
  """Converts YT formatted date strings into datetime objects."""
  if label is None:
      return None
  return label # TODO: convert to datetime

def run_with_retry(func, times=3, refresh_driver=None):
  for i in range(times-1):
    try:
      return func()
    except:
      print('Function call {} failed, retrying...'.format(i + 1))
      if refresh_driver:
        refresh_driver.navigate().refresh()
      time.sleep(RETRY_DELAY_SECONDS)
      
    return func()

class YouTubeScraper():
  def __init__(self, headless=True):
    chrome_options = Options()
    if headless:
      chrome_options.add_argument('--headless')
      chrome_options.add_argument('--mute-audio')
    
    try:
      self.driver = webdriver.Chrome(options=chrome_options)
    except SessionNotCreatedException:
      warnings.warn('Error due to likely incorrect version of ChromeDriver. Please update to latest version.')
      self.driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

    self.scraped_vid_urls = set([])
    self.scraped_channel_urls = set([])

    self._video_data_buffer = []
    self._vdb_lock = Lock()

    self._channel_data_buffer = []
    self._cdb_lock = Lock()
    
  def terminate(self):
    try:
      self.driver.quit()
    except Exception as e:
      print(f'Tried to terminate YouTubeScraper, but failed with exception: {e}')

  def perform_yt_search(self, search_term):
    """Opens up YouTube and performs a search for the specified term."""
    self.driver.get(YT_SEARCH_URL_TEMPLATE.format(search_term))
    action_wait()
    if 'youtube' not in self.driver.title.lower():
      return False
    return True
  
  def _retrieve_search_videos(self):
    """Returns all video link elements from a YouTube page."""
    videos = WebDriverWait(
      self.driver,
      LOAD_TIMEOUT_SECONDS,
      ignored_exceptions=SELENIUM_WAIT_EXCEPTIONS
      ).until(
        EC.presence_of_all_elements_located((
          By.XPATH,
          XPATH_PATTERNS['search_thumbnail'])))
    
    valid_videos = []
    for video in videos:
      link = video.get_attribute('href')
      if link is not None and 'youtube.com' in link.lower():
        valid_videos.append(video)
    return valid_videos
  
  def _retrieve_suggested_videos(self):
    """Returns all video link elements from a YouTube suggested bar."""
    videos = WebDriverWait(
      self.driver,
      LOAD_TIMEOUT_SECONDS,
      ignored_exceptions=SELENIUM_WAIT_EXCEPTIONS
      ).until(
        EC.presence_of_all_elements_located((
          By.XPATH,
          XPATH_PATTERNS['suggested_thumbnail'])))
    
    valid_videos = []
    for video in videos:
      try:
        link = video.get_attribute('href')
        if link is not None and 'youtube.com' in link.lower():
          valid_videos.append(video)
      except StaleElementReferenceException as e:
        continue
#     print(len(valid_videos) / len(videos))
    return valid_videos

  def choose_vid_from_search(self, scroll_chance=0.5, max_scrolls=15):
    """Selects a random YouTube video and clicks on the link. Should only be used on the search page."""
    for n_scrolls in range(max_scrolls):
      if np.random.rand() < scroll_chance:
        self.driver.execute_script('window.scrollTo(0, document.getElementById("content").scrollHeight)')
        action_wait()
      else:
        action_wait()
        break
        
    all_vids = self._retrieve_search_videos()
    if not all_vids:
      return None
    
    bottom_vids = all_vids[int(np.ceil(-len(all_vids) / (n_scrolls + 1))):]
    selected_vid = np.random.choice(bottom_vids)
    thumbnail_element = selected_vid.find_element(By.XPATH, './/img')
    thumbnail_link = thumbnail_element.get_property('src')
    if not thumbnail_element:
      return None
    
    selected_vid.click()
    
    # Return a link to the thumbnail
    return {'thumbnail_link': thumbnail_link}
  
  def choose_vid_from_suggested(self, scroll_chance=0.5, max_scrolls=5):
    """Selects a random YouTube video and clicks on the link. Should only be used on the suggested bar page."""
    for n_scrolls in range(max_scrolls):
      if np.random.rand() < scroll_chance:
        self.driver.execute_script('window.scrollTo(0, document.getElementById("content").scrollHeight)')
        action_wait()
      else:
        action_wait()
        break
        
    all_vids = self._retrieve_suggested_videos()
#     all_vids = run_with_retry(self._retrieve_suggested_videos())
    if not all_vids:
      return None
    
    bottom_vids = all_vids[int(np.ceil(-len(all_vids) / (n_scrolls + 1))):]
    selected_vid = np.random.choice(bottom_vids)
    
    self.driver.execute_script('arguments[0].scrollIntoView(true)', selected_vid);
    self.driver.execute_script('window.scrollBy(0, -50)')
    action_wait()  
    
    thumbnail_element = selected_vid.find_element(By.XPATH, './/img')
    if not thumbnail_element:
      return None
    thumbnail_link = thumbnail_element.get_property('src')
    
    run_with_retry(selected_vid.click)
    
    # Return a link to the thumbnail
    return {'thumbnail_link': thumbnail_link}
  
  def scrape_vid_data(self):
    """Scrapes video data from a YT video page."""
    data = {}
    
    target_items = ('view_count', 'date', 'video_title', 'video_description',
                    'channel_name_link', 'subscriber_count', 'likes')
    for item in target_items:
      pattern = XPATH_PATTERNS[item]
      try:
        element = WebDriverWait(self.driver, LOAD_TIMEOUT_SECONDS,
          ignored_exceptions=SELENIUM_WAIT_EXCEPTIONS).until(
          EC.presence_of_all_elements_located((By.XPATH, pattern)))
      except TimeoutException:
        warnings.warn(f'Timeout while waiting for element "{item}" to load.')
        return None
      data[item] = element
    
    current_date = datetime.now().strftime("%b %d, %Y")

    data['view_count'] = yt_label_to_num(data['view_count'][0].text)
    data['scrape_date'] = current_date
    data['date'] = data['date'][0].text
    data['video_title'] = data['video_title'][0].text
    data['video_description'] = data['video_description'][0].text
    data['channel_name'] = data['channel_name_link'][-1].text
    data['channel_link'] = data['channel_name_link'][-1].get_property('href')
    data['subscriber_count'] = yt_label_to_num(data['subscriber_count'][-1].text)
    data['likes'] = yt_label_to_num(data['likes'][0].get_attribute('aria-label'))
    # data['dislikes'] = yt_label_to_num(data['dislikes'][0].get_attribute('aria-label'))
    data['video_url'] = self.driver.current_url

    del data['channel_name_link']
    
    return data

  # Channel based scraping
  def _scrape_channel_page(self, channel_name, channel_url):
    if channel_url in self.scraped_channel_urls:
      return
    self.scraped_channel_urls.add(channel_url)

    # Naviate to the videos page
    video_page_url = channel_url + '/videos'
    self.driver.get(video_page_url)
    action_wait()

    view_counts = WebDriverWait(
      self.driver,
      LOAD_TIMEOUT_SECONDS,
      ignored_exceptions=SELENIUM_WAIT_EXCEPTIONS
      ).until(
        EC.presence_of_all_elements_located((
          By.XPATH,
          XPATH_PATTERNS['video_page_views'])))

    upload_dates = WebDriverWait(
      self.driver,
      LOAD_TIMEOUT_SECONDS,
      ignored_exceptions=SELENIUM_WAIT_EXCEPTIONS
      ).until(
        EC.presence_of_all_elements_located((
          By.XPATH,
          XPATH_PATTERNS['video_page_upload_dates'])))

    titles = WebDriverWait(
      self.driver,
      LOAD_TIMEOUT_SECONDS,
      ignored_exceptions=SELENIUM_WAIT_EXCEPTIONS
      ).until(
        EC.presence_of_all_elements_located((
          By.XPATH,
          XPATH_PATTERNS['video_page_titles'])))

    if view_counts is None or upload_dates is None or titles is None:
      warnings.warn('Some of the data loaded on the channel videos page was null, skipping.')
      return
    elif not (len(view_counts) == len(upload_dates) == len(titles)):
      warnings.warn('Number of view counts, upload dates, and titles do not match, skipping.')
      return

    # Convert elements to target format (int, datetime, str)
    view_counts = [yt_label_to_num(view_count.text) for view_count in view_counts]
    upload_dates = [yt_label_to_datetime(upload_date.text) for upload_date in upload_dates]
    titles = [title.text for title in titles]
    
    current_date = datetime.now().strftime('%b %d, %Y')

    channel_data = {
      'channel_name': channel_name,
      'channel_link': channel_url,
      'title': tuple(titles),
      'upload_date': tuple(upload_dates),
      'view_count': tuple(view_counts),
      'scrape_date': current_date
    }

    self._add_to_channel_data_buffer(channel_data)
  
  def _add_to_video_data_buffer(self, data):
    with self._vdb_lock:
      self._video_data_buffer.append(data)

  def _add_to_channel_data_buffer(self, data):
    with self._cdb_lock:
      self._channel_data_buffer.append(data)
      
  def flush_video_data(self):
    with self._vdb_lock:
      video_data = self._video_data_buffer
      self._video_data_buffer = []
    return video_data

  def flush_channel_data(self):
    with self._cdb_lock:
      channel_data = self._channel_data_buffer
      self._channel_data_buffer = []
    return channel_data
  
  def _scrape_loop(self, start_term, stop_check):
    # Start initial scrape
    self.perform_yt_search(start_term)

    # Scrape first video
    video_data = self.choose_vid_from_search()
    video_url = self.driver.current_url
    if video_url not in self.scraped_vid_urls:
      new_video_data = self.scrape_vid_data()
      if new_video_data is not None:
        video_data.update(new_video_data)
        self._add_to_video_data_buffer(video_data)
        self.scraped_vid_urls.add(video_url)
    else:
      action_wait()

    # Start scraping loop
    while True:
      video_data = run_with_retry(self.choose_vid_from_suggested)
      video_url = self.driver.current_url
      if video_url not in self.scraped_vid_urls:
        new_video_data = self.scrape_vid_data()
        if new_video_data is not None:
          video_data.update(new_video_data)
          self._add_to_video_data_buffer(video_data)
          self.scraped_vid_urls.add(video_url)
      else:
        action_wait()

      # Stop thread when variable set to true
      if stop_check():
        break


class YTSManager():
  def __init__(self):
    self.video_data = []
    self.channel_data = []
    self._threads = {}
    self._stop_scrape_thread = False
    self._thread_lock = Lock()
    self._video_flush_interval = 2 # Flush video data every x seconds
    self._channel_flush_interval = 2 # Flush channel data every x seconds
    self._channel_scrape_interval = 0.2 # Create new scraping threads every x seconds
    self.checking_thread = None
    self.channel_checking_thread = None
  
  def start_scrape_loops(self, start_terms):
    if hasattr(start_terms, '__len__') and len(start_terms) == 0:
      return
    
    if isinstance(start_terms, str):
      start_terms = (start_terms,)
    
    for start_term in start_terms:
      yts = YouTubeScraper()
      thread = threading.Thread(target=yts._scrape_loop, args=(start_term, self._stop_check))
      self._threads[thread] = (start_term, yts)
      thread.start()
      
    if not self.is_thread_checking_active():
      self._start_check_video_thread()
      
  def _check_video_threads(self):
    """Check to renew dead threads and flush video data buffers on a regular interval."""
    while len(self._threads) > 0:
      time.sleep(self._video_flush_interval)
      
      with self._thread_lock:
        # Flush video data on all threads
        for thread, (start_term, yts) in self._threads.items():
          self.video_data.extend(yts.flush_video_data())
            
        # Remove deleted threads, but keep the start words
        start_words_refresh_list = []
        updated_threads = {}
        for thread, (sw, yts) in self._threads.items():
          if thread.is_alive():
            updated_threads[thread] = (sw, yts)
          else:
            yts.terminate()
            start_words_refresh_list.append(sw)
        self._threads = updated_threads
        
        # Refresh any removed threads
        self.start_scrape_loops(start_words_refresh_list)
        
  def is_thread_checking_active(self):
    return self.checking_thread and self.checking_thread.is_alive()

  def is_channel_thread_checking_active(self):
    return self.channel_checking_thread and self.channel_checking_thread.is_alive()
        
  def _start_check_video_thread(self):
    self.checking_thread = threading.Thread(target=self._check_video_threads)
    self.checking_thread.start()

  def _start_check_channel_thread(self):
    self.channel_checking_thread = threading.Thread(target=self._check_channel_threads)
    self.channel_checking_thread.start()
                   
  def _stop_check(self):
    return self._stop_scrape_thread
      
  def stop_scraping(self):
    with self._thread_lock:
      self._stop_scrape_thread = True
      for thread, (_, yts) in self._threads.items():
        thread.join()
        yts.terminate()
      self._stop_scrape_thread = False
      self._threads = {}

  def stop_channel_scraping(self):
    print('Stopping channel scraping')
    with self._thread_lock:
      self._stop_scrape_thread = True
      print('Scrapers:', self.scrapers, 'Threads:', self._threads)
      for thread, yts in self._threads.items():
        print('Found thread:', thread)
        if thread.is_alive():
          print('Thread is alive')
          thread.join()
          print('Thread joined')
        yts.terminate()
        print('Thread scraper terminated')
      for scraper in self.scrapers:
        scraper.terminate()
        print('Scraper terminated')
      self._threads = {}
      self.scrapers = []
      self._stop_scrape_thread = False
      
  def print_status(self):
    print('# Videos Scraped: {}'.format(len(self.video_data)))
    print('# Threads Running: {}'.format(len(self._threads)))

  def print_channel_status(self):
    print('# Channels Scraped: {}'.format(len(self.channel_data)))
    print('# Threads Running: {}'.format(len(self._threads)))

  def get_dataframe(self):
    return pd.DataFrame(self.video_data)

  def get_channel_dataframe(self):
    return pd.DataFrame(self.channel_data)

  def start_channel_scrape_loops(self, channel_names, channel_urls, n_workers=8):
    self.channel_scrape_thread = threading.Thread(
      target=self._run_channel_scrape_loops,
      args=(channel_names, channel_urls, n_workers))
    self.channel_scrape_thread.start()

  def _run_channel_scrape_loops(self, channel_names, channel_urls, n_workers=8):
    if hasattr(channel_urls, '__len__') and len(channel_urls) == 0:
      return
    
    # Scraping agents are reused by the threads created below
    self.scrapers = [YouTubeScraper() for _ in range(n_workers)]

    early_stop = False
    i = 0
    while i < len(channel_names):
      if len(self._threads) < n_workers:
        # Create a new thread for a new channel
        channel_name = channel_names[i]
        channel_url = channel_urls[i]
        with self._thread_lock:
          if len(self.scrapers) == 0:
              print('No scrapers available, stopping scraping')
              early_stop = True
              break
          yts = self.scrapers.pop(0)
          thread = threading.Thread(target=yts._scrape_channel_page,
            args=(channel_name, channel_url))
          self._threads[thread] = yts
          thread.start()

        if early_stop:
          break

        i += 1

      # Wait for the channel sleep time
      time.sleep(self._channel_scrape_interval)

      # Get rid of dead threads
      with self._thread_lock:
        updated_threads = {}
        for thread, yts in self._threads.items():
          if thread.is_alive():
            updated_threads[thread] = yts
          else:
            self.scrapers.append(yts)
        self._threads = updated_threads

      if not self.is_channel_thread_checking_active():
        # Starts a thread to periodically flush data
        self._start_check_channel_thread()

      if self._stop_check():
        early_stop = True
        break

    # Wait for all threads to finish
    if not early_stop:
      with self._thread_lock:
        updated_threads = {}
        for thread, yts in self._threads.items():
          if thread.is_alive():
            thread.join()
          yts.terminate()
        for scraper in self.scrapers:
          scraper.terminate()
        self.scrapers = []
        self._threads = {}

  def _check_channel_threads(self):
    """Check to renew dead threads and flush video data buffers on a regular interval."""
    while len(self._threads) > 0:
      time.sleep(self._channel_flush_interval)
      
      with self._thread_lock:
        # Flush video data on all threads
        for _, yts in self._threads.items():
          self.channel_data.extend(yts.flush_channel_data())

if __name__ == '__main__':
  try:
    manager = YTSManager()
    manager.start_scrape_loops('testing')
    for _ in range(12):
        manager.print_status()
        time.sleep(5)
    manager.stop_scraping()
  except KeyboardInterrupt:
    manager.stop_scraping()
    print('\n\nStopped scraping')
  finally:
    print('Saving data')
    df = manager.get_dataframe()
    df.to_csv('test.csv')