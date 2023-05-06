import os
import sqlite3
import pandas as pd
import pathlib
import re
import numpy as np
import aiohttp
from asgiref import sync
import asyncio
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
import requests
from bs4 import BeautifulSoup

def get_extensions(filenames):
    extensions = set()
    for filename in filenames:
        regex_result = re.search("(.*)\.(.*)", filename)
        if regex_result is not None:
            extensions.add(regex_result.groups()[-1])
    return extensions


def get_files_with_extensions(filenames, extensions):
    selected = []
    for filename in filenames:
        regex_result = re.search("(.*)\.(.*)", filename)
        if regex_result is not None:
            extension = regex_result.groups()[-1]
            if extension in extensions:
                selected.append(regex_result.group())
    return selected


def split_size(sizes):
    splits = []
    filters = {pd.NA, np.nan}
    for x in sizes:
        if x in filters:
            continue
        match = re.search("(\d*\.?\d*)(.*)", x)
        if match:
            num = match.group(1)
            typ = match.group(2)
            try:
                num = float(num)
            except:
                print(type(num))
                num = 0
            splits.append((num, typ))
            
    return splits

def async_aiohttp_get_all(urls):
    """
    performs asynchronous get requests
    """
    async def get_all(urls):
        connector = aiohttp.TCPConnector(force_close=True)
        async with aiohttp.ClientSession(connector=connector) as session:
            async def fetch(url):
                async with session.get(url) as response:
                    return await response.read()
            return await tqdm_asyncio.gather(*[
                fetch(url) for url in urls
            ])
        
    # call get_all as a sync function to be used in a sync context
    #return get_all(urls)
    return sync.async_to_sync(get_all)(urls)
    

def write_file(data, filename, directory):
    _filename = directory + "/" + filename
    os.makedirs(os.path.dirname(_filename), exist_ok=True)
    with open(_filename, "wb") as f:
        f.write(response)

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

url = r"http://people.brunel.ac.uk/~mastjjb/jeb/orlib/files/"


r  = requests.get(url)
data = r.text
soup = BeautifulSoup(data, features="lxml")

links = []
for link in soup.find_all('a'):
    links.append(link.get('href'))

valid_files = get_files_with_extensions(links, get_extensions(links))
urls = [url + f for f in valid_files]

responses = []
for chunk in chunks(urls, len(urls)):
    responses += async_aiohttp_get_all(chunk)

i = -1
for response in responses:
    i += 1
    write_file(response, valid_files[i], "ORLib_files")