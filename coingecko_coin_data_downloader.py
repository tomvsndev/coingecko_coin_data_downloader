import datetime
import random

import aiofiles
import aiohttp
import asyncio
import os
import json
import time
from aiohttp import ClientSession


class CoinGeckoDownloader:
    def __init__(self, rate_limit=5, max_per_minute=5, storage_dir="coin_data"):
        self.base_url = "https://api.coingecko.com/api/v3/coins/"
        self.rate_limit = rate_limit  # Max API calls per minute
        self.max_per_minute = max_per_minute
        self.storage_dir = storage_dir
        self.semaphore = asyncio.Semaphore(self.rate_limit)

        self.debug = True # if debug will save to json wont load from the api
        self.debug_file_name = 'global_tickers.json'
        self.itter_fetch = 0
        self.itter_load = 0
        self.total = 0
        self.binance_symbols = None
        self.global_coin_dict = None


        # Make sure the storage directory exists
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)

    async def fetch_coin_data(self, coin_id: str, max_retries: int = 5):
        """Fetch single coin data (description, image, etc.) from CoinGecko with retries."""
        url = f"{self.base_url}{coin_id}"

        for attempt in range(1, max_retries + 1):
            try:
                async with aiohttp.ClientSession() as session:
                 async with session.get(url) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status == 429:
                        wait_time = 60 * attempt  # increase wait time with each retry
                        print(f"[429] Rate limited on {coin_id}. Sleeping {wait_time}s...")
                        await asyncio.sleep(wait_time)
                    else:
                        print(f"[{resp.status}] Failed to fetch {coin_id} on attempt {attempt}")
            except Exception as e:
                print(f"[Error] {coin_id} attempt {attempt}: {e}")

            await asyncio.sleep(2 * attempt)  # exponential backoff

        print(f"[Failed] All retries exhausted for {coin_id}")
        return None

    async def download_image(self, image_url: str, image_path: str):
        """Download image and save it to the specified path."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(image_url) as resp:

                    if resp.status == 200:
                        with open(image_path, "wb") as f:
                            f.write(await resp.read())
                            # print(f"Downloaded image to {image_path}")
                    if resp.status == 429:
                        await asyncio.sleep(60)

        except Exception as e:
            print(f"Error downloading image {image_url}: {e}")

    async def download_coin_images(self, coin_data: dict, coin_id: str):
        """Download all images (thumb, small, large) for a given coin."""
        image_urls = coin_data.get("image", {})
        if image_urls:
            image_folder = os.path.join(self.storage_dir, coin_id, "images")
            os.makedirs(image_folder, exist_ok=True)

            for size in ["thumb", "small", "large"]:
                image_url = image_urls.get(size)
                if image_url:
                    image_path = os.path.join(image_folder, f"{coin_id}_{size}.png")
                    await self.download_image(image_url, image_path)

    async def save_coin_data(self, coin_data: dict, coin_id: str):
        """Save coin data (description, symbol, etc.) to a local JSON file."""
        coin_folder = os.path.join(self.storage_dir, coin_id)
        os.makedirs(coin_folder, exist_ok=True)

        coin_file_path = os.path.join(coin_folder, f"{coin_id}.json")
        with open(coin_file_path, "w") as f:
            json.dump(coin_data, f, indent=4)

    async def process_coin(self, coin: str):
        """Process a single coin: fetch data, download images, and save."""

        async with self.semaphore:
            
            coin_id = coin['id']
            coin_file_path = os.path.join(self.storage_dir, coin['id'], f"{coin['id']}.json")
            if os.path.exists(coin_file_path):
                self.itter_load+=1
                #print(f"coin:{coin_id} already exists skipping..")


                del self.global_coin_dict[coin_id]
                return False
            
            self.itter_fetch += 1
            coin_data = await self.fetch_coin_data(coin_id)
            if coin_data:
                await self.save_coin_data(coin_data, coin_id)
                await self.download_coin_images(coin_data, coin_id)
                del self.global_coin_dict[coin_id]
                return True
            else:
                print(f"Skipping {coin_id} due to fetch failure")
                return False

    async def process_coins(self, coin_ids, endpoint='Binance'):
        """Process a list of coins."""

        def batches(iterable, n=1):

            l = len(iterable)
            for ndx in range(0, l, n):
                yield iterable[ndx:min(ndx + n, l)]

        total_coins = len(coin_ids)
        start_time = time.time()

        for batch in batches(coin_ids, self.rate_limit):
            for coin in batch:
                result = await self.process_coin(coin)
                if result:
                    elapsed_time = time.time() - start_time
                    avg_time = elapsed_time / self.itter_fetch if self.itter_fetch else 0
                    remaining = avg_time * (total_coins - self.itter_fetch)

                    eta = str(datetime.timedelta(seconds=int(remaining)))
                    print(
                        f"[{endpoint}] Processed: {self.itter_fetch}/{total_coins - self.itter_load} | Remaining: {total_coins - self.itter_load - self.itter_fetch} | ETA: {eta} "
                    )
                    # Sleep for a random float between 12 and 15 seconds
                    await asyncio.sleep(random.uniform(12, 15))





    async def load_tickers_from_file(self):
        """Load tickers from saved JSON file."""


        try:
            async with aiofiles.open(self.debug_file_name, "r") as f:
                content = await f.read()
                return json.loads(content)
            return False
        except Exception as e:
            print(f"Failed to load tickers from file: {e}")
            return False

    async def get_binance_symbols(self):
        url = "https://api.binance.com/api/v3/exchangeInfo"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    base_symbols = {item['baseAsset'].lower() for item in data['symbols']}

                    return base_symbols
                else:
                    print(f"Failed to fetch Binance exchange info: HTTP {resp.status}")
                    return set()

    async def start(self):
        """Start the downloading process."""
        self.global_coin_dict = {
            coin["id"]: coin for coin in await self.load_tickers_from_file()
        }

        binance_symbols = await self.get_binance_symbols()

        self.total = len(binance_symbols)
        self.binance_symbols = binance_symbols
        symbols_to_fetch = []

        #download what matches binance
        for id in self.global_coin_dict:
            id_data = self.global_coin_dict[id]
            symbol = id_data['id']
            if symbol in self.binance_symbols:
                symbols_to_fetch.append(id_data)


        await self.process_coins(symbols_to_fetch,endpoint='binance')

        #download all the rest what doesnt match binance



        # Filter coins to fetch from global_coin_dict based on the 'symbol' matching
        filtered_global_coin_dict = []

        for key, value in self.global_coin_dict.items():
                    filtered_global_coin_dict.append(self.global_coin_dict[key])

        await self.process_coins(filtered_global_coin_dict,endpoint='all the rest')







# Example usage:
if __name__ == "__main__":

    downloader = CoinGeckoDownloader(rate_limit=5)  # 5 calls per minute (adjust as needed)
    # # Run the download process
    asyncio.run(downloader.start())