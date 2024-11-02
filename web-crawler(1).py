import aiohttp
import asyncio
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from datetime import datetime
import os
import re
from collections import defaultdict
import logging
from aiohttp import ClientTimeout
import queue
import threading
import time

class FastURLPatternCrawler:
    def __init__(self, start_url, output_dir="crawled_urls", max_workers=10, max_connections=100):
        self.start_url = start_url
        self.domain = urlparse(start_url).netloc
        self.crawled_urls = set()
        self.urls_to_crawl = asyncio.Queue()
        self.output_dir = output_dir
        self.url_patterns = defaultdict(set)
        self.max_workers = max_workers
        self.max_connections = max_connections
        self.write_queue = queue.Queue()
        self.timeout = ClientTimeout(total=10)
        
        # Statistics tracking
        self.start_time = time.time()
        self.pattern_stats = defaultdict(int)
        self.total_requests = 0
        self.failed_requests = 0
        self.processed_urls = 0
        self.bytes_downloaded = 0
        
        # Initialize logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)
        
        # Create output directory
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # Create initial log file
        with open(os.path.join(output_dir, 'crawler_log.txt'), 'w') as f:
            f.write(f"Crawling started at: {datetime.now()}\n")
            f.write(f"Starting URL: {start_url}\n\n")
        
        # Initialize file writer thread
        self.writer_thread = threading.Thread(target=self._file_writer)
        self.writer_thread.daemon = True
        self.writer_thread.start()
        
        print(f"\n{'='*80}")
        print(f"Crawler Initialization - {datetime.now()}")
        print(f"Target Domain: {self.domain}")
        print(f"Max Workers: {max_workers}")
        print(f"Max Connections: {max_connections}")
        print(f"Output Directory: {output_dir}")
        print(f"{'='*80}\n")

    def _file_writer(self):
        """Background thread for handling file writes"""
        while True:
            try:
                pattern, url = self.write_queue.get()
                if pattern == "STOP":
                    print("\nFile writer shutting down...")
                    break
                
                # Create nested directory structure
                pattern_parts = pattern.split('/')
                current_dir = self.output_dir
                for part in pattern_parts:
                    current_dir = os.path.join(current_dir, part)
                    if not os.path.exists(current_dir):
                        os.makedirs(current_dir)
                
                # Write URL to file
                pattern_file = os.path.join(current_dir, "urls.txt")
                with open(pattern_file, 'a') as f:
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    f.write(f"[{timestamp}] {url}\n")
                
                self.pattern_stats[pattern] += 1
                self.write_queue.task_done()
                
            except Exception as e:
                self.logger.error(f"Error in file writer: {str(e)}")

    def is_valid_url(self, url):
        """Check if URL belongs to the same domain and hasn't been crawled yet"""
        try:
            parsed = urlparse(url)
            excluded_extensions = ('.pdf', '.jpg', '.jpeg', '.png', '.gif', '.css', '.js', 
                                 '.ico', '.xml', '.json', '.txt', '.doc', '.docx')
            
            return (parsed.netloc == self.domain and
                    url not in self.crawled_urls and
                    not url.endswith(excluded_extensions) and
                    '#' not in url)
        except:
            return False

    def get_url_pattern(self, url):
        """Extract pattern from URL"""
        try:
            path = urlparse(url).path.strip('/')
            parts = path.split('/')
            
            if not path:
                return "root"
            
            main_part = parts[0].lower()
            
            # Content patterns
            if main_part in ['blog', 'news', 'article', 'posts']:
                return f"content/{main_part}"
            
            # Product patterns
            if main_part in ['products', 'categories', 'shop']:
                return f"products/{parts[1] if len(parts) > 1 else 'general'}"
            
            # Date patterns
            if re.match(r'\d{4}', main_part):
                return f"dated/{main_part}"
            
            # Common sections
            if main_part in ['about', 'contact', 'faq', 'help', 'support']:
                return f"info/{main_part}"
            
            return f"misc/{main_part}"
            
        except Exception as e:
            self.logger.error(f"Error determining pattern for {url}: {str(e)}")
            return "misc/error"

    async def process_page(self, url, html):
        """Process HTML content and extract URLs"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            new_urls = set()
            
            for link in soup.find_all('a'):
                href = link.get('href')
                if href:
                    absolute_url = urljoin(url, href)
                    if self.is_valid_url(absolute_url):
                        new_urls.add(absolute_url)
            
            return new_urls
            
        except Exception as e:
            self.logger.error(f"Error processing page {url}: {str(e)}")
            return set()

    async def crawl_page(self, session, url):
        """Crawl a single page asynchronously"""
        self.total_requests += 1
        try:
            async with session.get(url, timeout=self.timeout) as response:
                if response.status == 200:
                    html = await response.text()
                    self.bytes_downloaded += len(html)
                    
                    new_urls = await self.process_page(url, html)
                    pattern = self.get_url_pattern(url)
                    
                    print(f"\nProcessed: {url}")
                    print(f"Pattern: {pattern}")
                    print(f"Found: {len(new_urls)} new URLs")
                    
                    self.write_queue.put((pattern, url))
                    self.crawled_urls.add(url)
                    self.processed_urls += 1
                    
                    for new_url in new_urls:
                        if new_url not in self.crawled_urls:
                            await self.urls_to_crawl.put(new_url)
                else:
                    self.failed_requests += 1
                    print(f"\nFailed ({response.status}): {url}")
                    
        except Exception as e:
            self.failed_requests += 1
            self.logger.error(f"Error crawling {url}: {str(e)}")

    async def crawler_worker(self):
        """Worker for processing URLs from the queue"""
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=self.max_connections)
        ) as session:
            while True:
                try:
                    url = await self.urls_to_crawl.get()
                    if url is None:
                        break
                    
                    if url not in self.crawled_urls:
                        await self.crawl_page(session, url)
                    
                    self.urls_to_crawl.task_done()
                except Exception as e:
                    self.logger.error(f"Worker error: {str(e)}")

    def print_stats(self):
        """Print current crawling statistics"""
        elapsed_time = time.time() - self.start_time
        urls_per_second = self.processed_urls / elapsed_time if elapsed_time > 0 else 0
        
        print(f"\n{'='*80}")
        print(f"Crawling Progress - {datetime.now()}")
        print(f"Processed URLs: {self.processed_urls}")
        print(f"Failed Requests: {self.failed_requests}")
        print(f"Processing Speed: {urls_per_second:.2f} URLs/second")
        print(f"Data Downloaded: {self.bytes_downloaded/1024/1024:.2f} MB")
        print(f"{'='*80}\n")

    async def monitor_progress(self):
        """Monitor crawling progress"""
        while True:
            self.print_stats()
            await asyncio.sleep(5)

    async def run(self):
        """Main crawler execution"""
        print("\nStarting crawler...")
        await self.urls_to_crawl.put(self.start_url)
        
        # Start progress monitor
        monitor = asyncio.create_task(self.monitor_progress())
        
        # Start workers
        workers = [asyncio.create_task(self.crawler_worker()) 
                  for _ in range(self.max_workers)]
        
        try:
            # Wait for initial URL to be processed
            await self.urls_to_crawl.join()
        finally:
            # Cancel monitor
            monitor.cancel()
            
            # Stop workers
            for _ in range(self.max_workers):
                await self.urls_to_crawl.put(None)
            
            await asyncio.gather(*workers, return_exceptions=True)
            
            # Stop file writer
            self.write_queue.put(("STOP", None))
            self.writer_thread.join()
            
            # Print final statistics
            self.print_stats()

def main():
    start_url = ""  # Replace with your target website
    crawler = FastURLPatternCrawler(start_url)
    
    try:
        asyncio.run(crawler.run())
    except KeyboardInterrupt:
        print("\nCrawling interrupted by user...")
    except Exception as e:
        print(f"\nCrawling error: {str(e)}")

if __name__ == "__main__":
    main()