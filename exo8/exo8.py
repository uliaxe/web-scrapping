import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import yaml
import json
import time
import logging
import re
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class BaseScraper(ABC):

    def __init__(self, name, config):
        self.name = name
        self.config = config
        self.base_url = config['url']
        self.max_pages = config.get('max_pages', 1)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "MultiSourceScraper-Bot-v1.0"})
        logging.info(f"[{self.name}] Module initialisé.")

    def _get_soup(self, url):
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'lxml')
        except requests.RequestException as e:
            logging.error(f"[{self.name}] Erreur HTTP pour {url}: {e}")
            return None

    @abstractmethod
    def parse_page(self, soup):
        pass

    @abstractmethod
    def get_next_page_url(self, soup, current_url):
        pass

    def scrape(self):
        all_data = []
        current_page_url = self.base_url
        pages_scraped = 0
        
        while current_page_url and pages_scraped < self.max_pages:
            logging.info(f"[{self.name}] Scraping de : {current_page_url}")
            soup = self._get_soup(current_page_url)
            if not soup:
                break
                
            page_data = self.parse_page(soup)
            all_data.extend(page_data)
            logging.info(f"[{self.name}] {len(page_data)} items trouvés sur la page.")
            
            pages_scraped += 1
            current_page_url = self.get_next_page_url(soup, current_page_url)
            time.sleep(0.1)
            
        return all_data

class BooksScraper(BaseScraper):
    
    def parse_page(self, soup):
        unified_data = []
        for article in soup.find_all('article', class_='product_pod'):
            title = article.find('h3').find('a')['title']
            url = urljoin(self.base_url, "catalogue/" + article.find('h3').find('a')['href'].replace('../', ''))
            price = article.find('p', class_='price_color').text.strip()
            
            unified_data.append({
                "source": self.name,
                "title": title,
                "url": url,
                "content": f"Prix: {price}",
                "metadata": {"price_gbp": float(price.replace('£', ''))}
            })
        return unified_data

    def get_next_page_url(self, soup, current_url):
        next_tag = soup.find('li', class_='next')
        if next_tag:
            next_url = next_tag.find('a')['href']
            return urljoin(self.base_url, "catalogue/" + next_url)
        return None

class QuotesScraper(BaseScraper):

    def parse_page(self, soup):
        unified_data = []
        for quote in soup.find_all('div', class_='quote'):
            text = quote.find('span', class_='text').text.strip()
            author = quote.find('small', class_='author').text.strip()
            author_url = urljoin(self.base_url, quote.find('a')['href'])
            
            unified_data.append({
                "source": self.name,
                "title": f"Citation de {author}",
                "url": author_url,
                "content": text,
                "metadata": {"author": author}
            })
        return unified_data

    def get_next_page_url(self, soup, current_url):
        next_tag = soup.find('li', class_='next')
        if next_tag:
            return urljoin(self.base_url, next_tag.find('a')['href'])
        return None

class JobsScraper(BaseScraper):

    def parse_page(self, soup):
        unified_data = []
        keyword = self.config.get('filter_keyword')
        
        for card in soup.find_all('div', class_='card-content'):
            title = card.find('h2', class_='title').text.strip()
            
            if keyword and keyword.lower() not in title.lower():
                continue
                
            company = card.find('h3', class_='company').text.strip()
            location = card.find('p', class_='location').text.strip()
            url = card.find('a', string=re.compile(r'Apply|Learn More'))['href']
            
            unified_data.append({
                "source": self.name,
                "title": title,
                "url": url,
                "content": f"Entreprise: {company} | Lieu: {location}",
                "metadata": {"company": company, "location": location}
            })
        return unified_data

    def get_next_page_url(self, soup, current_url):
        return None

SCRAPER_MAP = {
    'books': BooksScraper,
    'quotes': QuotesScraper,
    'jobs': JobsScraper,
}

def load_config(config_path='config.yaml'):
    logging.info(f"Chargement de la configuration depuis {config_path}")
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logging.error(f"Erreur: {config_path} non trouvé.")
        return None
    except yaml.YAMLError as e:
        logging.error(f"Erreur lors du parsing YAML: {e}")
        return None

def run_orchestrator():
    config = load_config()
    if not config:
        return

    all_scraped_data = []
    performance_report = []
    
    max_workers = config['settings'].get('max_workers', 3)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}

        for key, scraper_config in config['scrapers'].items():
            if scraper_config.get('enabled', False):
                if key in SCRAPER_MAP:
                    ScraperClass = SCRAPER_MAP[key]
                    scraper_instance = ScraperClass(scraper_config['name'], scraper_config)
                    future = executor.submit(scraper_instance.scrape)
                    futures[future] = scraper_config['name']
                else:
                    logging.warning(f"Clé de scraper '{key}' inconnue. Ignoré.")

        for future in as_completed(futures):
            name = futures[future]
            try:
                start_time = time.time()
                result_data = future.result()
                duration = time.time() - start_time
                
                all_scraped_data.extend(result_data)

                performance_report.append({
                    "source": name,
                    "items_trouves": len(result_data),
                    "temps_exec_sec": round(duration, 2)
                })
                logging.info(f"[{name}] Tâche terminée, {len(result_data)} items récupérés.")
                
            except Exception as e:
                logging.error(f"[{name}] Échec de la tâche de scraping : {e}")
                performance_report.append({"source": name, "items_trouves": 0, "error": str(e)})

    output_file = config['settings'].get('output_file', 'aggregated_data.json')
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_scraped_data, f, indent=4, ensure_ascii=False)
        logging.info(f"Agrégation terminée. {len(all_scraped_data)} items sauvegardés dans {output_file}")
    except IOError as e:
        logging.error(f"Impossible de sauvegarder le JSON agrégé : {e}")

    print("\n--- Rapport de Performance ---")
    for report in performance_report:
        print(f"Source: {report['source']}, Items: {report['items_trouves']}, Temps: {report.get('temps_exec_sec', 'N/A')}s")
    print("------------------------------")

if __name__ == "__main__":
    run_orchestrator()