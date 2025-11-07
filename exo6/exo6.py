import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import logging
import time
import json
import os
import re
from urllib.parse import urljoin

SITE_URL = "https://books.toscrape.com/"
CATALOGUE_URL = "https://books.toscrape.com/catalogue/"
START_PAGE = "https://books.toscrape.com/catalogue/page-1.html"

LOG_FILE = 'scraper.log'
PROGRESS_FILE = 'scraper_progress.log'
OUTPUT_FILE = 'books_data_resilient.jsonl'

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE, mode='a'),
            logging.StreamHandler()
        ]
    )

def create_resilient_session():
    session = requests.Session()
    
    retry_strategy = Retry(
        total=5,
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor=1,
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    session.timeout = (5, 30)
    
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    })
    
    return session

def load_progress():
    if not os.path.exists(PROGRESS_FILE):
        logging.info(f"Aucun fichier de progression ({PROGRESS_FILE}) trouvé. Démarrage depuis le début.")
        return START_PAGE
        
    try:
        with open(PROGRESS_FILE, 'r') as f:
            last_page = f.read().strip()
            if last_page:
                logging.info(f"Reprise du scraping à partir de : {last_page}")
                return last_page
            else:
                logging.warning("Fichier de progression vide. Démarrage depuis le début.")
                return START_PAGE
    except Exception as e:
        logging.error(f"Erreur lors de la lecture du fichier de progression : {e}. Redémarrage.")
        return START_PAGE

def save_progress(next_page_url):
    try:
        with open(PROGRESS_FILE, 'w') as f:
            f.write(next_page_url)
    except IOError as e:
        logging.error(f"Impossible de sauvegarder la progression sur {next_page_url}: {e}")

def save_data(book_data):
    try:
        with open(OUTPUT_FILE, 'a', encoding='utf-8') as f:
            json.dump(book_data, f, ensure_ascii=False)
            f.write('\n')
    except IOError as e:
        logging.error(f"Erreur lors de la sauvegarde du livre {book_data.get('titre')}: {e}")

def convert_rating_to_int(rating_text):
    ratings_map = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
    return ratings_map.get(rating_text, 0)

def extract_stock_count(stock_text):
    match = re.search(r'\((\d+) available\)', stock_text)
    return int(match.group(1)) if match else 0

def get_book_details(session, book_url):
    try:
        response = session.get(book_url)
        response.raise_for_status() 
    except requests.exceptions.RequestException as e:
        if hasattr(e, 'response') and e.response.status_code == 403:
             logging.critical(f"Accès Refusé (403) pour {book_url}. Blocage IP possible ! Arrêt.")
             raise SystemExit("Blocage IP détecté (403).")
        logging.error(f"Échec final de la requête pour {book_url}: {e}")
        return None

    try:
        soup = BeautifulSoup(response.content, 'lxml')
        main = soup.find('div', class_='product_main')
        title = main.find('h1').text
        price = float(main.find('p', class_='price_color').text.replace('£', ''))
        stock = extract_stock_count(main.find('p', class_='instock availability').text.strip())
        rating = convert_rating_to_int(main.find('p', class_='star-rating')['class'][1])
        
        desc_tag = soup.find('div', id='product_description')
        description = desc_tag.find_next_sibling('p').text if desc_tag else ""
        
        img_tag = soup.find('div', class_='item active').find('img')
        image_url_hd = urljoin(book_url, img_tag['src'])
        
        return {
            "titre": title,
            "url_detail": book_url,
            "prix_gbp": price,
            "note_sur_5": rating,
            "description": description,
            "stock_disponible": stock,
            "url_image_hd": image_url_hd
        }
    except Exception as e:
        logging.error(f"Erreur de parsing sur {book_url}: {e}")
        return None

if __name__ == "__main__":
    setup_logging()
    logging.info("--- Démarrage du Scraper Résilient ---")
    
    session = create_resilient_session()
    current_page_url = load_progress()
    
    books_scraped_session = 0
    start_time = time.time()

    while current_page_url:
        logging.info(f"Scraping de la page : {current_page_url}")
        
        try:
            response = session.get(current_page_url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error(f"Échec critique sur la page de listing {current_page_url}: {e}")
            break 
        soup = BeautifulSoup(response.content, 'lxml')
        
        books_on_page = soup.find_all('article', class_='product_pod')
        
        for book in books_on_page:
            relative_book_url = book.find('h3').find('a')['href']
            absolute_book_url = urljoin(CATALOGUE_URL, relative_book_url)
            
            book_details = get_book_details(session, absolute_book_url)
            
            if book_details:
                save_data(book_details)
                logging.debug(f"SUCCÈS : {book_details['titre']}")
                books_scraped_session += 1
            else:
                logging.warning(f"ÉCHEC : Impossible de scraper {absolute_book_url}")

            time.sleep(0.1) 
        
       
        next_page_tag = soup.find('li', class_='next')
        if next_page_tag:
            next_page_relative_url = next_page_tag.find('a')['href']
            current_page_url = urljoin(CATALOGUE_URL, next_page_relative_url)
  
            save_progress(current_page_url)
        else:
            current_page_url = None
            logging.info("Fin de la pagination atteinte.")
            if os.path.exists(PROGRESS_FILE):
                os.remove(PROGRESS_FILE)
                
        time.sleep(0.5) 
            
    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"--- Session de Scraping Terminée ---")
    logging.info(f"Temps total : {duration:.2f} secondes")
    logging.info(f"Livres scrapés cette session : {books_scraped_session}")
    if books_scraped_session > 0 and duration > 0:
        logging.info(f"Performance : {books_scraped_session / duration:.2f} livres/seconde")
    logging.info(f"Données sauvegardées dans : {OUTPUT_FILE}")
    logging.info(f"Logs complets dans : {LOG_FILE}")