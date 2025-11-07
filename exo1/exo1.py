import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import re
from urllib.parse import urljoin

SITE_URL = "https://books.toscrape.com/"
CATALOGUE_URL = "https://books.toscrape.com/catalogue/"

def convert_rating_to_int(rating_text):
    ratings_map = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
    return ratings_map.get(rating_text, 0)

def extract_stock_count(stock_text):
    match = re.search(r'\((\d+) available\)', stock_text)
    return int(match.group(1)) if match else 0

def extract_price_float(price_text):
    return float(price_text.replace('£', ''))

def get_book_details(book_url):
    try:
        response = requests.get(book_url)
        response.raise_for_status() 
    except requests.exceptions.RequestException as e:
        print(f"Erreur (ex: 404) pour {book_url}: {e}")
        return None

    soup = BeautifulSoup(response.content, 'html.parser')
    main = soup.find('div', class_='product_main')
    
    title = main.find('h1').text
    price = extract_price_float(main.find('p', class_='price_color').text)
    stock_text = main.find('p', class_='instock availability').text.strip()
    stock = extract_stock_count(stock_text)
    rating_class = main.find('p', class_='star-rating')['class'][1]
    rating = convert_rating_to_int(rating_class)
    desc_tag = soup.find('div', id='product_description')
    description = desc_tag.find_next_sibling('p').text if desc_tag else ""
    img_tag = soup.find('div', class_='item active').find('img')
    image_url_hd = urljoin(book_url, img_tag['src'])
    breadcrumbs = soup.find('ul', class_='breadcrumb').find_all('li')
    category_primary = breadcrumbs[2].find('a').text.strip()
    category_secondary = breadcrumbs[1].find('a').text.strip()

    return {
        "titre": title,
        "url_detail": book_url,
        "prix_gbp": price,
        "note_sur_5": rating,
        "categorie_principale": category_primary,
        "categorie_secondaire": category_secondary,
        "description": description,
        "stock_disponible": stock,
        "url_image_hd": image_url_hd
    }

def scrape_all_books():
    all_books_data = []
    current_page_url = "https://books.toscrape.com/catalogue/page-1.html"
    
    while current_page_url:
        print(f"Scraping de la page : {current_page_url}")
        
        try:
            response = requests.get(current_page_url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Erreur sur la page de listing {current_page_url}: {e}")
            break

        soup = BeautifulSoup(response.content, 'html.parser')
        
        books_on_page = soup.find_all('article', class_='product_pod')
        
        for book in books_on_page:
            relative_book_url = book.find('h3').find('a')['href']
            absolute_book_url = urljoin(CATALOGUE_URL, relative_book_url)
            
            book_details = get_book_details(absolute_book_url)
            if book_details:
                all_books_data.append(book_details)
        
        next_page_tag = soup.find('li', class_='next')
        if next_page_tag:
            next_page_relative_url = next_page_tag.find('a')['href']
            current_page_url = urljoin(CATALOGUE_URL, next_page_relative_url)
        else:
            current_page_url = None
            
    return all_books_data

if __name__ == "__main__":
    print("Démarrage du scraping de 'Books to Scrape'...")
    
    books_data = scrape_all_books()
    
    print(f"Scraping terminé. {len(books_data)} livres trouvés.")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"books_scrape_{timestamp}.json"
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(books_data, f, indent=4, ensure_ascii=False)
        print(f"Données sauvegardées avec succès dans : {filename}")
    except IOError as e:
        print(f"Erreur lors de la sauvegarde du fichier JSON : {e}")