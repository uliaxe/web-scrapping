import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import json
import re
from datetime import datetime
import time

SITE_URL = "https://books.toscrape.com/"
CATALOGUE_URL = "https://books.toscrape.com/catalogue/"
START_PAGE = "https://books.toscrape.com/index.html"

def get_soup(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'lxml')
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la requête vers {url}: {e}")
        return None

def extract_price_float(price_text):
    return float(price_text.replace('£', ''))

def get_stats_for_category(category_url):
    all_prices = []
    current_page_url = category_url
    
    while current_page_url:
        soup = get_soup(current_page_url)
        if not soup:
            break
            
        price_tags = soup.find_all('p', class_='price_color')
        for tag in price_tags:
            all_prices.append(extract_price_float(tag.text))
            
        next_page_tag = soup.find('li', class_='next')
        if next_page_tag:
            next_page_relative_url = next_page_tag.find('a')['href']
            current_page_url = urljoin(current_page_url, next_page_relative_url)
        else:
            current_page_url = None
            
        time.sleep(0.05)

    count = len(all_prices)
    if count == 0:
        return {
            "total_books": 0,
            "avg_price_gbp": 0,
            "min_price_gbp": 0,
            "max_price_gbp": 0
        }
        
    return {
        "total_books": count,
        "avg_price_gbp": round(sum(all_prices) / count, 2),
        "min_price_gbp": min(all_prices),
        "max_price_gbp": max(all_prices)
    }


def parse_category_node(li_element):
    a_tag = li_element.find('a', recursive=False)
    
    if not a_tag:
        return None

    category_name = a_tag.text.strip()
    category_relative_url = a_tag['href']
    category_url = urljoin(SITE_URL, category_relative_url)
    
    print(f"Traitement de la catégorie : {category_name}")
    stats = get_stats_for_category(category_url)

    subcategories_list = []
    nested_ul = li_element.find('ul')
    
    if nested_ul:
        for sub_li in nested_ul.find_all('li', recursive=False):
            parsed_child = parse_category_node(sub_li)
            if parsed_child:
                subcategories_list.append(parsed_child)

    return {
        "nom_categorie": category_name,
        "url_categorie": category_url,
        "statistiques": stats,
        "sous_categories": subcategories_list
    }

if __name__ == "__main__":
    print("Démarrage du scraping de l'arborescence des catégories...")
    
    start_soup = get_soup(START_PAGE)
    
    if start_soup:
        try:
            category_root_ul = start_soup.find('ul', class_='nav-list').find('li').find('ul')
        except AttributeError:
            print("Erreur: Impossible de trouver la structure de catégorie attendue.")
            exit(1)
            
        top_level_categories_li = category_root_ul.find_all('li', recursive=False)
        
        full_category_tree = []
        for li_element in top_level_categories_li:
            category_data = parse_category_node(li_element)
            if category_data:
                full_category_tree.append(category_data)
        
        print("\nScraping de l'arborescence terminé.")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"category_tree_{timestamp}.json"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(full_category_tree, f, indent=4, ensure_ascii=False)
            print(f"Arborescence sauvegardée avec succès dans : {filename}")
        except IOError as e:
            print(f"Erreur lors de la sauvegarde du JSON : {e}")
    else:
        print("Impossible de scraper la page de démarrage. Arrêt.")