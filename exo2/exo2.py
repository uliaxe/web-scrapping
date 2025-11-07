import requests
import requests_cache
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import networkx as nx
from collections import Counter
import re
import time

SITE_URL = "http://quotes.toscrape.com/"

print("Mise en place du cache (quotes_cache.sqlite)...")
requests_cache.install_cache('quotes_cache', backend='sqlite', expire_after=3600)

def get_soup(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        print(f"GET {url} (Cached: {response.from_cache})")
        return BeautifulSoup(response.content, 'lxml')
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la requête vers {url}: {e}")
        return None

def extract_author_details(author_page_url):
    soup = get_soup(author_page_url)
    if not soup:
        return {}

    author_details = soup.find('div', class_='author-details')
    if not author_details:
        return {}

    bio = ""
    date_deces = None
    
    bio_tag = author_details.find('h3', class_='author-title')
    if bio_tag and bio_tag.next_sibling:
        bio = bio_tag.next_sibling.strip()
        if bio == "":
             bio_tag = bio_tag.find_next_sibling('p')
             if bio_tag:
                 bio = bio_tag.text.strip()
        
        bio = re.sub(r'\s+Read more on Goodreads\.$', '', bio)


    born_date = author_details.find('span', class_='author-born-date').text.strip()
    born_location = author_details.find('span', class_='author-born-location').text.strip()
    
    death_tag = author_details.find('span', class_='author-died-date')
    if death_tag:
        date_deces = death_tag.text.strip()

    return {
        "biographie": bio,
        "date_naissance": born_date,
        "lieu_naissance": born_location,
        "date_deces": date_deces
    }

def scrape_all_quotes_and_authors():
    
    all_quotes = []
    authors_data = {}
    current_page_url = SITE_URL
    
    while current_page_url:
        soup = get_soup(current_page_url)
        if not soup:
            break
            
        for quote_div in soup.find_all('div', class_='quote'):
            text = quote_div.find('span', class_='text').text.strip()
            author_name = quote_div.find('small', class_='author').text.strip()
            author_page_link = quote_div.find('a')['href']
            author_url = urljoin(SITE_URL, author_page_link)
            
            tags = [tag.text for tag in quote_div.find_all('a', class_='tag')]
            
            all_quotes.append({
                "text": text,
                "author": author_name,
                "tags": tags
            })
            
            if author_name not in authors_data:
                print(f"Nouvel auteur trouvé : {author_name}. Scraping des détails...")
                details = extract_author_details(author_url)
                authors_data[author_name] = details
                time.sleep(0.1)

        next_li = soup.find('li', class_='next')
        if next_li:
            next_page_relative = next_li.find('a')['href']
            current_page_url = urljoin(SITE_URL, next_page_relative)
        else:
            current_page_url = None
            
    return all_quotes, authors_data

def build_and_export_graph(quotes, authors):
    print("Construction du graphe de relations...")
    G = nx.DiGraph()

    for author_name, details in authors.items():
        G.add_node(author_name, 
                   type='Auteur', 
                   bio=details.get('biographie'),
                   naissance=f"{details.get('date_naissance')} {details.get('lieu_naissance')}",
                   deces=details.get('date_deces')
                  )
    
    all_tags = set()
    for i, quote in enumerate(quotes):
        quote_id = f"citation_{i}"
        quote_text_short = quote['text'][:50] + "..."
        
        G.add_node(quote_id, type='Citation', text=quote['text'], label=quote_text_short)

        if quote['author'] in G:
            G.add_edge(quote_id, quote['author'], relation='CITÉ_PAR')
            
        for tag_name in quote['tags']:
            if tag_name not in all_tags:
                G.add_node(tag_name, type='Tag')
                all_tags.add(tag_name)
            
            G.add_edge(quote_id, tag_name, relation='A_POUR_TAG')

    print(f"Graphe créé : {G.number_of_nodes()} nœuds, {G.number_of_edges()} arêtes.")
    
    try:
        nx.write_graphml(G, "quotes_graph.graphml")
        print("Graphe exporté avec succès en 'quotes_graph.graphml'")
    except Exception as e:
        print(f"Erreur lors de l'exportation du graphe : {e}")

if __name__ == "__main__":
    print("Démarrage du scraping de 'Quotes to Scrape'...")
    
    quotes_list, authors_dict = scrape_all_quotes_and_authors()
    
    print(f"\n--- Scraping Terminé ---")
    print(f"{len(quotes_list)} citations trouvées.")
    print(f"{len(authors_dict)} auteurs uniques trouvés.")
    author_names_from_quotes = [q['author'] for q in quotes_list]
    author_counts = Counter(author_names_from_quotes)
    
    print("\n--- Auteurs les plus cités ---")
    for author, count in author_counts.most_common(5):
        print(f"{author}: {count} citations")

    build_and_export_graph(quotes_list, authors_dict)