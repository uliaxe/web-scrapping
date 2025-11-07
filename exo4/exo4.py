import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time

print("Démarrage du script d'analyse de BooksToScrape...")

def nettoyer_prix(prix_str):
    match = re.search(r'£([\d\.]+)', prix_str)
    if match:
        return float(match.group(1))
    return 0.0

def nettoyer_note(note_str_list):
    mapping = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}
    for classe in note_str_list:
        if classe in mapping:
            return mapping[classe]
    return 0

def est_en_stock(stock_str):
    return "In stock" in stock_str


BASE_URL = "https://books.toscrape.com/"
URL_CATALOGUE = "https://books.toscrape.com/catalogue/"

livres_data = []
session = requests.Session()

print("1. Récupération des catégories...")
try:
    reponse_accueil = session.get(BASE_URL)
    reponse_accueil.raise_for_status()
    soup_accueil = BeautifulSoup(reponse_accueil.text, 'html.parser')
    
    liens_categories = []
    for a in soup_accueil.select('.side_categories ul li ul li a'):
        liens_categories.append(BASE_URL + a['href'])
    
    print(f"   Trouvé {len(liens_categories)} catégories.")

except requests.exceptions.RequestException as e:
    print(f"Erreur lors de la récupération des catégories : {e}")
    exit()

print("\n2. Démarrage du scraping des livres (cela peut prendre un moment)...")
compteur_livres = 0

for cat_url in liens_categories:
    url_page_courante = cat_url
    nom_categorie = cat_url.split('/')[-2]

    while url_page_courante:
        try:
            time.sleep(0.5)
            reponse_page = session.get(url_page_courante)
            if reponse_page.status_code != 200:
                break 

            soup_page = BeautifulSoup(reponse_page.text, 'html.parser')

            liens_livres = soup_page.select('h3 a')
            for lien_livre in liens_livres:
                url_livre_relative = lien_livre['href'].replace('../../../', '')
                url_livre_absolue = URL_CATALOGUE + url_livre_relative
                
                try:
                    time.sleep(0.2)
                    rep_livre = session.get(url_livre_absolue)
                    if rep_livre.status_code != 200:
                        continue
                    
                    soup_livre = BeautifulSoup(rep_livre.text, 'html.parser')

                    titre = soup_livre.select_one('h1').text
                    prix = nettoyer_prix(soup_livre.select_one('.price_color').text)
                    note = nettoyer_note(soup_livre.select_one('p.star-rating')['class'])
                    dispo = est_en_stock(soup_livre.select_one('.availability').text.strip())
                    
                    livres_data.append({
                        'Titre': titre,
                        'Prix': prix,
                        'Note': note,
                        'En_Stock': dispo,
                        'Catégorie': nom_categorie
                    })
                    compteur_livres += 1
                
                except requests.exceptions.RequestException:
                    continue

            lien_next = soup_page.select_one('li.next a')
            if lien_next:
                url_page_courante = cat_url.rsplit('/', 1)[0] + '/' + lien_next['href']
            else:
                url_page_courante = None 
        except requests.exceptions.RequestException as e:
            print(f"Erreur lors du scraping de {url_page_courante}: {e}")
            url_page_courante = None
print(f"\nScraping terminé. Total de {compteur_livres} livres trouvés.")

df = pd.DataFrame(livres_data)

print("\nAperçu des données collectées :")
print(df.head())

print("\n--- Phase 3 : Analyse des Données ---")

print("\nPrix moyen par Catégorie :")
prix_par_categorie = df.groupby('Catégorie')['Prix'].mean().sort_values(ascending=False)
print(prix_par_categorie.to_string())

print("\nPrix moyen par Note :")
prix_par_note = df.groupby('Note')['Prix'].mean().sort_index()
print(prix_par_note)

print("\nTendances de prix (Statistiques descriptives) :")
print(df['Prix'].describe())

print("\nLivres en rupture de stock :")
livres_hors_stock = df[df['En_Stock'] == False]
if livres_hors_stock.empty:
    print("Tous les livres sont en stock.")
else:
    print(f"   Total de {len(livres_hors_stock)} livres hors stock.")
    print(livres_hors_stock[['Titre', 'Catégorie']])

print("\nDistribution des Notes (Ratings) :")
distribution_notes = df['Note'].value_counts().sort_index()
print(distribution_notes)

print("\n--- Analyse Terminée ---")