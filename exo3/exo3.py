import requests
from bs4 import BeautifulSoup
import pandas as pd
import argparse
import re
from datetime import datetime
import sys
import dateparser

SITE_URL = "https://realpython.github.io/fake-jobs/"

def scrape_all_jobs(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Erreur: Impossible de joindre {url}. {e}", file=sys.stderr)
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    job_cards = soup.find_all('div', class_='card-content')
    
    jobs_data = []
    for card in job_cards:
        title = card.find('h2', class_='title').text.strip()
        company = card.find('h3', class_='company').text.strip()
        location = card.find('p', class_='location').text.strip()
        date_posted_raw = card.find('time').text.strip()
        
        link_tag = card.find('a', string=re.compile(r'Apply|Learn More'))
        apply_url = link_tag['href'] if link_tag else None
        
        jobs_data.append({
            "titre": title,
            "entreprise": company,
            "localisation": location,
            "date_publication_raw": date_posted_raw,
            "url_application": apply_url
        })
    return jobs_data

def clean_and_process_data(jobs_list):
    if not jobs_list:
        return pd.DataFrame()
        
    df = pd.DataFrame(jobs_list)
    df['date_publication'] = df['date_publication_raw'].apply(
        lambda x: dateparser.parse(x) if x else None
    )
    df['date_publication_std'] = pd.to_datetime(df['date_publication']).dt.strftime('%Y-%m-%d')
    df['url_valide'] = df['url_application'].apply(
        lambda x: bool(re.match(r'^https?://', x)) if x else False
    )
    
    df['est_doublon'] = df.duplicated(
        subset=['titre', 'entreprise', 'localisation'], 
        keep='first'
    )

    
    return df

def filter_data(df, keyword, location):
    filtered_df = df.copy()
    
    if keyword:
        print(f"Filtrage des titres contenant : '{keyword}'")
        filtered_df = filtered_df[
            filtered_df['titre'].str.contains(keyword, case=False, na=False)
        ]

    if location:
        print(f"Filtrage des localisations contenant : '{location}'")
        filtered_df = filtered_df[
            filtered_df['localisation'].str.contains(location, case=False, na=False)
        ]
        
    return filtered_df

def generate_and_print_stats(df):
    print("\n--- Statistiques sur les offres filtrées ---")
    
    if df.empty:
        print("Aucune donnée à analyser.")
        return

    print("\nOffres par localisation (Top 10) :")
    stats_loc = df['localisation'].value_counts()
    print(stats_loc.head(10).to_string())
    
    print("\nOffres par entreprise (Top 10) :")
    stats_comp = df['entreprise'].value_counts()
    print(stats_comp.head(10).to_string())
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(
        description="Scraper 'Fake Jobs' avec filtres dynamiques."
    )
    parser.add_argument(
        '-k', '--keyword', 
        type=str, 
        default="Python", 
        help="Mot-clé à rechercher dans le titre (défaut: 'Python')"
    )
    parser.add_argument(
        '-l', '--location', 
        type=str, 
        help="Mot-clé à rechercher dans la localisation"
    )
    parser.add_argument(
        '-s', '--stats', 
        action='store_true', 
        help="Afficher les statistiques des résultats filtrés"
    )
    parser.add_argument(
        '--include-duplicates', 
        action='store_true', 
        help="Inclure les doublons dans le résultat"
    )
    parser.add_argument(
        '-o', '--output', 
        type=str, 
        default="fake_jobs_results.csv", 
        help="Nom du fichier CSV de sortie (défaut: 'fake_jobs_results.csv')"
    )
    
    args = parser.parse_args()

    print("Démarrage du scraping 'Fake Jobs'...")
    all_jobs_raw = scrape_all_jobs(SITE_URL)
    
    print(f"{len(all_jobs_raw)} annonces brutes trouvées.")
    
    df_cleaned = clean_and_process_data(all_jobs_raw)
    
    if df_cleaned.empty:
        print("Aucune donnée n'a pu être traitée. Arrêt.")
        sys.exit(1)
        
    df_filtered = filter_data(df_cleaned, args.keyword, args.location)
    
    if not args.include_duplicates:
        print("Suppression des doublons...")
        final_df = df_filtered[~df_filtered['est_doublon']].copy()
    else:
        print("Conservation des doublons.")
        final_df = df_filtered.copy()

    columns_to_export = [
        'titre', 'entreprise', 'localisation', 'date_publication_std', 
        'url_application', 'url_valide'
    ]
    final_df_export = final_df[[col for col in columns_to_export if col in final_df]]


    print(f"\n{len(final_df_export)} annonces correspondent aux filtres.")
    if args.stats:
        generate_and_print_stats(final_df_export)

    try:
        final_df_export.to_csv(args.output, encoding='utf-8-sig', index=False)
        print(f"\nRésultats sauvegardés avec succès dans : {args.output}")
    except IOError as e:
        print(f"Erreur lors de la sauvegarde du CSV : {e}", file=sys.stderr)