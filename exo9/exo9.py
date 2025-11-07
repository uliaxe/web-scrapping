import requests
from bs4 import BeautifulSoup
import logging
import sys

SITE_URL = "http://quotes.toscrape.com/"
LOGIN_URL = "http://quotes.toscrape.com/login"
LOGOUT_URL = "http://quotes.toscrape.com/logout"

USERNAME = "admin"
PASSWORD = "admin"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def perform_login():
    with requests.Session() as session:
        logging.info(f"Accès à la page de login : {LOGIN_URL}")
        try:
            response_get = session.get(LOGIN_URL, timeout=10)
            response_get.raise_for_status()
        except requests.RequestException as e:
            logging.error(f"Impossible de joindre la page de login : {e}")
            return False

        soup = BeautifulSoup(response_get.content, 'lxml')
        
        try:
            csrf_token = soup.find('input', {'name': 'csrf_token'})['value']
            logging.info(f"Token CSRF récupéré : {csrf_token[:10]}...")
        except TypeError:
            logging.error("Impossible de trouver le token CSRF. La structure de la page a peut-être changé.")
            return False
        
        login_payload = {
            'username': USERNAME,
            'password': PASSWORD,
            'csrf_token': csrf_token
        }
        
        logging.info(f"Tentative de connexion en tant que '{USERNAME}'...")
        try:
            headers = {'Referer': LOGIN_URL}
            response_post = session.post(LOGIN_URL, data=login_payload, headers=headers)
            response_post.raise_for_status()
        except requests.RequestException as e:
            logging.error(f"La requête POST de login a échoué : {e}")
            return False
        if response_post.url != LOGIN_URL:
            soup_post_login = BeautifulSoup(response_post.content, 'lxml')
            logout_link = soup_post_login.find('a', href='/logout')
            
            if logout_link:
                user_greet_elem = (
                    soup_post_login.find('p', class_='login')
                    or soup_post_login.find('p', class_='navbar-text')
                    or soup_post_login.find('div', class_='alert')
                    or soup_post_login.find('small')
                )

                if user_greet_elem and user_greet_elem.text:
                    user_greet = user_greet_elem.text.strip()
                else:
                    parent = logout_link.find_parent()
                    user_greet = parent.get_text(separator=' ', strip=True) if parent else 'Utilisateur connecté'

                logging.info(f"SUCCÈS : Connexion réussie. {user_greet}")

                logging.info("Test de la déconnexion...")
                response_logout = session.get(LOGOUT_URL, headers={'Referer': SITE_URL})

                soup_logout = BeautifulSoup(response_logout.content, 'lxml')
                login_link = soup_logout.find('a', href='/login')
                
                if login_link:
                    logging.info("SUCCÈS : Déconnexion réussie.")
                    return True
                else:
                    logging.warning("ÉCHEC : La déconnexion semble avoir échoué.")
                    return False
            else:
                logging.error("ÉCHEC : Connexion échouée. Bouton 'Logout' non trouvé.")
                return False
        else:
            soup_error = BeautifulSoup(response_post.content, 'lxml')
            error_msg = soup_error.find('p', class_='error')
            if error_msg:
                logging.error(f"ÉCHEC : Connexion échouée. Message du site : {error_msg.text.strip()}")
            else:
                logging.error("ÉCHEC : Connexion échouée pour une raison inconnue.")
            return False

if __name__ == "__main__":
    
    success = perform_login()
    
    if success:
        print("\nFlux d'authentification (Login/Logout) complété avec succès.")
    else:
        print("\nLe flux d'authentification a échoué. Vérifiez les logs.")