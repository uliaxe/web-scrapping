import pandas as pd
import json
import logging
from pydantic import BaseModel, Field, ValidationError, field_validator
from typing import Optional
from scipy.stats import zscore
import numpy as np

INPUT_FILE = 'books_data_resilient.jsonl' 
INPUT_FORMAT = 'jsonl'
CLEAN_OUTPUT_FILE = 'books_data_clean.csv'
REPORT_FILE = 'data_quality_report.txt'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(REPORT_FILE, mode='w'),
        logging.StreamHandler()
    ]
)


class BookModel(BaseModel):

    titre: str = Field(min_length=1)
    url_detail: str
    prix_gbp: float = Field(gt=0)
    note_sur_5: int = Field(ge=0, le=5)
    description: Optional[str] = None
    stock_disponible: int = Field(ge=0) 
    url_image_hd: str

    @field_validator('titre', 'description')
    def clean_text(cls, v):
        if v is None:
            return v
        v = v.strip()
        v = " ".join(v.split())
        return v
    
    @field_validator('url_detail', 'url_image_hd')
    def check_url_format(cls, v):
        if not v.startswith(('http://', 'https://')):
            raise ValueError('URL doit commencer par http:// ou https://')
        return v


def load_and_validate_data(filepath, file_format):
    valid_records = []
    invalid_records_details = []
    # Default metrics to return on early error cases so callers can rely on a stable structure
    default_metrics = {
        "total_records": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "validation_errors_by_field": {},
        "invalid_record_examples": []
    }
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            if file_format == 'jsonl':
                raw_lines = f.readlines()
                raw_data = [json.loads(line) for line in raw_lines]
            elif file_format == 'json':
                raw_data = json.load(f)
            else:
                raise ValueError("Format de fichier non supporté. Utilisez 'json' ou 'jsonl'.")
                
    except FileNotFoundError:
        logging.error(f"Erreur: Fichier '{filepath}' non trouvé.")
        return pd.DataFrame(), default_metrics
    except json.JSONDecodeError:
        logging.error(f"Erreur: Impossible de décoder le JSON dans '{filepath}'.")
        return pd.DataFrame(), default_metrics
    
    if not isinstance(raw_data, list):
        logging.error("Erreur: Le JSON n'est pas une liste d'objets.")
        return pd.DataFrame(), default_metrics

    logging.info(f"Début de la validation pour {len(raw_data)} enregistrements...")

    error_counts = {}

    for i, record in enumerate(raw_data):
        try:
            clean_record = BookModel.model_validate(record)
            valid_records.append(clean_record.model_dump())
            
        except ValidationError as e:
            errors = e.errors()
            invalid_records_details.append({'index': i, 'record': record, 'errors': errors})
            for error in errors:
                field = error['loc'][0] if error['loc'] else 'unknown_field'
                error_counts[field] = error_counts.get(field, 0) + 1

    logging.info(f"Validation terminée. {len(valid_records)} valides, {len(invalid_records_details)} invalides.")
    
    quality_metrics = {
        "total_records": len(raw_data),
        "valid_records": len(valid_records),
        "invalid_records": len(invalid_records_details),
        "validation_errors_by_field": error_counts,
        "invalid_record_examples": invalid_records_details[:5]
    }

    return pd.DataFrame(valid_records), quality_metrics

def analyze_and_clean_dataframe(df):
    
    if df.empty:
        logging.warning("DataFrame vide, aucune analyse post-validation à effectuer.")
        return df, {}
        
    analysis_metrics = {}

    missing_desc_count = df['description'].isnull().sum()
    if missing_desc_count > 0:
        logging.info(f"Imputation de {missing_desc_count} descriptions manquantes.")
        df['description'] = df['description'].fillna('Description non disponible')
    analysis_metrics['imputed_descriptions'] = missing_desc_count

    if len(df) > 2:
        df['prix_zscore'] = zscore(df['prix_gbp'])
        anomalies = df[np.abs(df['prix_zscore']) > 3]
        analysis_metrics['anomalies_prix_detectees'] = len(anomalies)
        analysis_metrics['anomalies_prix_exemples'] = anomalies[['titre', 'prix_gbp', 'prix_zscore']].to_dict('records')
        logging.info(f"{len(anomalies)} anomalies de prix détectées (Z-score > 3).")
    else:
        logging.warning("Pas assez de données pour une détection d'anomalies fiable.")
        analysis_metrics['anomalies_prix_detectees'] = 0

    return df, analysis_metrics


def generate_quality_report(initial_metrics, analysis_metrics):
    logging.info("\n" + "="*50)
    logging.info("RAPPORT FINAL DE QUALITÉ DES DONNÉES")
    logging.info("="*50)
    
    logging.info(f"Total des enregistrements traités : {initial_metrics.get('total_records', 0)}")
    logging.info(f"Enregistrements valides : {initial_metrics.get('valid_records', 0)}")
    logging.info(f"Enregistrements invalides : {initial_metrics.get('invalid_records', 0)}")
    
    if initial_metrics.get('invalid_records', 0) > 0:
        logging.warning("\n--- Détail des erreurs de validation (par champ) ---")
        for field, count in initial_metrics.get('validation_errors_by_field', {}).items():
            logging.warning(f"  - Champ '{field}': {count} erreurs")
            
        logging.warning("\n--- Exemples d'enregistrements invalides ---")
        for ex in initial_metrics.get('invalid_record_examples', []):
            logging.warning(f"  - Ligne {ex['index']}: Erreurs -> {ex['errors']}")

    logging.info("\n" + "="*50)
    logging.info("ANALYSE POST-VALIDATION")
    logging.info("="*50)
    logging.info(f"Descriptions manquantes imputées : {analysis_metrics.get('imputed_descriptions', 'N/A')}")
    logging.info(f"Anomalies de prix (Z-score) : {analysis_metrics.get('anomalies_prix_detectees', 'N/A')}")
    
    if analysis_metrics.get('anomalies_prix_detectees', 0) > 0:
        logging.info("--- Exemples d'anomalies de prix ---")
        for ex in analysis_metrics.get('anomalies_prix_exemples', []):
            logging.info(f"  - Titre: {ex['titre'][:40]}... | Prix: £{ex['prix_gbp']:.2f} (Z-score: {ex['prix_zscore']:.2f})")

if __name__ == "__main__":
    logging.info(f"--- Démarrage du Pipeline de Nettoyage ---")
    logging.info(f"Source: {INPUT_FILE} | Rapport: {REPORT_FILE}")

    df_clean, validation_metrics = load_and_validate_data(INPUT_FILE, INPUT_FORMAT)

    df_final, analysis_metrics = analyze_and_clean_dataframe(df_clean)

    generate_quality_report(validation_metrics, analysis_metrics)
    
    if not df_final.empty:
        try:
            df_final.to_csv(CLEAN_OUTPUT_FILE, index=False, encoding='utf-8-sig')
            logging.info(f"Données nettoyées sauvegardées dans : {CLEAN_OUTPUT_FILE}")
        except IOError as e:
            logging.error(f"Impossible de sauvegarder le CSV propre : {e}")
    else:
        logging.warning("Aucune donnée valide n'a été sauvegardée.")
        
    logging.info("--- Pipeline Terminé ---")