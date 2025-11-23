import streamlit as st
import pandas as pd
import os
import io # Nécessaire pour gérer les fichiers en mémoire

# --- Configuration des chemins et des limites ---
OUTPUT_EXCEL_FILE = 'output_data.xlsx'
OVERFLOW_CSV_FILE = 'overflow_data.csv'
MAX_EXCEL_CELL_LENGTH = 32767

# --- Fonction de Traitement Principal (Adaptée pour Streamlit) ---
def process_data(uploaded_file, excel_path, overflow_path, max_length):
    """
    Lit le fichier téléchargé, effectue le traitement, et écrit les fichiers de sortie.
    Retourne un dictionnaire de succès et les messages de log.
    """
    st.info("🚀 Démarrage du traitement du fichier...")
    logs = []
    
    # Lecture du fichier CSV directement à partir du FileUploader de Streamlit
    try:
        # L'objet file_uploader peut être lu directement par Pandas
        df = pd.read_csv(uploaded_file)
    except Exception as e:
        logs.append(f"❌ Erreur lors de la lecture du fichier CSV : {e}")
        st.error(logs[-1])
        return {"success": False, "logs": logs}

    initial_count = len(df)
    logs.append(f"✅ Fichier CSV lu avec succès. Nombre initial d'enregistrements : {initial_count}")
    st.success(f"Fichier lu : {uploaded_file.name}")

    # --- 1. Suppression des colonnes indésirables ---
    columns_to_drop = ['DefinedSchemaSystemId', 'ESRS', 'DR', 'SUBDR', 'TableLineItems', 'ElementLabel']
    cols_present = [col for col in columns_to_drop if col in df.columns]
    df_cleaned = df.drop(columns=cols_present, axis=1, errors='ignore') # errors='ignore' est plus sûr

    logs.append(f"✅ Colonnes {cols_present} supprimées.")
    st.progress(20, 'Colonnes supprimées...')

    # --- 2. Déduplication des enregistrements ---
    subset_for_deduplication = [
        'Entity', 'Period', 'Element', 
        'DIM1', 'VALUE1', 'DIM2', 'VALUE2', 'DIM3', 'VALUE3', 'DIM4', 'VALUE4', 
        'DIM5', 'VALUE5', 'DIM6', 'VALUE6', 'DIM7', 'VALUE7', 'DIM8', 'VALUE8', 
        'DIM9', 'VALUE9', 'DIM10', 'VALUE10'
    ]

    # S'assurer que toutes les colonnes clés existent pour la déduplication
    valid_subset = [col for col in subset_for_deduplication if col in df_cleaned.columns]
    
    df_cleaned.drop_duplicates(subset=valid_subset, keep='first', inplace=True)
    
    deduplicated_count = len(df_cleaned)
    logs.append(f"✅ Déduplication terminée. {initial_count - deduplicated_count} doublon(s) retiré(s). Nombre d'enregistrements restants : {deduplicated_count}")
    st.progress(40, 'Déduplication effectuée...')

    # --- 3. Gestion des données volumineuses ---
    
    # Convertir 'Value' en string et gérer les NaN pour le calcul de la longueur
    if 'Value' in df_cleaned.columns:
        df_cleaned['Value'] = df_cleaned['Value'].astype(str).fillna('')
        long_value_mask = df_cleaned['Value'].str.len() > max_length
    else:
        # S'il n'y a pas de colonne 'Value', on suppose qu'il n'y a pas de problème de taille
        long_value_mask = pd.Series([False] * len(df_cleaned), index=df_cleaned.index)

    df_overflow = df_cleaned[long_value_mask].copy()
    df_excel = df_cleaned[~long_value_mask].copy()

    logs.append(f"📊 Analyse des longueurs : {len(df_overflow)} enregistrement(s) dépassent {max_length} caractères.")
    st.progress(60, 'Analyse des tailles de cellules...')

    # --- 4. Sauvegarde des fichiers de sortie (en mémoire pour Streamlit) ---
    results = {}
    
    # Export CSV d'excédent
    if not df_overflow.empty:
        csv_buffer = io.StringIO()
        df_overflow.to_csv(csv_buffer, index=False, encoding='utf-8')
        results['overflow_csv'] = csv_buffer.getvalue().encode('utf-8') # Données binaires pour le téléchargement
        logs.append(f"💾 {len(df_overflow)} enregistrement(s) longs prêts à être téléchargés au format CSV.")

    # Export Excel
    if not df_excel.empty:
        # Pour Excel, on utilise BytesIO car c'est un format binaire
        excel_buffer = io.BytesIO()
        try:
            df_excel.to_excel(excel_buffer, index=False, engine='xlsxwriter')
            excel_buffer.seek(0)
            results['excel'] = excel_buffer.getvalue() # Données binaires pour le téléchargement
            logs.append(f"💾 {len(df_excel)} enregistrement(s) normaux prêts à être téléchargés au format Excel.")
        except Exception as e:
            logs.append(f"❌ Erreur lors de l'écriture du fichier Excel : {e}")
            st.error(logs[-1])
            st.progress(100, 'Échec de l\'écriture Excel.')
            return {"success": False, "logs": logs}
    else:
        logs.append("⚠️ Le DataFrame Excel est vide. Aucun fichier Excel ne sera créé.")
    
    st.progress(100, 'Traitement terminé.')
    logs.append("🎉 Traitement terminé avec succès.")
    return {"success": True, "logs": logs, "files": results}


# --- Structure de l'Application Streamlit ---

st.set_page_config(page_title="CSV Data Processor", layout="wide")

st.title("⚙️ Outil de Traitement de Fichiers CSV")
st.markdown("Téléversez votre fichier CSV pour nettoyer les données, supprimer les doublons et générer un fichier Excel optimisé.")

# Fichier Uploader
uploaded_file = st.file_uploader(
    "1. Choisissez votre fichier CSV", 
    type=['csv'], 
    help="Le fichier doit contenir les colonnes Entity, Period, Element, Value, DIM1-10, VALUE1-10..."
)

# Affichage des logs
log_container = st.container()

if uploaded_file is not None:
    # Le bouton lance le traitement
    if st.button("2. Lancer le Traitement des Données", type="primary"):
        
        # Le st.spinner affiche un message d'attente
        with st.spinner('Traitement en cours... Veuillez patienter.'):
            # Exécute la fonction de traitement
            result = process_data(uploaded_file, OUTPUT_EXCEL_FILE, OVERFLOW_CSV_FILE, MAX_EXCEL_CELL_LENGTH)
        
        # Affichage des logs dans l'interface
        with log_container:
            st.subheader("Journal des Opérations")
            for log in result['logs']:
                st.code(log) # Affiche les logs dans un bloc de code pour la clarté

        # --- Section de Téléchargement ---
        if result['success'] and 'files' in result:
            st.balloons() # Petite animation de succès
            st.subheader("✅ Fichiers de Sortie")
            
            files = result['files']

            # Bouton de Téléchargement Excel
            if 'excel' in files:
                st.download_button(
                    label="Télécharger le fichier Excel (Données Normales)",
                    data=files['excel'],
                    file_name=OUTPUT_EXCEL_FILE,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    help="Ce fichier contient les données prêtes pour Excel (cellules < 32k caractères)."
                )

            # Bouton de Téléchargement CSV d'excédent
            if 'overflow_csv' in files:
                st.download_button(
                    label="Télécharger le fichier CSV (Données Excédentaires)",
                    data=files['overflow_csv'],
                    file_name=OVERFLOW_CSV_FILE,
                    mime="text/csv",
                    help="Ce fichier contient les enregistrements dont la colonne 'Value' dépasse la limite de 32k caractères d'Excel."
                )