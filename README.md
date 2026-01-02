# Comparateur de Données (SQL Server / PostgreSQL / CSV) – Python + Pandas + SQLAlchemy + Streamlit

Ce projet fournit:
- **Chargement des données** depuis :
  - Une requête **SQL** exécutée via **SQLAlchemy** (SQL Server, PostgreSQL)
  - Un **fichier CSV** local
- **Comparaison de deux DataFrames** pour vérifier :
  - La **volumétrie** (comptage des lignes)
  - L'**identité des données** (égalité des lignes, indépendamment de l'ordre)
- Une **application web** simple (**Streamlit**) permettant de choisir les sources (SQL/CSV), lancer la comparaison et télécharger les écarts.

## Installation
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Lancer l'application
```bash
streamlit run app/streamlit_app.py
```
