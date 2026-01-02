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

## Exemples de chaînes de connexion


### Sommaire

*   SQL Server — SQLAlchemy + pyodbc (ODBC requis)
    *   Basique (login/mot de passe)
    *   Authentification Windows (Trusted Connection)
    *   Azure SQL (TLS/Encrypt)
    *   DSN
    *   DSN‑less avec `odbc_connect` encodé
*   SQL Server — pytds (sans ODBC, sans SQLAlchemy)
*   SQL Server — mssql‑python (sans ODBC, driver Microsoft)
*   PostgreSQL — SQLAlchemy + psycopg2
*   CSV — Local
*   Astuces & Dépannage
*   Variables d’environnement (optionnel)

***

### SQL Server — SQLAlchemy + pyodbc (ODBC requis)

> **Pré‑requis** : un **driver ODBC** Microsoft est installé (ex. *ODBC Driver 17 for SQL Server* ou *18*).  
> **Important** : dans l’URL, les **espaces** du nom de driver sont encodés en `+`.

#### Basique (login/mot de passe)

```python
# Chaîne SQLAlchemy (pour l’UI “SQL (SQLAlchemy/ODBC)”)
mssql+pyodbc://UTILISATEUR:MOTDEPASSE@SERVEUR/NOM_BASE?driver=ODBC+Driver+17+for+SQL+Server
```

#### Authentification Windows (Trusted Connection)

```python
mssql+pyodbc://@SERVEUR/NOM_BASE?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes
```

#### Azure SQL (TLS/Encrypt)

```python
mssql+pyodbc://UTILISATEUR:MOTDEPASSE@serveur.database.windows.net/NOM_BASE
?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no
```

> Remarque : **`Encrypt=yes`** est recommandé/obligatoire selon la politique ;  
> **`TrustServerCertificate=yes`** peut dépanner en dev mais à éviter en prod.

#### DSN

```python
# DSN préconfiguré côté Windows/macOS/Linux
mssql+pyodbc://@MonDSN
# ou avec login/mot de passe
mssql+pyodbc://user:pass@MonDSN
```

***

### SQL Server — pytds (sans ODBC, sans SQLAlchemy)

> **Avantage** : pas besoin d’installer de **driver système** ODBC ; fonctionne via **pip**.

```text
# Onglet : “SQL Server (pytds)”
Hôte/IP      : MONSERVEUR
Base         : MaBase
Utilisateur  : monuser
Mot de passe : ********
Port         : 1433
Requête SQL  : SELECT TOP 100 * FROM dbo.Clients
```

> L’UI propose la case **“Forcer toutes les colonnes en string”** (recommandé) pour éviter les conflits de types et supprimer les “.0” indésirables.

***

### SQL Server — mssql‑python (sans ODBC, driver Microsoft)

> **Avantage** : driver **Microsoft** en **pip** (sur Windows), **sans** ODBC.

```text
# Onglet : “SQL Server (mssql-python)”
Hôte/IP      : MONSERVEUR
Base         : MaBase
Utilisateur  : monuser
Mot de passe : ********
Port         : 1433
Requête SQL  : SELECT TOP 100 * FROM dbo.Fournisseurs
```

> L’UI propose aussi **“Forcer toutes les colonnes en string”**.

***

### PostgreSQL — SQLAlchemy + psycopg2

#### Basique

```python
postgresql+psycopg2://UTILISATEUR:MOTDEPASSE@HOTE:5432/NOM_BASE
```

#### Avec SSL

```python
postgresql+psycopg2://user:pass@pg-host:5432/dbname?sslmode=require
```

***

### CSV — Local

```text
# Onglet : “CSV”
Uploader : MonFichier.csv
```

> Le loader lit en `dtype="string"` + formatage intelligent pour **éviter “.0”** et **préserver les zéros en tête**.

***

### Astuces & Dépannage

*   **Nom du driver ODBC encodé** (SQLAlchemy) :  
    `?driver=ODBC+Driver+17+for+SQL+Server` (**pas** d’espaces).
*   **Erreur “IM002 … Data source name not found …”** :  
    le driver ODBC indiqué n’existe pas / n’est pas installé → utiliser **pytds** ou **mssql‑python** si tu ne peux rien installer.
*   **Conflits Pandas `merge` (object vs int64)** :  
    coche **“Forcer toutes les colonnes en string”** ; les anti‑joins d’échantillons sont sécurisés (cast `str`) dans le code.
*   **Affichage “,0” ou “.0”** :  
    le **formatage intelligent** enlève les décimales inutiles (`311.0 → "311"`) et conserve les **codes** tels quels (`0000000012` reste `0000000012`).

***

### Variables d’environnement (optionnel)

Tu peux stocker tes chaînes dans `.env` (copie de `.env.example`) :

```env
# .env
MSSQL_CONN_STR=mssql+pyodbc://user:pass@SERVEUR/DB?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=yes
POSTGRES_CONN_STR=postgresql+psycopg2://user:pass@localhost:5432/dbname
```

Puis les coller dans l’UI au moment de l’utilisation.

***

#### Exemples rapides prêts à coller

```text
# SQL Server (SQLAlchemy/ODBC, login)
mssql+pyodbc://sa:SuperSecret@SQLHOST/CRM?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=yes
```

```text
# SQL Server (SQLAlchemy/ODBC, Windows)
mssql+pyodbc://@SQLHOST/CRM?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes
```

```text
# SQL Server (SQLAlchemy/ODBC, DSN)
mssql+pyodbc://@MonDSN
```

```text
# SQL Server (SQLAlchemy/ODBC, odbc_connect encodé)
mssql+pyodbc:///?odbc_connect=DRIVER%3D%7BODBC%20Driver%2018%20for%20SQL%20Server%7D%3BSERVER%3Dtcp%3ASQLHOST%2C1433%3BDATABASE%3DCRM%3BUID%3Duser%3BPWD%3Dpass%3BEncrypt%3Dyes%3BTrustServerCertificate%3Dyes%3B
```

```text
# PostgreSQL (SQLAlchemy/psycopg2)
postgresql+psycopg2://user:pass@localhost:5432/ma_base
```

```text
# SQL Server (pytds) — remplir les champs de l’onglet dédié :
Hôte=SQLHOST, Base=CRM, User=user, Pwd=pass, Port=1433
```

```text
# SQL Server (mssql-python) — idem dans l’onglet dédié :
Hôte=SQLHOST, Base=CRM, User=user, Pwd=pass, Port=1433
```

***
