
import streamlit as st
import pandas as pd
import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.loaders.csv_loader import load_from_csv
from src.loaders.sql_loader import load_from_sql
from src.loaders.sqlserver_pytds import load_from_sqlserver_pytds
from src.loaders.sqlserver_mssqlpy import load_from_sqlserver_mssqlpy
from src.compare.dataframe_compare import compare_dataframes

st.set_page_config(page_title='Comparateur de Données', layout='wide')
st.title('🔍 Comparateur de Données (SQL / CSV)')
st.caption('Pandas + SQLAlchemy + Streamlit + pytds + mssql-python')

# Init session state defaults
init_keys = [
    'A_df','B_df','A_type','B_type',
    'A_conn','B_conn','A_sql','B_sql','A_as_string','B_as_string',
    'A_pytds_host','A_pytds_db','A_pytds_user','A_pytds_password','A_pytds_port','A_pytds_sql','A_pytds_as_string',
    'B_pytds_host','B_pytds_db','B_pytds_user','B_pytds_password','B_pytds_port','B_pytds_sql','B_pytds_as_string',
    'A_mpy_host','A_mpy_db','A_mpy_user','A_mpy_password','A_mpy_port','A_mpy_sql','A_mpy_as_string',
    'B_mpy_host','B_mpy_db','B_mpy_user','B_mpy_password','B_mpy_port','B_mpy_sql','B_mpy_as_string',
]
for key in init_keys:
    if key not in st.session_state:
        if key.endswith('_as_string'):
            st.session_state[key] = True
        elif key.endswith('_port'):
            st.session_state[key] = 1433
        else:
            st.session_state[key] = None

def load_ui(label: str, side_key: str):
    st.subheader(label)
    typ = st.radio(
        f"Type de source – {label}",
        ['CSV', 'SQL (SQLAlchemy/ODBC)', 'SQL Server (pytds)', 'SQL Server (mssql-python)'],
        horizontal=True,
        key=f'{side_key}_type_radio',
        index=0 if (st.session_state.get(f'{side_key}_type') or 'CSV')=='CSV' else 1 if st.session_state.get(f'{side_key}_type')=='SQL (SQLAlchemy/ODBC)' else 2 if st.session_state.get(f'{side_key}_type')=='SQL Server (pytds)' else 3
    )
    st.session_state[f'{side_key}_type'] = typ

    
    if typ == 'CSV':
        col1, col2 = st.columns([1,1])
        with col1:
            delim_label = f"{label}: Délimiteur CSV"
            # Propose les séparateurs courants : virgule, point-virgule, tabulation, |, :
            delim = st.selectbox(
                delim_label,
                options=[',',';','\\t','|',':'],
                index=0,
                key=f'{side_key}_csv_delim'
            )
            if delim == '\\t':
                st.caption('Délimiteur actuel: TAB')
        with col2:
            st.caption("Format attendu: encodage UTF-8 par défaut.")

        up = st.file_uploader(f"{label}: Charger un CSV", type=['csv'], key=f'{side_key}_csv')
        if up is not None:
            try:
                # Normalise la valeur de l’UI : si '\\t', on passe bien '\t' à pandas
                sep = '\t' if st.session_state.get(f'{side_key}_csv_delim') in ('\\t', '\t') \
                        else st.session_state.get(f'{side_key}_csv_delim', ',')

                df = pd.read_csv(up, dtype='string', sep=sep)
                # Trim de sécurité
                for c in df.columns:
                    if pd.api.types.is_string_dtype(df[c]):
                        df[c] = df[c].str.strip()
                st.session_state[f'{side_key}_df'] = df
                st.success(f"{label}: {len(df)} lignes chargées (CSV, sep='{sep if sep!='\\t' else 'TAB'}')")
                st.dataframe(df.head(20))
            except Exception as e:
                st.error(f"{label}: erreur de lecture CSV – {e}")
        else:
            df_prev = st.session_state.get(f'{side_key}_df')
            if isinstance(df_prev, pd.DataFrame):
                st.info(f"{label}: {len(df_prev)} lignes en mémoire (CSV)")
                st.dataframe(df_prev.head(20))


    elif typ == 'SQL (SQLAlchemy/ODBC)':
        conn = st.text_input(f"{label}: Chaîne de connexion SQLAlchemy", value=st.session_state.get(f'{side_key}_conn') or '')
        sql = st.text_area(f"{label}: Requête SQL", value=st.session_state.get(f'{side_key}_sql') or '', height=150)
        st.session_state[f'{side_key}_conn'] = conn
        st.session_state[f'{side_key}_sql'] = sql
        as_str_default = st.session_state.get(f'{side_key}_as_string', True)
        st.checkbox(f"{label}: Forcer toutes les colonnes en string (SQL)", value=as_str_default, key=f'{side_key}_as_string')
        if conn and sql and st.button(f"Exécuter {label}", key=f'{side_key}_exec_sqlalchemy'):
            with st.spinner('Chargement depuis SQL (SQLAlchemy/ODBC)...'):
                try:
                    df = load_from_sql(conn, sql, as_string=st.session_state[f'{side_key}_as_string'])
                    st.session_state[f'{side_key}_df'] = df
                    st.success(f"{label}: {len(df)} lignes chargées (SQL)")
                    st.dataframe(df.head(20))
                except Exception as e:
                    st.error(f"{label}: erreur SQL – {e}")
        else:
            df_prev = st.session_state.get(f'{side_key}_df')
            if isinstance(df_prev, pd.DataFrame):
                st.info(f"{label}: {len(df_prev)} lignes en mémoire (SQL)")
                st.dataframe(df_prev.head(20))

    elif typ == 'SQL Server (pytds)':
        host = st.text_input(f"{label} (pytds): Hôte/IP", value=st.session_state.get(f'{side_key}_pytds_host') or '')
        db = st.text_input(f"{label} (pytds): Base", value=st.session_state.get(f'{side_key}_pytds_db') or '')
        user = st.text_input(f"{label} (pytds): Utilisateur", value=st.session_state.get(f'{side_key}_pytds_user') or '')
        pw = st.text_input(f"{label} (pytds): Mot de passe", value=st.session_state.get(f'{side_key}_pytds_password') or '', type='password')
        port = st.number_input(f"{label} (pytds): Port", value=st.session_state.get(f'{side_key}_pytds_port') or 1433, min_value=1, max_value=65535, step=1)
        sql2 = st.text_area(f"{label} (pytds): Requête SQL", value=st.session_state.get(f'{side_key}_pytds_sql') or '', height=150)
        as_str_default2 = st.session_state.get(f'{side_key}_pytds_as_string', True)
        st.checkbox(f"{label} (pytds): Forcer toutes les colonnes en string", value=as_str_default2, key=f'{side_key}_pytds_as_string')
        # Save inputs (non-widget keys only)
        st.session_state[f'{side_key}_pytds_host'] = host
        st.session_state[f'{side_key}_pytds_db'] = db
        st.session_state[f'{side_key}_pytds_user'] = user
        st.session_state[f'{side_key}_pytds_password'] = pw
        st.session_state[f'{side_key}_pytds_port'] = port
        st.session_state[f'{side_key}_pytds_sql'] = sql2
        if host and db and sql2 and st.button(f"Exécuter {label} (pytds)", key=f'{side_key}_exec_pytds'):
            with st.spinner('Chargement depuis SQL Server (pytds)...'):
                try:
                    df = load_from_sqlserver_pytds(host, db, user=user or None, password=pw or None, port=int(port), query=sql2, as_string=st.session_state[f'{side_key}_pytds_as_string'])
                    st.session_state[f'{side_key}_df'] = df
                    st.success(f"{label}: {len(df)} lignes chargées (pytds)")
                    st.dataframe(df.head(20))
                except Exception as e:
                    st.error(f"{label}: erreur pytds – {e}")
        else:
            df_prev = st.session_state.get(f'{side_key}_df')
            if isinstance(df_prev, pd.DataFrame):
                st.info(f"{label}: {len(df_prev)} lignes en mémoire (pytds)")
                st.dataframe(df_prev.head(20))

    else:  # 'SQL Server (mssql-python)'
        host = st.text_input(f"{label} (mssql-python): Hôte/IP", value=st.session_state.get(f'{side_key}_mpy_host') or '')
        db = st.text_input(f"{label} (mssql-python): Base", value=st.session_state.get(f'{side_key}_mpy_db') or '')
        user = st.text_input(f"{label} (mssql-python): Utilisateur", value=st.session_state.get(f'{side_key}_mpy_user') or '')
        pw = st.text_input(f"{label} (mssql-python): Mot de passe", value=st.session_state.get(f'{side_key}_mpy_password') or '', type='password')
        port = st.number_input(f"{label} (mssql-python): Port", value=st.session_state.get(f'{side_key}_mpy_port') or 1433, min_value=1, max_value=65535, step=1)
        sql3 = st.text_area(f"{label} (mssql-python): Requête SQL", value=st.session_state.get(f'{side_key}_mpy_sql') or '', height=150)
        as_str_default3 = st.session_state.get(f'{side_key}_mpy_as_string', True)
        st.checkbox(f"{label} (mssql-python): Forcer toutes les colonnes en string", value=as_str_default3, key=f'{side_key}_mpy_as_string')
        # Save inputs
        st.session_state[f'{side_key}_mpy_host'] = host
        st.session_state[f'{side_key}_mpy_db'] = db
        st.session_state[f'{side_key}_mpy_user'] = user
        st.session_state[f'{side_key}_mpy_password'] = pw
        st.session_state[f'{side_key}_mpy_port'] = port
        st.session_state[f'{side_key}_mpy_sql'] = sql3
        if host and db and sql3 and st.button(f"Exécuter {label} (mssql-python)", key=f'{side_key}_exec_mpy'):
            with st.spinner('Chargement depuis SQL Server (mssql-python)...'):
                try:
                    df = load_from_sqlserver_mssqlpy(host, db, user=user or None, password=pw or None, port=int(port), query=sql3, as_string=st.session_state[f'{side_key}_mpy_as_string'])
                    st.session_state[f'{side_key}_df'] = df
                    st.success(f"{label}: {len(df)} lignes chargées (mssql-python)")
                    st.dataframe(df.head(20))
                except Exception as e:
                    st.error(f"{label}: erreur mssql-python – {e}")
        else:
            df_prev = st.session_state.get(f'{side_key}_df')
            if isinstance(df_prev, pd.DataFrame):
                st.info(f"{label}: {len(df_prev)} lignes en mémoire (mssql-python)")
                st.dataframe(df_prev.head(20))

colA, colB = st.columns(2)
with colA:
    load_ui('Source A', 'A')
with colB:
    load_ui('Source B', 'B')

st.divider()
keys_input = st.text_input('Colonnes clé (optionnel, séparées par des virgules)')
float_tol = st.number_input('Tolérance flottants', value=1e-9, min_value=0.0, format='%f')

dfA = st.session_state.get('A_df')
dfB = st.session_state.get('B_df')

if st.button('Comparer', type='primary'):
    if not isinstance(dfA, pd.DataFrame) or not isinstance(dfB, pd.DataFrame):
        st.error('Veuillez charger les deux sources (A et B) avant de lancer la comparaison.')
    else:
        keys = [k.strip() for k in keys_input.split(',') if k.strip()] if keys_input else None
        with st.spinner('Comparaison en cours...'):
            res = compare_dataframes(dfA, dfB, keys=keys, float_tol=float_tol)
        st.success('Comparaison terminée')
        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric('Lignes A', res['left_count'])
        with m2:
            st.metric('Lignes B', res['right_count'])
        with m3:
            st.metric('Volumétrie identique', '✅' if res['row_count_equal'] else '❌')
        st.metric('Colonnes identiques', '✅' if res['columns_equal'] else '❌')
        st.metric('Données identiques', '✅' if res['data_equal'] else '❌')
        diffs = res.get('differences', {})
        if 'only_in_left_sample' in diffs:
            st.subheader('Lignes uniquement dans A (échantillon)')
            df_only_left = pd.DataFrame(diffs['only_in_left_sample'])
            st.dataframe(df_only_left)
            st.download_button('Télécharger (CSV)', df_only_left.to_csv(index=False).encode('utf-8'), file_name='only_in_left_sample.csv')
        if 'only_in_right_sample' in diffs:
            st.subheader('Lignes uniquement dans B (échantillon)')
            df_only_right = pd.DataFrame(diffs['only_in_right_sample'])
            st.dataframe(df_only_right)
            st.download_button('Télécharger (CSV)', df_only_right.to_csv(index=False).encode('utf-8'), file_name='only_in_right_sample.csv')
        if 'mismatched_rows_sample' in diffs:
            st.subheader('Lignes divergentes (sur clés) – Échantillon')
            flat = []
            for item in diffs['mismatched_rows_sample']:
                row = {}
                for k, v in item.get('keys', {}).items():
                    row[f'key_{k}'] = v
                cols = item.get('columns', [])
                for c in cols:
                    row[f'left_{c}'] = item['left_values'].get(c)
                    row[f'right_{c}'] = item['right_values'].get(c)
                flat.append(row)
            df_mis = pd.DataFrame(flat)
            st.dataframe(df_mis)
            st.download_button('Télécharger (CSV)', df_mis.to_csv(index=False).encode('utf-8'), file_name='mismatched_rows_sample.csv')
