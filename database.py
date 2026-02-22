import sqlite3
import pandas as pd
from thefuzz import process

DB_NAME = "consultas.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sites (
            sigla TEXT PRIMARY KEY,
            localidade TEXT,
            nome_da_localidade TEXT,
            area TEXT,
            ddd TEXT,
            telefone TEXT,
            cx TEXT,
            tx TEXT,
            ie TEXT
        )
    ''')
    conn.commit()
    conn.close()

def process_excel(file_path):
    df = pd.read_excel(file_path)
    df = df.fillna('') # Limpa dados nulos
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    for index, row in df.iterrows():
        # Busca pelas colunas (pode ser ajustado se o Excel tiver pequenas variações de maiúsculas)
        sigla = str(row.get('Sigla', '')).strip().upper()
        if not sigla:
            continue

        cursor.execute('''
            INSERT INTO sites (sigla, localidade, nome_da_localidade, area, ddd, telefone, cx, tx, ie)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sigla) DO UPDATE SET
                localidade=excluded.localidade,
                nome_da_localidade=excluded.nome_da_localidade,
                area=excluded.area,
                ddd=excluded.ddd,
                telefone=excluded.telefone,
                cx=excluded.cx,
                tx=excluded.tx,
                ie=excluded.ie
        ''', (
            sigla,
            str(row.get('localidade', '')),
            str(row.get('NomeDaLocalidade', '')),
            str(row.get('Area', '')),
            str(row.get('DDD', '')),
            str(row.get('Telefone', '')),
            str(row.get('CX', '')),
            str(row.get('TX', '')),
            str(row.get('IE', ''))
        ))
    conn.commit()
    conn.close()

def query_data(user_text):
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM sites")
    all_rows = cursor.fetchall()
    conn.close()

    if not all_rows:
        return "O banco de dados está vazio. Peça ao administrador para importar os dados no Painel Admin."

    # Normalizar texto da busca
    words = user_text.upper().replace('?', '').replace(',', '').split()
    siglas = {row['sigla']: row for row in all_rows}
    cidades = {row['nome_da_localidade'].upper(): row for row in all_rows if row['nome_da_localidade']}

    # 1. Busca Exata por Sigla
    for word in words:
        if word in siglas:
            return format_response(siglas[word])

    # 2. Busca Exata por Cidade (pode ter nomes compostos)
    user_text_upper = user_text.upper()
    matched_cities = []
    for cidade, row in cidades.items():
        if cidade in user_text_upper:
            matched_cities.append(row)

    if matched_cities:
        if len(matched_cities) == 1:
            return format_response(matched_cities[0])
        else:
            return "Encontrei várias siglas para essa cidade:<br><br>" + "<br>".join([f"• <b>{m['sigla']}</b> - {m['nome_da_localidade']}" for m in matched_cities])

    # 3. Busca Aproximada (Fuzzy) por Sigla
    sigla_list = list(siglas.keys())
    best_match = None
    highest_score = 0
    
    for word in words:
        if len(word) < 3: continue # Ignora preposições curtas
        match, score = process.extractOne(word, sigla_list)
        if score > highest_score:
            highest_score = score
            best_match = match

    if highest_score >= 70:
        return f"Não encontrei exatamente, mas você quis dizer <b>{best_match}</b>?<br><br>" + format_response(siglas[best_match])

    return "Desculpe, não consegui identificar a sigla ou cidade na sua pergunta. Pode tentar de outra forma?"

def format_response(row):
    return f"""
    <b>Sigla:</b> {row['sigla']}<br>
    <b>Cidade:</b> {row['nome_da_localidade']} ({row['localidade']})<br>
    <b>Área:</b> {row['area']}<br>
    <b>Contato:</b> ({row['ddd']}) {row['telefone']}<br>
    <b>IE / TX / CX:</b> {row['ie']} / {row['tx']} / {row['cx']}
    """