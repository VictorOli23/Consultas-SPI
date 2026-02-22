import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import pandas as pd
from datetime import datetime
from thefuzz import process

DB_URL = os.getenv("DATABASE_URL")

# Legenda completa para conversão
LEGENDA_HORARIOS = {
    'Y': '07:00 as 22:11', 'D': '7:01 às 7:00', '1': '07:00 as 16:00', 
    '2': '07:30 as 16:30', '3': '08:00 as 17:00', 'A': '7:01 ás 8:00', 
    'G': '16:01 ás 7:00', 'K': '17:00 ás 7:00'
}

def get_connection():
    return psycopg2.connect(DB_URL)

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS sites (sigla TEXT PRIMARY KEY, nome_da_localidade TEXT, ddd TEXT)")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS escala (
            id SERIAL PRIMARY KEY, ddd_aba TEXT, tecnico TEXT, contato_corp TEXT, 
            supervisor TEXT, cm TEXT, dia_mes TEXT, mes_ano TEXT, horario TEXT
        )
    """)
    # Força a existência das colunas
    for col in ['contato_corp', 'supervisor', 'cm', 'horario', 'dia_mes']:
        cursor.execute(f"ALTER TABLE escala ADD COLUMN IF NOT EXISTS {col} TEXT")
    conn.commit()
    conn.close()

def process_excel_sites(file_path):
    xl = pd.ExcelFile(file_path)
    aba = 'padrao' if 'padrao' in xl.sheet_names else xl.sheet_names[0]
    df = xl.parse(aba).fillna('')
    conn = get_connection()
    cursor = conn.cursor()
    for _, row in df.iterrows():
        sigla = str(row.get('Sigla', '')).strip().upper()
        if sigla:
            cursor.execute("INSERT INTO sites (sigla, nome_da_localidade, ddd) VALUES (%s, %s, %s) ON CONFLICT (sigla) DO UPDATE SET ddd=EXCLUDED.ddd",
                           (sigla, str(row.get('NomeDaLocalidade')), str(row.get('DDD'))))
    conn.commit()
    conn.close()

def process_excel_escala(file_path):
    xl = pd.ExcelFile(file_path, engine='openpyxl')
    mes_ano = datetime.now().strftime('%m-%Y')
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE escala") # Limpa tudo antes de subir
    
    abas_alvo = [s for s in xl.sheet_names if any(d in s for d in ['12','14','15','16','17','18','19'])]
    all_rows = []

    for aba in abas_alvo:
        # Lê a aba e converte tudo para string para evitar erros de tipo
        df = xl.parse(aba).astype(str).replace('nan', '')
        
        # Localiza a linha onde está escrito "Funcionários"
        header_idx = None
        for i, row in df.iterrows():
            if any('Funcionários' in str(v) for v in row.values):
                header_idx = i
                break
        
        if header_idx is not None:
            df.columns = [c.strip() for c in df.iloc[header_idx]]
            df = df.iloc[header_idx + 1:]
            
            # Mapeia colunas de dias lidando com o formato "22/2"
            col_dias = {}
            for col in df.columns:
                dia_limpo = col.split('/')[0].strip()
                if dia_limpo.isdigit():
                    col_dias[col] = dia_limpo

            for _, row in df.iterrows():
                tec = row.get('Funcionários', '').strip()
                if not tec or tec.lower() in ['', 'funcionários']: continue
                
                for col_orig, dia_limpo in col_dias.items():
                    val = row[col_orig].strip().upper()
                    # Salva apenas se for um código de plantão (Y, D, 1, 2...) e não Folga (F)
                    if val and val not in ['F', '', 'C', 'L', 'FE']:
                        all_rows.append((
                            aba, tec, row.get('ContatoCorp.', ''), 
                            row.get('Supervisor', ''), row.get('CM', ''), 
                            dia_limpo, mes_ano, val
                        ))
    
    if all_rows:
        execute_values(cursor, "INSERT INTO escala (ddd_aba, tecnico, contato_corp, supervisor, cm, dia_mes, mes_ano, horario) VALUES %s", all_rows)
    conn.commit()
    conn.close()

def query_data(user_text):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    hoje = datetime.now()
    dia_atual = str(hoje.day) # "22"
    
    cursor.execute("SELECT sigla FROM sites")
    siglas = [r['sigla'] for r in cursor.fetchall()]
    
    match = process.extractOne(user_text.upper(), siglas)[0] if process.extractOne(user_text.upper(), siglas)[1] > 80 else None

    if match:
        cursor.execute("SELECT * FROM sites WHERE sigla = %s", (match,))
        site = cursor.fetchone()
        
        # Busca por DDD na aba da escala e pelo dia limpo
        cursor.execute("""
            SELECT * FROM escala 
            WHERE ddd_aba LIKE %s AND dia_mes = %s AND mes_ano = %s
        """, (f"%{site['ddd']}%", dia_atual, hoje.strftime('%m-%Y')))
        
        plantoes = cursor.fetchall()
        conn.close()

        res = f"📡 <b>NetQuery Terminal</b><br><hr>📍 <b>{site['nome_da_localidade']} ({match})</b><br>📅 Dia: {hoje.strftime('%d/%m')}<br><br>"
        
        if plantoes:
            for p in plantoes:
                # Converte Y em "07:00 as 22:11" etc.
                h = LEGENDA_HORARIOS.get(p['horario'], f"Escala {p['horario']}")
                res += f"👨‍🔧 {p['tecnico']} (<b>{h}</b>)<br>📞 {p['contato_corp']}<br>👤 Sup: {p['supervisor']}<br>🖥️ CM: {p['cm']}<hr>"
        else:
            res += f"⚠️ Nenhum técnico ativo para o DDD {site['ddd']} hoje (Coluna {dia_atual} da escala)."
        return res
    
    conn.close()
    return "Sigla não encontrada."
