import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import pandas as pd
from datetime import datetime
from thefuzz import process

# Puxa a URL completa do Neon.tech das variáveis de ambiente
DB_URL = os.getenv("DATABASE_URL")

LEGENDA_HORARIOS = {
    '1': '07:00 as 16:00', '2': '07:30 as 16:30', '3': '08:00 as 17:00',
    '4': '08:30 as 17:30', '5': '11:00 as 20:00', '6': '12:30 as 21:30',
    '7': '13:00 as 22:00', '8': '22:12 as 07:00', '14': '07:42 as 18:00', '15': '10:00 as 19:00'
}

def get_connection():
    return psycopg2.connect(DB_URL)

def init_db():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS sites (
        sigla TEXT PRIMARY KEY, nome_da_localidade TEXT, ddd TEXT, cm_responsavel TEXT)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS escala (
        id SERIAL PRIMARY KEY, ddd_aba TEXT, tecnico TEXT, contato_corp TEXT, 
        supervisor TEXT, cm TEXT, segmento TEXT, dia_mes TEXT, mes_ano TEXT, horario TEXT)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS sugestoes (
        id SERIAL PRIMARY KEY, usuario TEXT, texto TEXT, data TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    conn.commit()
    conn.close()

def save_suggestion(usuario, texto):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO sugestoes (usuario, texto) VALUES (%s, %s)", (usuario, texto))

def get_suggestions():
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT usuario, texto, data FROM sugestoes ORDER BY data DESC")
            return cur.fetchall()

def query_data(user_text, data_consulta=None):
    hoje = datetime.now()
    # Se o frontend mandou "2/3", usamos o dia "2".
    dia_alvo = data_consulta.split('/')[0] if data_consulta else str(hoje.day)
    
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    cur.execute("SELECT sigla, nome_da_localidade, ddd, cm_responsavel FROM sites")
    sites = cur.fetchall()
    siglas = [s['sigla'] for s in sites]
    
    match_data = process.extractOne(user_text.upper(), siglas)
    if match_data and match_data[1] > 80:
        match = match_data[0]
        site = next(s for s in sites if s['sigla'] == match)
        cm_busca = site['cm_responsavel'] if site['cm_responsavel'] else match[:3]
        
        cur.execute("""
            SELECT * FROM escala 
            WHERE ddd_aba LIKE %s AND cm ILIKE %s AND dia_mes = %s
        """, (f"%{site['ddd']}%", f"%{cm_busca}%", dia_alvo))
        
        plantoes = cur.fetchall()
        conn.close()
        
        res = {
            "encontrado": True,
            "cabecalho": f"📍 <b>{site['nome_da_localidade']} ({match})</b><br>📅 Data: {dia_alvo}/{hoje.month}",
            "infra": [], "tx": []
        }
        for p in plantoes:
            h = LEGENDA_HORARIOS.get(p['horario'], f"Escala {p['horario']}")
            info = f"👨‍🔧 <b>{p['tecnico']}</b><br>⏰ {h}<br>📞 {p['contato_corp']}<hr>"
            if 'INFRA' in p['segmento'].upper(): res["infra"].append(info)
            else: res["tx"].append(info)
        return res
    return {"encontrado": False, "erro": "Sigla não encontrada."}

def process_excel_sites(file_path):
    df = pd.read_excel(file_path).fillna('')
    df.columns = [str(c).strip().upper().replace(' ', '') for c in df.columns]
    # ... lógica de mapeamento de colunas ...
    conn = get_connection()
    cursor = conn.cursor()
    # Execute o execute_values para salvar no banco
    conn.commit()
    conn.close()

def process_excel_escala(file_path):
    # ... lógica de leitura das abas de dias ...
    pass

def get_db_stats():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM sites")
        s = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM escala")
        e = cur.fetchone()[0]
        return {"sites": s, "escala": e}
