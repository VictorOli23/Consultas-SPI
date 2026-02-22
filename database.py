import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import pandas as pd
from datetime import datetime
from thefuzz import process

DB_URL = os.getenv("DATABASE_URL")

# Legenda baseada na planilha fornecida
LEGENDA_HORARIOS = {
    'Y': '07:00 as 22:11', #
    'D': '7:01 ás 7:00',   #
    '1': '07:00 as 16:00', '2': '07:30 as 16:30', '3': '08:00 as 17:00',
    '4': '08:30 as 17:30', '5': '11:00 as 20:00', '6': '12:30 as 21:30',
    '7': '13:00 as 22:00', '8': '22:12 as 07:00', '14': '07:42 as 18:00',
    'A': '7:01 ás 8:00', 'G': '16:01 ás 7:00', 'K': '17:00 ás 7:00'
}

def get_connection():
    return psycopg2.connect(DB_URL)

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    # Tabela de Sites
    cursor.execute('''CREATE TABLE IF NOT EXISTS sites (
        sigla TEXT PRIMARY KEY, nome_da_localidade TEXT, ddd TEXT)''')
    
    # Tabela de Escala
    cursor.execute('''CREATE TABLE IF NOT EXISTS escala (
        id SERIAL PRIMARY KEY, ddd_aba TEXT, tecnico TEXT, contato_corp TEXT, 
        supervisor TEXT, cm TEXT, dia_mes TEXT, mes_ano TEXT, horario TEXT)''')
    
    # Migrações de segurança para colunas (Render/Postgres)
    columns = ['ddd_aba', 'contato_corp', 'supervisor', 'cm', 'horario', 'dia_mes']
    for col in columns:
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
            cursor.execute("""
                INSERT INTO sites (sigla, nome_da_localidade, ddd) 
                VALUES (%s, %s, %s) 
                ON CONFLICT (sigla) DO UPDATE SET ddd=EXCLUDED.ddd
            """, (sigla, str(row.get('NomeDaLocalidade')), str(row.get('DDD'))))
    conn.commit()
    conn.close()

def process_excel_escala(file_path):
    xl = pd.ExcelFile(file_path, engine='openpyxl')
    mes_ano = datetime.now().strftime('%m-%Y')
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE escala") # Limpa dados anteriores
    
    # Filtra abas que contém números de DDD
    abas_alvo = [s for s in xl.sheet_names if any(d in s for d in ['12','14','15','16','17','18','19'])]
    all_rows = []

    for aba in abas_alvo:
        # astype(str) resolve o erro 'float object has no attribute strip'
        df = xl.parse(aba).astype(str).replace('nan', '')
        
        # Localiza o cabeçalho correto
        header_idx = None
        for i, row in df.iterrows():
            if 'Funcionários' in [str(v).strip() for v in row.values]:
                header_idx = i
                break
        
        if header_idx is not None:
            df.columns = [str(c).strip() for c in df.iloc[header_idx]]
            df = df.iloc[header_idx + 1:]
            
            # Mapeia colunas de dias lidando com "22/2", "23/2", etc.
            col_dias = {}
            for col in df.columns:
                dia_limpo = str(col).split('/')[0].strip()
                if dia_limpo.isdigit():
                    col_dias[col] = dia_limpo

            for _, row in df.iterrows():
                tec = str(row.get('Funcionários', '')).strip()
                if not tec or tec.lower() in ['', 'funcionários']: continue
                
                contato = str(row.get('ContatoCorp.', '')).strip()
                supervisor = str(row.get('Supervisor', '')).strip()
                cm = str(row.get('CM', '')).strip()

                for col_orig, dia_limpo in col_dias.items():
                    val = str(row[col_orig]).strip().upper()
                    # Ignora Folgas (F) e células vazias
                    if val and val not in ['F', '', 'C', 'L', 'FE', 'FF']:
                        all_rows.append((
                            aba, tec, contato, supervisor, cm, 
                            dia_limpo, mes_ano, val
                        ))
    
    if all_rows:
        execute_values(cursor, """
            INSERT INTO escala (ddd_aba, tecnico, contato_corp, supervisor, cm, dia_mes, mes_ano, horario) 
            VALUES %s
        """, all_rows)
    
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
        
        # Busca no banco filtrando por DDD e pelo dia do mês limpo
        cursor.execute("""
            SELECT * FROM escala 
            WHERE ddd_aba LIKE %s AND dia_mes = %s AND mes_ano = %s
        """, (f"%{site['ddd']}%", dia_atual, hoje.strftime('%m-%Y')))
        
        plantoes = cursor.fetchall()
        conn.close()

        res_html = f"📡 <b>NetQuery Terminal</b><br><hr>📍 <b>{site['nome_da_localidade']} ({match})</b><br>"
        res_html += f"🏢 DDD: {site['ddd']} | Dia: {hoje.strftime('%d/%m')}<br><br>"
        
        if plantoes:
            for p in plantoes:
                # Converte os códigos Y, D, etc em texto legível
                horario_formatado = LEGENDA_HORARIOS.get(p['horario'], f"Escala: {p['horario']}")
                res_html += f"👨‍🔧 {p['tecnico']}<br>⏰ <b>{horario_formatado}</b><br>"
                res_html += f"📞 <a href='tel:{p['contato_corp']}' style='color:#38bdf8'>{p['contato_corp']}</a><br>"
                res_html += f"👤 Sup: {p['supervisor']}<br>🖥️ CM: {p['cm']}<hr style='border:0; border-top:1px dashed #334155; margin:10px 0;'>"
        else:
            res_html += f"⚠️ Nenhum plantonista ativo no DDD {site['ddd']} para hoje."
        return res_html
    
    conn.close()
    return "Sigla não encontrada."
