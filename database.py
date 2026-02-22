import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import pandas as pd
from datetime import datetime
from thefuzz import process

DB_URL = os.getenv("DATABASE_URL")

# Legenda baseada na sua planilha
LEGENDA_HORARIOS = {
    'Y': '07:00 as 22:11',
    'D': '7:01 às 7:00',
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
    cursor.execute('''CREATE TABLE IF NOT EXISTS sites (sigla TEXT PRIMARY KEY, nome_da_localidade TEXT, ddd TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS escala (
        id SERIAL PRIMARY KEY, ddd_aba TEXT, tecnico TEXT, contato_corp TEXT, 
        supervisor TEXT, cm TEXT, dia_mes TEXT, mes_ano TEXT, horario TEXT)''')
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
    cursor.execute("TRUNCATE TABLE escala")
    
    abas_alvo = [s for s in xl.sheet_names if any(d in s for d in ['12','14','15','16','17','18','19'])]
    all_data = []

    for aba in abas_alvo:
        df = xl.parse(aba).fillna('')
        
        # Acha a linha "Funcionários"
        idx = None
        for i, row in df.iterrows():
            if 'Funcionários' in [str(v).strip() for v in row.values]:
                idx = i
                break
        
        if idx is not None:
            df.columns = [str(c).strip() for c in df.iloc[idx]]
            df = df.iloc[idx+1:]
            
            # LÓGICA DE DIA FLEXÍVEL: Pega o que vem antes da barra (Ex: "23/2" -> "23")
            col_dias = {}
            for col in df.columns:
                # Transforma "23/2" ou "23/" em "23"
                dia_extraido = str(col).split('/')[0].strip()
                if dia_extraido.isdigit():
                    col_dias[col] = dia_extraido

            for _, row in df.iterrows():
                tec = str(row.get('Funcionários', '')).strip()
                if not tec or tec.lower() in ['nan', 'funcionários']: continue
                
                for col_original, dia_limpo in col_dias.items():
                    valor = str(row[col_original]).strip().upper()
                    # Salva se tiver escala e não for Folga (F)
                    if valor and valor not in ['F', 'NAN', '', 'C']:
                        all_data.append((
                            aba, tec, str(row.get('ContatoCorp.', '')), 
                            str(row.get('Supervisor', '')), str(row.get('CM', '')), 
                            dia_limpo, mes_ano, valor
                        ))
    
    if all_data:
        execute_values(cursor, "INSERT INTO escala (ddd_aba, tecnico, contato_corp, supervisor, cm, dia_mes, mes_ano, horario) VALUES %s", all_data)
    conn.commit()
    conn.close()

def query_data(user_text):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    hoje = datetime.now()
    dia_str = str(hoje.day) # Vai buscar "22", "23", etc conforme o dia atual
    
    cursor.execute("SELECT sigla FROM sites")
    siglas = [r['sigla'] for r in cursor.fetchall()]
    match = process.extractOne(user_text.upper(), siglas)[0] if process.extractOne(user_text.upper(), siglas)[1] > 80 else None

    if match:
        cursor.execute("SELECT * FROM sites WHERE sigla = %s", (match,))
        s = cursor.fetchone()
        
        cursor.execute("""
            SELECT * FROM escala 
            WHERE ddd_aba LIKE %s AND dia_mes = %s AND mes_ano = %s
        """, (f"%{s['ddd']}%", dia_str, hoje.strftime('%m-%Y')))
        
        plantoes = cursor.fetchall()
        conn.close()

        res = f"📡 <b>NetQuery Terminal</b><br><hr>📍 <b>{s['nome_da_localidade']} ({match})</b><br>📅 Dia: {hoje.strftime('%d/%m')}<br><br>"
        
        if plantoes:
            for p in plantoes:
                h = LEGENDA_HORARIOS.get(p['horario'], f"Escala {p['horario']}")
                res += f"👨‍🔧 {p['tecnico']} (<b>{h}</b>)<br>📞 {p['contato_corp']}<br>👤 Sup: {p['supervisor']}<br>🖥️ CM: {p['cm']}<hr>"
        else:
            res += f"⚠️ Nenhum plantonista ativo no DDD {s['ddd']} para o dia {dia_str}."
        return res
    
    conn.close()
    return "Sigla não encontrada."
