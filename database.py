import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import pandas as pd
from datetime import datetime
from thefuzz import process

DB_URL = os.getenv("DATABASE_URL")

# Legenda completa baseada na sua foto
LEGENDA_HORARIOS = {
    '1': '07:00 as 16:00', '2': '07:30 as 16:30', '3': '08:00 as 17:00',
    '4': '08:30 as 17:30', '5': '11:00 as 20:00', '6': '12:30 as 21:30',
    '7': '13:00 as 22:00', '8': '22:12 as 07:00', '9': '08:00 as 12:00 SABADO',
    '10': '08:00 as 17:00 SABADO', '11': '09:00 as 13:00 SABADO', 
    '12': '09:00 AS 18:00 SABADO', '13': '18:00 as 22:00 SABADO',
    '14': '07:42 as 18:00', '15': '10:00 as 19:00',
    'A': '7:01 ás 8:00', 'B': '7:01 ás 17:30', 'D': '7:01 ás 7:00',
    'E': '16:01 ás 22:11', 'G': '16:01 ás 7:00', 'H': '16:31 ás 22:11',
    'I': '16:31 ás 7:00', 'J': '17:00 ás 22:11', 'K': '17:00 ás 7:00',
    'M': '17:31 ás 22:11', 'N': '17:31 ás 7:00', 'O': '20:01 ás 22:11',
    'P': '20:01 ás 7:00', 'Q': '21:31 ás 22:11', 'R': '21:31 ás 7:11',
    'S': '22:01 ás 7:00', 'T': '18:01 ás 7:00', 'U': '17:00:00 ás 8:00',
    'V': '18:00 ÁS 8:00', 'X': '22:01 ás 8:00', 'Z': '21:31 ás 8:00',
    'W': '08:00 as 18:00', 'Y': '07:00 as 22:11', 'AA': '22:01 as 9:00',
    'AB': '08:01 as 08:00', 'AC': '12:01 ás 07:00', 'AD': '22:00 as 03:00'
}

def get_connection():
    return psycopg2.connect(DB_URL)

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sites (
            sigla TEXT PRIMARY KEY, nome_da_localidade TEXT, localidade TEXT,
            area TEXT, ddd TEXT, telefone TEXT, cx TEXT, tx TEXT, ie TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS escala (
            id SERIAL PRIMARY KEY, ddd_aba TEXT, tecnico TEXT, 
            contato_corp TEXT, supervisor TEXT, cm TEXT, 
            dia_mes INT, mes_ano TEXT, horario TEXT
        )
    ''')
    # Migrações essenciais
    cursor.execute("ALTER TABLE escala ADD COLUMN IF NOT EXISTS ddd_aba TEXT")
    cursor.execute("ALTER TABLE escala ADD COLUMN IF NOT EXISTS contato_corp TEXT")
    cursor.execute("ALTER TABLE escala ADD COLUMN IF NOT EXISTS supervisor TEXT")
    cursor.execute("ALTER TABLE escala ADD COLUMN IF NOT EXISTS cm TEXT")
    cursor.execute("ALTER TABLE escala ADD COLUMN IF NOT EXISTS horario TEXT")
    
    conn.commit()
    conn.close()

def process_excel_sites(file_path):
    xl = pd.ExcelFile(file_path)
    aba = 'padrao' if 'padrao' in xl.sheet_names else xl.sheet_names[0]
    df = xl.parse(aba).fillna('')
    sites_unicos = {}
    for _, row in df.iterrows():
        sigla = str(row.get('Sigla', '')).strip().upper()
        if not sigla: continue
        sites_unicos[sigla] = (
            sigla, str(row.get('NomeDaLocalidade','')), str(row.get('localidade','')),
            str(row.get('Area','')), str(row.get('DDD','')), str(row.get('Telefone','')),
            str(row.get('CX','')), str(row.get('TX','')), str(row.get('IE',''))
        )
    if sites_unicos:
        conn = get_connection()
        cursor = conn.cursor()
        execute_values(cursor, """
            INSERT INTO sites (sigla, nome_da_localidade, localidade, area, ddd, telefone, cx, tx, ie)
            VALUES %s ON CONFLICT (sigla) DO UPDATE SET nome_da_localidade=EXCLUDED.nome_da_localidade, ddd=EXCLUDED.ddd
        """, list(sites_unicos.values()))
        conn.commit()
        conn.close()

def process_excel_escala(file_path):
    xl = pd.ExcelFile(file_path, engine='openpyxl')
    mes_ano = datetime.now().strftime('%m-%Y')
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE escala")
    
    # Busca todas as abas que tem números de DDD no nome
    abas_alvo = [s for s in xl.sheet_names if any(d in s for d in ['12','14','15','16','17','18','19'])]
    all_data = []

    for aba in abas_alvo:
        df = xl.parse(aba).fillna('')
        
        # Achar a linha onde o cabeçalho "Funcionários" existe
        idx = None
        for i, row in df.iterrows():
            if 'Funcionários' in [str(v).strip() for v in row.values]:
                idx = i
                break
        
        if idx is not None:
            df.columns = [str(c).strip() for c in df.iloc[idx]]
            df = df.iloc[idx+1:]
        
        # Limpar nomes de colunas que são dias (remover .0 se existir)
        def clean_day(c):
            s = str(c).split('.')[0]
            return s if s.isdigit() else s
        
        df.columns = [clean_day(c) for c in df.columns]
        col_dias = [c for c in df.columns if c.isdigit()]
        
        for _, row in df.iterrows():
            tec = str(row.get('Funcionários', '')).strip()
            if not tec or tec.lower() in ['nan', 'funcionários', '']: continue
            
            contato = str(row.get('ContatoCorp.', '')).strip()
            supervisor = str(row.get('Supervisor', '')).strip()
            cm = str(row.get('CM', '')).strip()
            
            for dia in col_dias:
                valor = str(row[dia]).strip().upper()
                # Não salva células vazias ou NAN
                if valor and valor != 'NAN':
                    all_data.append((
                        aba, tec, contato, supervisor, cm, int(dia), mes_ano, valor
                    ))

    if all_data:
        execute_values(cursor, """
            INSERT INTO escala (ddd_aba, tecnico, contato_corp, supervisor, cm, dia_mes, mes_ano, horario)
            VALUES %s
        """, all_data)
        
    conn.commit()
    conn.close()

def query_data(user_text):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    hoje = datetime.now()
    dia_hoje = hoje.day
    mes_ano_hoje = hoje.strftime('%m-%Y')
    
    cursor.execute("SELECT sigla FROM sites")
    siglas = [r['sigla'] for r in cursor.fetchall()]
    
    words = user_text.upper().replace('?', '').split()
    match = next((w for w in words if w in siglas), None)
    if not match:
        res_f = process.extractOne(user_text.upper(), siglas)
        if res_f and res_f[1] >= 80: match = res_f[0]

    if match:
        cursor.execute("SELECT * FROM sites WHERE sigla = %s", (match,))
        s = cursor.fetchone()
        
        # BUSCA REFORÇADA: Busca por ddd simples ou nomes compostos na aba
        cursor.execute("""
            SELECT tecnico, contato_corp, supervisor, cm, horario 
            FROM escala 
            WHERE (ddd_aba ILIKE %s OR ddd_aba ILIKE %s)
            AND dia_mes = %s AND mes_ano = %s
            AND horario NOT IN ('F', 'C', 'L', 'FE', 'FF', '', 'NAN')
        """, (f"%{s['ddd']}%", f"%{s['area']}%", dia_hoje, mes_ano_hoje))
        
        plantoes = cursor.fetchall()
        conn.close()

        res_html = f"📡 <b>NetQuery Terminal</b><br><hr>📍 <b>{s['nome_da_localidade']} ({match})</b><br>"
        res_html += f"🏢 DDD: {s['ddd']} | Dia: {hoje.strftime('%d/%m')}<br><br>"
        
        if plantoes:
            for p in plantoes:
                h_extenso = LEGENDA_HORARIOS.get(p['horario'], f"Código: {p['horario']}")
                res_html += f"👨‍🔧 {p['tecnico']} (<b>{h_extenso}</b>)<br>"
                res_html += f"📞 <a href='tel:{p['contato_corp']}' style='color:#38bdf8'>{p['contato_corp']}</a><br>"
                res_html += f"👤 Sup: {p['supervisor']}<br>🖥️ CM: {p['cm']}<hr style='border:0; border-top:1px dashed #334155; margin:10px 0;'>"
        else:
            res_html += f"⚠️ <b>Atenção:</b> Nenhum plantonista ativo para o DDD {s['ddd']} no dia {dia_hoje}/{hoje.month}. Verifique se a escala foi carregada corretamente."
        return res_html
    
    conn.close()
    return "Sigla não encontrada. Ex: 'Quem atende IVA?'"
