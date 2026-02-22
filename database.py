import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import pandas as pd
from datetime import datetime
from thefuzz import process

DB_URL = os.getenv("DATABASE_URL")

# Legenda completa baseada na sua planilha
LEGENDA_HORARIOS = {
    'Y': '07:00 as 22:11', 'D': '7:01 às 7:00', '1': '07:00 as 16:00', 
    '2': '07:30 as 16:30', '3': '08:00 as 17:00', '4': '08:30 as 17:30', 
    '5': '11:00 as 20:00', '6': '12:30 as 21:30', '7': '13:00 as 22:00', 
    '8': '22:12 as 07:00', '14': '07:42 as 18:00', 'A': '7:01 ás 8:00', 
    'B': '7:01 ás 17:30', 'G': '16:01 ás 7:00', 'K': '17:00 ás 7:00'
}

def get_connection():
    return psycopg2.connect(DB_URL)

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS sites (
        sigla TEXT PRIMARY KEY, nome_da_localidade TEXT, ddd TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS escala (
        id SERIAL PRIMARY KEY, ddd_aba TEXT, tecnico TEXT, contato_corp TEXT, 
        supervisor TEXT, cm TEXT, dia_mes TEXT, mes_ano TEXT, horario TEXT)''')
    
    columns = ['ddd_aba', 'contato_corp', 'supervisor', 'cm', 'horario', 'dia_mes']
    for col in columns:
        cursor.execute(f"ALTER TABLE escala ADD COLUMN IF NOT EXISTS {col} TEXT")
    
    conn.commit()
    conn.close()

def process_excel_sites(file_path):
    xl = pd.ExcelFile(file_path)
    aba = 'padrao' if 'padrao' in xl.sheet_names else xl.sheet_names[0]
    df = xl.parse(aba).fillna('')
    
    header_idx = 0
    for i, row in df.iterrows():
        if any('Sigla' in str(v) for v in row.values):
            header_idx = i
            break
            
    df.columns = [str(c).strip() for c in df.iloc[header_idx]]
    df = df.iloc[header_idx + 1:]
    
    conn = get_connection()
    cursor = conn.cursor()
    for _, row in df.iterrows():
        sigla = str(row.get('Sigla', '')).strip().upper()
        if sigla and sigla not in ['NAN', 'NONE', 'SIGLA']:
            nome = str(row.get('NomeDaLocalidade', '')).replace('nan', '').strip()
            ddd = str(row.get('DDD', '')).replace('.0', '').replace('nan', '').strip()
            cursor.execute("""
                INSERT INTO sites (sigla, nome_da_localidade, ddd) 
                VALUES (%s, %s, %s) 
                ON CONFLICT (sigla) DO UPDATE SET ddd=EXCLUDED.ddd, nome_da_localidade=EXCLUDED.nome_da_localidade
            """, (sigla, nome, ddd))
    conn.commit()
    conn.close()

def process_excel_escala(file_path):
    xl = pd.ExcelFile(file_path, engine='openpyxl')
    mes_ano = datetime.now().strftime('%m-%Y')
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE escala")
    
    abas_alvo = [s for s in xl.sheet_names if any(d in s for d in ['12','14','15','16','17','18','19'])]
    
    # DICIONÁRIO ANTI-DUPLICATAS (Isso resolve o seu erro do Acacio)
    escala_limpa = {}

    for aba in abas_alvo:
        df = xl.parse(aba).fillna('')
        
        header_idx = None
        for i, row in df.iterrows():
            if any('Funcionários' in str(v).strip() for v in row.values):
                header_idx = i
                break
        
        if header_idx is None: continue
        
        header_row = df.iloc[header_idx].values
        df_dados = df.iloc[header_idx + 1:]
        
        tec_idx, contato_idx, sup_idx, cm_idx = -1, -1, -1, -1
        dias_idx_map = {}
        
        for i, val in enumerate(header_row):
            v_str = str(val).strip()
            if 'Funcionários' in v_str or 'Funcionarios' in v_str: tec_idx = i
            elif 'Contato' in v_str: contato_idx = i
            elif 'Superv' in v_str: sup_idx = i
            elif 'CM' == v_str.upper(): cm_idx = i
            else:
                dia_limpo = None
                if isinstance(val, (datetime, pd.Timestamp)):
                    dia_limpo = str(val.day)
                elif v_str.endswith('00:00:00'):
                    try: dia_limpo = str(pd.to_datetime(v_str).day)
                    except: pass
                else:
                    poss_dia = v_str.split('/')[0].split('.')[0].strip()
                    if poss_dia.isdigit() and 1 <= int(poss_dia) <= 31:
                        dia_limpo = str(int(poss_dia))
                
                if dia_limpo:
                    dias_idx_map[i] = dia_limpo

        for _, row in df_dados.iterrows():
            row_vals = row.values
            if tec_idx == -1 or len(row_vals) <= tec_idx: continue
            
            tec = str(row_vals[tec_idx]).strip()
            if not tec or tec.lower() in ['nan', 'none', '', 'funcionários']: continue
            
            contato = str(row_vals[contato_idx]).replace('.0', '').replace('nan', '').strip() if contato_idx != -1 and len(row_vals) > contato_idx else ''
            supervisor = str(row_vals[sup_idx]).replace('nan', '').strip() if sup_idx != -1 and len(row_vals) > sup_idx else ''
            cm = str(row_vals[cm_idx]).replace('nan', '').strip() if cm_idx != -1 and len(row_vals) > cm_idx else ''
            
            for d_idx, d_limpo in dias_idx_map.items():
                if len(row_vals) > d_idx:
                    plantao_val = str(row_vals[d_idx]).strip().upper()
                    if plantao_val and plantao_val not in ['F', 'NAN', 'NONE', 'NULL', '', 'C', 'L', 'FE', 'FF']:
                        
                        # A CHAVE ÚNICA: Se o Excel tiver 2 Acacios no mesmo dia, ele salva apenas 1 e não quebra o sistema.
                        chave_unica = f"{aba}_{tec}_{d_limpo}_{mes_ano}"
                        
                        escala_limpa[chave_unica] = (
                            str(aba).upper(), tec, contato, supervisor, cm, d_limpo, mes_ano, plantao_val
                        )

    # Converte o dicionário sem duplicatas em uma lista para enviar ao banco
    all_rows = list(escala_limpa.values())

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
    dia_atual = str(hoje.day)
    
    cursor.execute("SELECT sigla FROM sites")
    siglas = [r['sigla'] for r in cursor.fetchall()]
    
    match = process.extractOne(user_text.upper(), siglas)[0] if process.extractOne(user_text.upper(), siglas)[1] > 80 else None

    if match:
        cursor.execute("SELECT * FROM sites WHERE sigla = %s", (match,))
        site = cursor.fetchone()
        
        cursor.execute("""
            SELECT * FROM escala 
            WHERE ddd_aba LIKE %s AND dia_mes = %s AND mes_ano = %s
        """, (f"%{site['ddd']}%", dia_atual, hoje.strftime('%m-%Y')))
        
        plantoes = cursor.fetchall()
        conn.close()

        res = f"📡 <b>NetQuery Terminal</b><br><hr>📍 <b>{site['nome_da_localidade']} ({match})</b><br>"
        res += f"📅 Dia: {hoje.strftime('%d/%m')} | DDD: {site['ddd']}<br><br>"
        
        if plantoes:
            for p in plantoes:
                h = LEGENDA_HORARIOS.get(p['horario'], f"Escala {p['horario']}")
                res += f"👨‍🔧 {p['tecnico']} (<b>{h}</b>)<br>📞 {p['contato_corp']}<br>👤 Sup: {p['supervisor']}<br>🖥️ CM: {p['cm']}<hr style='border-top:1px dashed #334155;'>"
        else:
            res += f"⚠️ Nenhum técnico com plantão ativo (Y/D/1...) encontrado para o dia {dia_atual} no DDD {site['ddd']}."
        return res
    
    conn.close()
    return "Sigla não encontrada."
