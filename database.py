import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import pandas as pd
from datetime import datetime
from thefuzz import process

DB_URL = os.getenv("DATABASE_URL")

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
    'V': '18:00 ÁS 8:00', 'W': '08:00 as 18:00', 'X': '22:01 ás 8:00', 
    'Y': '07:00 as 22:11', 'Z': '21:31 ás 8:00', 'AA': '22:01 as 9:00',
    'AB': '08:01 as 08:00', 'AC': '12:01 ás 07:00', 'AD': '22:00 as 03:00'
}

def get_connection():
    return psycopg2.connect(DB_URL)

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS sites (sigla TEXT PRIMARY KEY, nome_da_localidade TEXT, ddd TEXT, cm_responsavel TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS escala (id SERIAL PRIMARY KEY, ddd_aba TEXT, tecnico TEXT, contato_corp TEXT, supervisor TEXT, cm TEXT, segmento TEXT, dia_mes TEXT, mes_ano TEXT, horario TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS sugestoes (id SERIAL PRIMARY KEY, usuario TEXT, texto TEXT, data TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS historico (id SERIAL PRIMARY KEY, sigla TEXT, status TEXT, data TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    conn.commit()
    conn.close()

# --- GRAVAÇÃO DO HISTÓRICO ONLINE ---
def save_historico(sigla, status):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO historico (sigla, status) VALUES (%s, %s)", (sigla, status))
    conn.commit()
    conn.close()

def get_historico():
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT sigla, status, data FROM historico ORDER BY data DESC LIMIT 10")
    rows = cursor.fetchall()
    conn.close()
    return [{"sigla": r['sigla'], "status": r['status'], "tempo": r['data'].strftime('%H:%M')} for r in rows]

def save_suggestion(usuario, texto):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO sugestoes (usuario, texto) VALUES (%s, %s)", (usuario, texto))
    conn.commit()
    conn.close()

def get_suggestions():
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT usuario, texto, data FROM sugestoes ORDER BY data DESC")
    rows = cursor.fetchall()
    conn.close()
    return [{"usuario": r['usuario'], "texto": r['texto'], "data": r['data'].strftime('%d/%m/%Y %H:%M')} for r in rows]

# --- PROCESSAMENTO EXCEL (MANTIDO) ---
def process_excel_sites(file_path):
    df = pd.read_excel(file_path).fillna('')
    df.columns = [str(c).strip().upper().replace(' ', '') for c in df.columns]
    col_sigla = next((c for c in df.columns if 'SIGLA' in c), None)
    col_nome = next((c for c in df.columns if 'NOME' in c or 'LOCAL' in c), None)
    col_ddd = next((c for c in df.columns if 'DDD' in c), None)
    col_cm = next((c for c in df.columns if 'CX' in c or 'TX' in c or 'CM' in c), None)

    conn = get_connection()
    cursor = conn.cursor()
    dados_dict = {}
    for _, row in df.iterrows():
        sigla = str(row.get(col_sigla, '')).strip().upper()
        if sigla and sigla not in ['NAN', 'NONE', 'SIGLA', '']:
            nome = str(row.get(col_nome, '')).replace('nan', '').strip()
            if nome.endswith('.0'): nome = nome[:-2]
            ddd = str(row.get(col_ddd, '')).replace('.0', '').replace('nan', '').strip()
            cm = str(row.get(col_cm, '')).replace('nan', '').strip().upper()
            dados_dict[sigla] = (sigla, nome, ddd, cm)

    dados_insercao = list(dados_dict.values())
    if dados_insercao:
        execute_values(cursor, """
            INSERT INTO sites (sigla, nome_da_localidade, ddd, cm_responsavel) VALUES %s 
            ON CONFLICT (sigla) DO UPDATE SET nome_da_localidade=EXCLUDED.nome_da_localidade, ddd=EXCLUDED.ddd, cm_responsavel=EXCLUDED.cm_responsavel
        """, dados_insercao)
    conn.commit()
    conn.close()

def process_excel_escala(file_path):
    xl = pd.ExcelFile(file_path, engine='openpyxl')
    mes_ano = datetime.now().strftime('%m-%Y')
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM escala")
    
    abas_alvo = [s for s in xl.sheet_names if any(char.isdigit() for char in s)]
    chaves_vistas = set()
    all_rows = []

    for aba in abas_alvo:
        df = xl.parse(aba, dtype=str).fillna('')
        header_row = []
        df_dados = df
        
        if any('FUNCION' in str(c).strip().upper() for c in df.columns):
            header_row = df.columns
        else:
            for i, row in df.iterrows():
                if any('FUNCION' in str(v).strip().upper() for v in row.values):
                    header_row = row.values
                    df_dados = df.iloc[i + 1:]
                    break
                    
        if len(header_row) == 0: continue
        
        tec_idx, contato_idx, sup_idx, cm_idx, seg_idx = -1, -1, -1, -1, -1
        dias_idx_map = {}
        
        for i, val in enumerate(header_row):
            v_str = str(val).strip().upper()
            if 'FUNCION' in v_str: tec_idx = i
            elif 'CONTATO' in v_str: contato_idx = i
            elif 'SUPERV' in v_str: sup_idx = i
            elif 'CM' == v_str: cm_idx = i
            elif 'SEGMENTO' in v_str: seg_idx = i
            else:
                dia_limpo = None
                if isinstance(val, (datetime, pd.Timestamp)): dia_limpo = str(val.day)
                elif v_str.endswith('00:00:00'):
                    try: dia_limpo = str(pd.to_datetime(v_str).day)
                    except: pass
                else:
                    poss_dia = v_str.split('/')[0].split('.')[0].strip()
                    if poss_dia.isdigit() and 1 <= int(poss_dia) <= 31:
                        dia_limpo = str(int(poss_dia))
                if dia_limpo: dias_idx_map[i] = dia_limpo

        for _, row in df_dados.iterrows():
            row_vals = row.values
            if tec_idx == -1 or len(row_vals) <= tec_idx: continue
            
            tec = str(row_vals[tec_idx]).strip()
            if not tec or tec.upper() in ['NAN', 'NONE', '', 'FUNCIONÁRIOS', 'FUNCIONARIOS']: continue
            
            contato = str(row_vals[contato_idx]).replace('.0', '').replace('nan', '').strip() if contato_idx != -1 and len(row_vals) > contato_idx else ''
            supervisor = str(row_vals[sup_idx]).replace('nan', '').strip() if sup_idx != -1 and len(row_vals) > sup_idx else ''
            cm = str(row_vals[cm_idx]).replace('nan', '').strip().upper() if cm_idx != -1 and len(row_vals) > cm_idx else ''
            segmento = str(row_vals[seg_idx]).replace('nan', '').strip() if seg_idx != -1 and len(row_vals) > seg_idx else 'Não especificado'
            
            for d_idx, d_limpo in dias_idx_map.items():
                if len(row_vals) > d_idx:
                    plantao_val = str(row_vals[d_idx]).strip().upper()
                    if plantao_val and plantao_val not in ['F', 'NAN', 'NONE', 'NULL', '', 'C', 'L', 'FE', 'FF']:
                        chave_unica = f"{aba}_{tec}_{d_limpo}_{mes_ano}"
                        if chave_unica not in chaves_vistas:
                            chaves_vistas.add(chave_unica)
                            all_rows.append((str(aba).upper(), tec, contato, supervisor, cm, segmento, d_limpo, mes_ano, plantao_val))

    if all_rows:
        execute_values(cursor, "INSERT INTO escala (ddd_aba, tecnico, contato_corp, supervisor, cm, segmento, dia_mes, mes_ano, horario) VALUES %s", all_rows)
    conn.commit()
    conn.close()

# --- BUSCA INTELIGENTE ---
def query_data(user_text, data_consulta=None):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    hoje = datetime.now()
    
    dia_alvo = data_consulta.split('/')[0] if data_consulta else str(hoje.day)
    mes_alvo = data_consulta.split('/')[1] if data_consulta and '/' in data_consulta else str(hoje.month)
    
    cursor.execute("SELECT sigla, nome_da_localidade, ddd, cm_responsavel FROM sites")
    sites_db = cursor.fetchall()
    siglas = [r['sigla'] for r in sites_db]
    
    match_data = process.extractOne(user_text.upper(), siglas)
    match = match_data[0] if match_data and match_data[1] > 80 else None

    if match:
        site = next((s for s in sites_db if s['sigla'] == match), None)
        cm_banco = site.get('cm_responsavel', '').strip()
        cm_busca = cm_banco if cm_banco and cm_banco != 'NAN' else match[:3]
        
        cursor.execute("SELECT * FROM escala WHERE ddd_aba LIKE %s AND cm ILIKE %s AND dia_mes = %s", (f"%{site['ddd']}%", f"%{cm_busca}%", dia_alvo))
        plantoes = cursor.fetchall()
        conn.close()

        resposta = {
            "encontrado": True,
            "cabecalho": f"📍 <b>{site['nome_da_localidade']} ({match})</b><br>📅 Data de Busca: {dia_alvo}/{mes_alvo} | DDD: {site['ddd']} | Base: {cm_busca}",
            "infra": [], "tx": []
        }
        
        if plantoes:
            save_historico(match, "Plantonista localizado") # Grava no histórico lateral
            for p in plantoes:
                h_fmt = LEGENDA_HORARIOS.get(p['horario'], f"Escala {p['horario']}")
                tec_info = f"<div style='margin-bottom: 10px;'><b style='color:white;'>👨‍🔧 {p['tecnico']}</b><br>⏰ {h_fmt}<br>"
                if p['segmento'] and p['segmento'] != 'Não especificado': tec_info += f"⚙️ {p['segmento']}<br>"
                tec_info += f"📞 <a href='tel:{p['contato_corp']}' style='color:#38bdf8; text-decoration:none;'>{p['contato_corp']}</a><br>👤 Sup: {p['supervisor']}</div><hr style='border-top:1px dashed var(--border); margin:8px 0;'>"
                
                if 'INFRA' in p['segmento'].upper(): resposta["infra"].append(tec_info)
                else: resposta["tx"].append(tec_info)
        else:
             save_historico(match, "Sem cobertura") # Grava no histórico lateral
             resposta["erro"] = f"⚠️ Nenhum técnico exclusivo da base <b>{cm_busca}</b> de plantão hoje."
             
        return resposta
    
    conn.close()
    save_historico(user_text.upper()[:10], "Não encontrada") # Limita a 10 char pra nao zoar o banco
    return {"encontrado": False, "erro": "Sigla não encontrada no banco de dados."}
