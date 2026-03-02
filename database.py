import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import pandas as pd
from datetime import datetime, timedelta
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
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS sites (sigla TEXT PRIMARY KEY, nome_da_localidade TEXT, ddd TEXT, cm_responsavel TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS escala (id SERIAL PRIMARY KEY, ddd_aba TEXT, tecnico TEXT, contato_corp TEXT, supervisor TEXT, cm TEXT, segmento TEXT, dia_mes TEXT, mes_ano TEXT, horario TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS sugestoes (id SERIAL PRIMARY KEY, usuario TEXT, texto TEXT, data TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS historico (id SERIAL PRIMARY KEY, usuario TEXT, sigla TEXT, status TEXT, data TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS usuarios_online (nome TEXT PRIMARY KEY, ultima_atividade TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    try: cursor.execute("ALTER TABLE historico ADD COLUMN IF NOT EXISTS usuario TEXT DEFAULT 'Anônimo'")
    except: pass
    conn.close()

# --- NOVO: COLETA DADOS PARA O AUTOCOMPLETE ---
def get_autocomplete_data():
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Pega os Sites
    cursor.execute("SELECT sigla, nome_da_localidade FROM sites")
    sites = cursor.fetchall()
    
    # Pega as Bases (CM)
    cursor.execute("SELECT DISTINCT cm FROM escala WHERE cm != ''")
    bases = cursor.fetchall()
    
    conn.close()
    
    resultado = []
    for s in sites:
        resultado.append({"termo": s['sigla'], "detalhe": s['nome_da_localidade'], "tipo": "📍 Site"})
    for b in bases:
        if b['cm']: resultado.append({"termo": b['cm'], "detalhe": "Região Inteira", "tipo": "🗺️ Base"})
        
    return resultado

def get_all_tecnicos():
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT DISTINCT tecnico, contato_corp FROM escala WHERE tecnico != '' ORDER BY tecnico ASC")
    rows = cursor.fetchall()
    conn.close()
    return [{"nome": r['tecnico'], "contato": r['contato_corp']} for r in rows]

def ping_user(nome):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO usuarios_online (nome, ultima_atividade) VALUES (%s, CURRENT_TIMESTAMP) ON CONFLICT (nome) DO UPDATE SET ultima_atividade = CURRENT_TIMESTAMP", (nome,))
    conn.commit()
    conn.close()

def get_online_users():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT nome FROM usuarios_online WHERE ultima_atividade >= NOW() - INTERVAL '2 minutes'")
    rows = cursor.fetchall()
    conn.close()
    return [r[0] for r in rows]

def save_historico(usuario, sigla, status):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO historico (usuario, sigla, status) VALUES (%s, %s, %s)", (usuario, sigla, status))
    conn.commit()
    conn.close()

def get_historico():
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT usuario, sigla, status, data FROM historico ORDER BY data DESC LIMIT 15")
    rows = cursor.fetchall()
    conn.close()
    resultados = []
    for r in rows:
        hora_br = r['data'] - timedelta(hours=3)
        resultados.append({"usuario": r['usuario'], "sigla": r['sigla'], "status": r['status'], "tempo": hora_br.strftime('%H:%M')})
    return resultados

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
    resultados = []
    for r in rows:
        hora_br = r['data'] - timedelta(hours=3)
        resultados.append({"usuario": r['usuario'], "texto": r['texto'], "data": hora_br.strftime('%d/%m/%Y %H:%M')})
    return resultados

def process_excel_sites(file_path):
    xl = pd.ExcelFile(file_path)
    conn = get_connection()
    cursor = conn.cursor()
    dados_dict = {}

    for sheet in xl.sheet_names:
        df = xl.parse(sheet, dtype=str).fillna('')
        header_idx = -1
        for i, row in df.iterrows():
            row_str = " ".join([str(v).upper() for v in row.values])
            if 'SIGLA' in row_str:
                header_idx = i; break
                
        if header_idx != -1:
            df.columns = [str(c).strip().upper().replace(' ', '').replace('Í', 'I').replace('Ó', 'O') for c in df.iloc[header_idx]]
            df = df.iloc[header_idx + 1:]
        else:
            df.columns = [str(c).strip().upper().replace(' ', '').replace('Í', 'I').replace('Ó', 'O') for c in df.columns]

        if 'SIGLA' not in df.columns: continue
        
        col_sigla = 'SIGLA'
        col_nome = 'NOMEDALOCALIDADE' if 'NOMEDALOCALIDADE' in df.columns else next((c for c in df.columns if 'MUNIC' in c or 'CIDAD' in c or 'LOCAL' in c or 'NOME' in c), None)
        col_ddd = next((c for c in df.columns if 'DDD' in c), None)
        col_cm = next((c for c in df.columns if c in ['CM', 'CX', 'TX', 'CMRESPONSAVEL', 'BASE']), None)

        for _, row in df.iterrows():
            sigla = str(row.get(col_sigla, '')).strip().upper()
            if sigla and sigla not in ['NAN', 'NONE', 'SIGLA', '']:
                nome = str(row.get(col_nome, '')).replace('nan', '').strip() if col_nome else ''
                if nome.endswith('.0'): nome = nome[:-2]
                ddd = str(row.get(col_ddd, '')).replace('.0', '').replace('nan', '').strip() if col_ddd else ''
                cm = str(row.get(col_cm, '')).replace('nan', '').strip().upper() if col_cm else ''
                if len(sigla) <= 10: dados_dict[sigla] = (sigla, nome, ddd, cm)

    dados_insercao = list(dados_dict.values())
    if dados_insercao:
        execute_values(cursor, "INSERT INTO sites (sigla, nome_da_localidade, ddd, cm_responsavel) VALUES %s ON CONFLICT (sigla) DO UPDATE SET nome_da_localidade=EXCLUDED.nome_da_localidade, ddd=EXCLUDED.ddd, cm_responsavel=EXCLUDED.cm_responsavel", dados_insercao)
    conn.commit()
    conn.close()

def process_excel_escala(file_path):
    xl = pd.ExcelFile(file_path, engine='openpyxl')
    hoje_br = datetime.now() - timedelta(hours=3)
    mes_ano = hoje_br.strftime('%m-%Y')
    
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM escala")
    
    abas_alvo = xl.sheet_names
    chaves_vistas = set()
    all_rows = []

    for aba in abas_alvo:
        if aba.strip().upper() in ['LEGENDA', 'INSTRUÇÕES', 'RESUMO', 'MENU']: continue
        df = xl.parse(aba, dtype=str).fillna('')
        header_row = []
        df_dados = df
        
        if any('FUNCION' in str(c).strip().upper() for c in df.columns): header_row = df.columns
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
            elif v_str in ['CM', 'BASE', 'AREA', 'ÁREA'] or v_str == 'CM_RESPONSAVEL': cm_idx = i
            elif 'SEGMENTO' in v_str: seg_idx = i
            else:
                dia_limpo = None
                if isinstance(val, (datetime, pd.Timestamp)): dia_limpo = str(val.day)
                elif v_str.endswith('00:00:00'):
                    try: dia_limpo = str(pd.to_datetime(v_str).day)
                    except: pass
                else:
                    poss_dia = v_str.split('/')[0].split('.')[0].strip()
                    if poss_dia.isdigit() and 1 <= int(poss_dia) <= 31: dia_limpo = str(int(poss_dia))
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

    if all_rows: execute_values(cursor, "INSERT INTO escala (ddd_aba, tecnico, contato_corp, supervisor, cm, segmento, dia_mes, mes_ano, horario) VALUES %s", all_rows)
    conn.commit()
    conn.close()

def query_data(user_text, data_consulta=None, nome_usuario="Anônimo"):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    hoje = datetime.now() - timedelta(hours=3)
    dia_alvo = data_consulta.split('/')[0] if data_consulta else str(hoje.day)
    mes_alvo = data_consulta.split('/')[1] if data_consulta and '/' in data_consulta else str(hoje.month)
    termo = user_text.strip().upper()

    cursor.execute("SELECT DISTINCT ddd_aba FROM escala WHERE ddd_aba ILIKE %s", (f"%{termo}%",))
    abas_encontradas = [r['ddd_aba'] for r in cursor.fetchall()]
    
    if abas_encontradas and "CAS" in termo:
        cursor.execute("SELECT * FROM escala WHERE ddd_aba IN %s AND dia_mes = %s", (tuple(abas_encontradas), dia_alvo))
        plantoes = cursor.fetchall()
        if plantoes:
            resposta = {"encontrado": True, "cabecalho": f"📍 <b>Planilha(s): {', '.join(abas_encontradas)}</b><br>📅 Data: {dia_alvo}/{mes_alvo} | Todos os plantonistas desta aba", "infra": [], "tx": []}
            save_historico(nome_usuario, termo, "Localizado (Planilha)")
            for p in plantoes:
                h_fmt = LEGENDA_HORARIOS.get(p['horario'], f"Escala {p['horario']}")
                tec_safe = str(p['tecnico']).replace("'", "").replace('"', '')
                contato_safe = str(p['contato_corp']).replace("'", "").replace('"', '')
                nome_base_visual = f" <span style='font-size:0.75rem; color:var(--warning);'>({p['cm']})</span>" if p['cm'] else ""
                tec_info = f"<div style='margin-bottom: 10px;'><span style='color:var(--primary); cursor:pointer; font-weight:bold; text-decoration:underline;' onclick=\"abrirMascarasComTecnico('{tec_safe}', '{contato_safe}')\" title='Criar Máscara'>👨‍🔧 {p['tecnico']}{nome_base_visual} <i class='fa-solid fa-share-from-square' style='font-size:0.85em;'></i></span><br>⏰ {h_fmt}<br>"
                if p['segmento'] and p['segmento'] != 'Não especificado': tec_info += f"⚙️ {p['segmento']}<br>"
                tec_info += f"📞 <a href='tel:{p['contato_corp']}' style='color:#38bdf8; text-decoration:none;'>{p['contato_corp']}</a><br>👤 Sup: {p['supervisor']}</div><hr style='border-top:1px dashed var(--border); margin:8px 0;'>"
                if 'INFRA' in p['segmento'].upper(): resposta["infra"].append(tec_info)
                else: resposta["tx"].append(tec_info)
            conn.close()
            return resposta

    cursor.execute("SELECT DISTINCT cm FROM escala WHERE cm != ''")
    bases_db = [r['cm'] for r in cursor.fetchall()]
    match_base = process.extractOne(termo, bases_db)
    if match_base and match_base[1] >= 85: 
        cm_busca = match_base[0]
        cursor.execute("SELECT * FROM escala WHERE cm = %s AND dia_mes = %s", (cm_busca, dia_alvo))
        plantoes = cursor.fetchall()
        if plantoes:
            resposta = {"encontrado": True, "cabecalho": f"📍 <b>Região / Base: {cm_busca}</b><br>📅 Data: {dia_alvo}/{mes_alvo} | Todos os plantonistas da região", "infra": [], "tx": []}
            save_historico(nome_usuario, cm_busca, "Localizado (Região)")
            for p in plantoes:
                h_fmt = LEGENDA_HORARIOS.get(p['horario'], f"Escala {p['horario']}")
                tec_safe = str(p['tecnico']).replace("'", "").replace('"', '')
                contato_safe = str(p['contato_corp']).replace("'", "").replace('"', '')
                tec_info = f"<div style='margin-bottom: 10px;'><span style='color:var(--primary); cursor:pointer; font-weight:bold; text-decoration:underline;' onclick=\"abrirMascarasComTecnico('{tec_safe}', '{contato_safe}')\" title='Criar Máscara'>👨‍🔧 {p['tecnico']} <i class='fa-solid fa-share-from-square' style='font-size:0.85em; margin-left:3px;'></i></span><br>⏰ {h_fmt}<br>"
                if p['segmento'] and p['segmento'] != 'Não especificado': tec_info += f"⚙️ {p['segmento']}<br>"
                tec_info += f"📞 <a href='tel:{p['contato_corp']}' style='color:#38bdf8; text-decoration:none;'>{p['contato_corp']}</a><br>👤 Sup: {p['supervisor']}</div><hr style='border-top:1px dashed var(--border); margin:8px 0;'>"
                if 'INFRA' in p['segmento'].upper(): resposta["infra"].append(tec_info)
                else: resposta["tx"].append(tec_info)
            conn.close()
            return resposta

    cursor.execute("SELECT sigla, nome_da_localidade, ddd, cm_responsavel FROM sites")
    sites_db = cursor.fetchall()
    siglas = [r['sigla'] for r in sites_db]
    match_sigla = process.extractOne(termo, siglas)
    if match_sigla and match_sigla[1] > 80:
        site = next((s for s in sites_db if s['sigla'] == match_sigla[0]), None)
        cm_banco = site.get('cm_responsavel', '').strip()
        cm_busca = cm_banco if cm_banco and cm_banco != 'NAN' else match_sigla[0][:3]
        
        cursor.execute("SELECT * FROM escala WHERE cm ILIKE %s AND dia_mes = %s", (f"%{cm_busca}%", dia_alvo))
        plantoes = cursor.fetchall()
        conn.close()
        resposta = {"encontrado": True, "cabecalho": f"📍 <b>{site['nome_da_localidade']} ({match_sigla[0]})</b><br>📅 Data de Busca: {dia_alvo}/{mes_alvo} | DDD: {site['ddd']} | Base: {cm_busca}", "infra": [], "tx": []}
        
        if plantoes:
            save_historico(nome_usuario, match_sigla[0], "Localizado")
            for p in plantoes:
                h_fmt = LEGENDA_HORARIOS.get(p['horario'], f"Escala {p['horario']}")
                tec_safe = str(p['tecnico']).replace("'", "").replace('"', '')
                contato_safe = str(p['contato_corp']).replace("'", "").replace('"', '')
                tec_info = f"<div style='margin-bottom: 10px;'><span style='color:var(--primary); cursor:pointer; font-weight:bold; text-decoration:underline;' onclick=\"abrirMascarasComTecnico('{tec_safe}', '{contato_safe}')\" title='Criar Máscara'>👨‍🔧 {p['tecnico']} <i class='fa-solid fa-share-from-square' style='font-size:0.85em; margin-left:3px;'></i></span><br>⏰ {h_fmt}<br>"
                if p['segmento'] and p['segmento'] != 'Não especificado': tec_info += f"⚙️ {p['segmento']}<br>"
                tec_info += f"📞 <a href='tel:{p['contato_corp']}' style='color:#38bdf8; text-decoration:none;'>{p['contato_corp']}</a><br>👤 Sup: {p['supervisor']}</div><hr style='border-top:1px dashed var(--border); margin:8px 0;'>"
                if 'INFRA' in p['segmento'].upper(): resposta["infra"].append(tec_info)
                else: resposta["tx"].append(tec_info)
        else:
             save_historico(nome_usuario, match_sigla[0], "Sem cobertura")
             resposta["erro"] = f"⚠️ Nenhum técnico exclusivo da base <b>{cm_busca}</b> de plantão hoje."
        return resposta
    
    conn.close()
    save_historico(nome_usuario, termo[:10], "Inválido")
    return {"encontrado": False, "erro": "Sigla, Base ou Planilha não encontrada."}
