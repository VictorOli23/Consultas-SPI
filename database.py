import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import pandas as pd
from datetime import datetime
from thefuzz import process

DB_URL = os.getenv("DATABASE_URL")

def get_connection():
    return psycopg2.connect(DB_URL)

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    
    # 1. Tabela de Sites (Localidades)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sites (
            sigla TEXT PRIMARY KEY, 
            nome_da_localidade TEXT, 
            localidade TEXT,
            area TEXT, 
            ddd TEXT, 
            telefone TEXT, 
            cx TEXT, 
            tx TEXT, 
            ie TEXT
        )
    ''')
    
    # 2. Tabela de Escala (Estrutura Base)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS escala (
            id SERIAL PRIMARY KEY, 
            ddd_aba TEXT, 
            tecnico TEXT, 
            dia_mes INT, 
            mes_ano TEXT, 
            horario TEXT,
            UNIQUE(ddd_aba, tecnico, dia_mes, mes_ano)
        )
    ''')
    
    # MIGRAÇÃO: Garante que as novas colunas existem no banco do Render
    cursor.execute("ALTER TABLE escala ADD COLUMN IF NOT EXISTS contato_corp TEXT")
    cursor.execute("ALTER TABLE escala ADD COLUMN IF NOT EXISTS supervisor TEXT")
    cursor.execute("ALTER TABLE escala ADD COLUMN IF NOT EXISTS cm TEXT")
    
    # 3. Tabela de Funcionários
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS funcionarios (
            nome TEXT PRIMARY KEY, 
            telefone TEXT
        )
    ''')
    
    conn.commit()
    conn.close()

def process_excel_sites(file_path):
    xl = pd.ExcelFile(file_path)
    aba_sites = 'padrao' if 'padrao' in xl.sheet_names else xl.sheet_names[0]
    df = xl.parse(aba_sites).fillna('')
    
    # BLOQUEIO DE DUPLICADOS: Usa dicionário para filtrar siglas repetidas na planilha
    sites_unicos = {}
    for _, row in df.iterrows():
        sigla = str(row.get('Sigla', '')).strip().upper()
        if not sigla: continue
        
        sites_unicos[sigla] = (
            sigla, 
            str(row.get('NomeDaLocalidade','')), 
            str(row.get('localidade','')),
            str(row.get('Area','')), 
            str(row.get('DDD','')), 
            str(row.get('Telefone','')),
            str(row.get('CX','')), 
            str(row.get('TX','')), 
            str(row.get('IE',''))
        )
    
    data_list = list(sites_unicos.values())
    
    if data_list:
        conn = get_connection()
        cursor = conn.cursor()
        query = """
            INSERT INTO sites (sigla, nome_da_localidade, localidade, area, ddd, telefone, cx, tx, ie)
            VALUES %s 
            ON CONFLICT (sigla) DO UPDATE SET 
                nome_da_localidade=EXCLUDED.nome_da_localidade, 
                ddd=EXCLUDED.ddd, 
                area=EXCLUDED.area,
                localidade=EXCLUDED.localidade,
                telefone=EXCLUDED.telefone,
                cx=EXCLUDED.cx,
                tx=EXCLUDED.tx,
                ie=EXCLUDED.ie
        """
        execute_values(cursor, query, data_list)
        conn.commit()
        conn.close()

def process_excel_escala(file_path):
    xl = pd.ExcelFile(file_path, engine='openpyxl')
    mes_ano = datetime.now().strftime('%m-%Y')
    
    conn = get_connection()
    cursor = conn.cursor()
    # Limpa a escala para evitar conflitos de meses anteriores
    cursor.execute("TRUNCATE TABLE escala") 
    
    # Filtro de abas que contêm DDD ou nomes específicos
    abas_alvo = [s for s in xl.sheet_names if any(d in s for d in ['12','14','15','16','17','18','19'])]
    
    escala_limpa = {} # Chave: (aba, tecnico, dia) para evitar duplicados no mesmo arquivo

    for aba in abas_alvo:
        df = xl.parse(aba).fillna('')
        
        # Localiza o cabeçalho real 'Funcionários'
        header_row_idx = None
        for i, row in df.iterrows():
            if 'Funcionários' in row.values:
                header_row_idx = i
                break
        
        if header_row_idx is not None:
            df.columns = df.iloc[header_row_idx]
            df = df.iloc[header_row_idx + 1:]
        
        col_dias = [c for c in df.columns if str(c).isdigit()]
        
        for _, row in df.iterrows():
            tec = str(row.get('Funcionários', '')).strip()
            if not tec or tec.lower() in ['nan', 'funcionários', '']: continue
            
            contato = str(row.get('ContatoCorp.', '')).strip()
            supervisor = str(row.get('Supervisor', '')).strip()
            cm = str(row.get('CM', '')).strip()
            
            for dia in col_dias:
                valor = str(row[dia]).strip()
                if valor and valor.upper() != 'F':
                    # Chave única para evitar o erro de ON CONFLICT no lote
                    chave = (aba, tec, int(dia), mes_ano)
                    escala_limpa[chave] = (
                        aba, tec, contato, supervisor, cm, int(dia), mes_ano, valor
                    )

    data_to_insert = list(escala_limpa.values())

    if data_to_insert:
        execute_values(cursor, """
            INSERT INTO escala (ddd_aba, tecnico, contato_corp, supervisor, cm, dia_mes, mes_ano, horario)
            VALUES %s
        """, data_to_insert)
        
    conn.commit()
    conn.close()

def query_data(user_text):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    hoje = datetime.now()
    
    cursor.execute("SELECT sigla FROM sites")
    siglas = [r['sigla'] for r in cursor.fetchall()]
    
    words = user_text.upper().replace('?', '').split()
    match = next((w for w in words if w in siglas), None)
    
    if not match:
        res_fuzzy = process.extractOne(user_text.upper(), siglas)
        if res_fuzzy and res_fuzzy[1] >= 80: match = res_fuzzy[0]

    if match:
        cursor.execute("SELECT * FROM sites WHERE sigla = %s", (match,))
        site = cursor.fetchone()
        
        # Busca escala batendo DDD + Dia de Hoje
        cursor.execute("""
            SELECT tecnico, contato_corp, supervisor, cm, horario 
            FROM escala 
            WHERE ddd_aba LIKE %s AND dia_mes = %s AND mes_ano = %s
        """, (f"%{site['ddd']}%", hoje.day, hoje.strftime('%m-%Y')))
        
        plantonistas = cursor.fetchall()
        conn.close()

        res = f"📡 <b>NetQuery Terminal</b><br><hr>📍 <b>{site['nome_da_localidade']} ({match})</b><br>"
        res += f"🏢 DDD: {site['ddd']} | Dia: {hoje.strftime('%d/%m')}<br><br>"
        
        if plantonistas:
            for p in plantonistas:
                res += f"👨‍🔧 {p['tecnico']} (<b>{p['horario']}</b>)<br>"
                res += f"📞 <a href='tel:{p['contato_corp']}' style='color:#38bdf8'>{p['contato_corp']}</a><br>"
                res += f"👤 Sup: {p['supervisor']}<br>"
                res += f"🖥️ CM: {p['cm']}<hr style='border:0; border-top:1px dashed #334155; margin:10px 0;'>"
        else:
            res += "⚠️ Nenhuma escala de plantão encontrada para hoje nesta região."
        return res
    
    conn.close()
    return "Sigla não encontrada. Exemplo: 'Plantão SJC?'"
