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
    
    # 1. Tabela de Sites
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sites (
            sigla TEXT PRIMARY KEY, nome_da_localidade TEXT, localidade TEXT,
            area TEXT, ddd TEXT, telefone TEXT, cx TEXT, tx TEXT, ie TEXT
        )
    ''')
    
    # 2. Tabela de Escala (Estrutura Base)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS escala (
            id SERIAL PRIMARY KEY, ddd_aba TEXT, tecnico TEXT, 
            dia_mes INT, mes_ano TEXT, horario TEXT,
            UNIQUE(ddd_aba, tecnico, dia_mes, mes_ano)
        )
    ''')
    
    # --- MIGRAÇÃO: Força a criação das colunas novas caso a tabela já exista ---
    cursor.execute("ALTER TABLE escala ADD COLUMN IF NOT EXISTS contato_corp TEXT")
    cursor.execute("ALTER TABLE escala ADD COLUMN IF NOT EXISTS supervisor TEXT")
    cursor.execute("ALTER TABLE escala ADD COLUMN IF NOT EXISTS cm TEXT")
    
    # 3. Tabela de Funcionários
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS funcionarios (nome TEXT PRIMARY KEY, telefone TEXT)
    ''')
    
    conn.commit()
    conn.close()

def process_excel_sites(file_path):
    # Carregamento rápido de sites
    xl = pd.ExcelFile(file_path)
    aba_sites = 'padrao' if 'padrao' in xl.sheet_names else xl.sheet_names[0]
    df = xl.parse(aba_sites).fillna('')
    
    data_list = []
    for _, row in df.iterrows():
        sigla = str(row.get('Sigla', '')).strip().upper()
        if not sigla: continue
        data_list.append((
            sigla, str(row.get('NomeDaLocalidade','')), str(row.get('localidade','')),
            str(row.get('Area','')), str(row.get('DDD','')), str(row.get('Telefone','')),
            str(row.get('CX','')), str(row.get('TX','')), str(row.get('IE',''))
        ))
    
    if data_list:
        conn = get_connection()
        cursor = conn.cursor()
        # Inserção em lote para ser instantâneo
        query = """
            INSERT INTO sites (sigla, nome_da_localidade, localidade, area, ddd, telefone, cx, tx, ie)
            VALUES %s 
            ON CONFLICT (sigla) DO UPDATE SET 
                nome_da_localidade=EXCLUDED.nome_da_localidade, 
                ddd=EXCLUDED.ddd, 
                area=EXCLUDED.area
        """
        execute_values(cursor, query, data_list)
        conn.commit()
        conn.close()

def process_excel_escala(file_path):
    xl = pd.ExcelFile(file_path, engine='openpyxl')
    mes_ano = datetime.now().strftime('%m-%Y')
    
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE escala") # Limpeza ultra rápida
    
    all_data = []
    abas_escala = [s for s in xl.sheet_names if any(d in s for d in ['12','14','15','16','17','18','19'])]
    
    for aba in abas_escala:
        df = xl.parse(aba).fillna('')
        
        # Detecta onde começa o cabeçalho 'Funcionários' (pula linhas vazias do topo)
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
                    all_data.append((
                        aba, tec, contato, supervisor, cm, int(dia), mes_ano, valor
                    ))
        
        # Envia para o banco em blocos para não travar a memória
        if len(all_data) > 1000:
            execute_values(cursor, """
                INSERT INTO escala (ddd_aba, tecnico, contato_corp, supervisor, cm, dia_mes, mes_ano, horario)
                VALUES %s
            """, all_data)
            all_data = []

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

        res = f"📡 <b>Terminal NetQuery</b><br><hr>📍 <b>{site['nome_da_localidade']} ({match})</b><br>"
        res += f"🏢 DDD: {site['ddd']} | Dia: {hoje.strftime('%d/%m')}<br><br>"
        
        if plantonistas:
            for p in plantonistas:
                res += f"👨‍🔧 {p['tecnico']} (<b>{p['horario']}</b>)<br>"
                res += f"📞 <a href='tel:{p['contato_corp']}' style='color:#38bdf8'>{p['contato_corp']}</a><br>"
                res += f"👤 Sup: {p['supervisor']}<br>"
                res += f"🖥️ CM: {p['cm']}<hr style='border:0; border-top:1px dashed #334155; margin:10px 0;'>"
        else:
            res += "⚠️ Nenhuma escala de plantão encontrada para hoje."
        return res
    
    conn.close()
    return "Sigla não encontrada. Tente: 'Quem está em SJC?'"
