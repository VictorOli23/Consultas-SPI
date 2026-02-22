import os
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from datetime import datetime
from thefuzz import process

DB_URL = os.getenv("DATABASE_URL")

def get_connection():
    return psycopg2.connect(DB_URL)

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    # Tabela de Regiões
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
    # Tabela de Escala
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS escala (
            id SERIAL PRIMARY KEY,
            ddd TEXT,
            tecnico TEXT,
            dia_mes INT,
            mes_ano TEXT,
            horario TEXT,
            UNIQUE(ddd, tecnico, dia_mes, mes_ano)
        )
    ''')
    # Tabela de Funcionários
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
    conn = get_connection()
    cursor = conn.cursor()
    
    # Processa a aba 'padrao' para dados técnicos
    aba_sites = 'padrao' if 'padrao' in xl.sheet_names else xl.sheet_names[0]
    df_sites = xl.parse(aba_sites).fillna('')
    for _, row in df_sites.iterrows():
        sigla = str(row.get('Sigla', '')).strip().upper()
        if not sigla: continue
        cursor.execute("""
            INSERT INTO sites (sigla, localidade, nome_da_localidade, area, ddd, telefone, cx, tx, ie)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sigla) DO UPDATE SET
                nome_da_localidade=EXCLUDED.nome_da_localidade, 
                ddd=EXCLUDED.ddd, 
                telefone=EXCLUDED.telefone,
                area=EXCLUDED.area,
                localidade=EXCLUDED.localidade
        """, (sigla, str(row.get('localidade','')), str(row.get('NomeDaLocalidade','')), 
              str(row.get('Area','')), str(row.get('DDD','')), str(row.get('Telefone','')),
              str(row.get('CX','')), str(row.get('TX','')), str(row.get('IE',''))))
    conn.commit()
    conn.close()

def process_excel_escala(file_path):
    xl = pd.ExcelFile(file_path)
    conn = get_connection()
    cursor = conn.cursor()
    
    # 1. Funcionários
    if 'funcionarios' in xl.sheet_names:
        df_func = xl.parse('funcionarios').fillna('')
        for _, row in df_func.iterrows():
            nome = str(row.get('Nome', '')).strip()
            if not nome: continue
            cursor.execute("""
                INSERT INTO funcionarios (nome, telefone) VALUES (%s, %s)
                ON CONFLICT (nome) DO UPDATE SET telefone = EXCLUDED.telefone
            """, (nome, str(row.get('Telefone', '')).strip()))

    # 2. Escalas (Abas numéricas)
    mes_ano_atual = datetime.now().strftime('%m-%Y')
    ddd_sheets = [s for s in xl.sheet_names if s.isdigit()]
    
    for ddd in ddd_sheets:
        df = xl.parse(ddd).fillna('')
        colunas_dias = [c for c in df.columns if str(c).isdigit()]
        for _, row in df.iterrows():
            tecnico = str(row.get('Nome', '')).strip()
            if not tecnico: continue
            for dia in colunas_dias:
                valor = str(row[dia]).strip()
                if valor and valor.upper() != 'F':
                    cursor.execute("""
                        INSERT INTO escala (ddd, tecnico, dia_mes, mes_ano, horario)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (ddd, tecnico, dia_mes, mes_ano) 
                        DO UPDATE SET horario = EXCLUDED.horario
                    """, (ddd, tecnico, int(dia), mes_ano_atual, valor))
    conn.commit()
    conn.close()

def query_data(user_text):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    hoje = datetime.now()
    dia_hoje = hoje.day
    mes_ano_hoje = hoje.strftime('%m-%Y')

    cursor.execute("SELECT * FROM sites")
    sites = cursor.fetchall()
    siglas_list = [s['sigla'] for s in sites]
    
    words = user_text.upper().replace('?', '').split()
    match_sigla = next((w for w in words if w in siglas_list), None)
    
    if not match_sigla:
        best_match, score = process.extractOne(user_text.upper(), siglas_list)
        if score >= 80: match_sigla = best_match

    if match_sigla:
        site_data = next(item for item in sites if item["sigla"] == match_sigla)
        cursor.execute("""
            SELECT e.tecnico, e.horario, f.telefone
            FROM escala e
            LEFT JOIN funcionarios f ON e.tecnico = f.nome
            WHERE e.ddd = %s AND e.dia_mes = %s AND e.mes_ano = %s
        """, (str(site_data['ddd']), dia_hoje, mes_ano_hoje))
        
        plantonistas = cursor.fetchall()
        conn.close()

        res = f"📡 <b>Terminal NetQuery</b><br><hr>"
        res += f"📍 <b>Localidade:</b> {site_data['nome_da_localidade']} ({match_sigla})<br>"
        res += f"🏢 <b>Área / DDD:</b> {site_data['area']} / {site_data['ddd']}<br><br>"
        res += f"📅 <b>Escala para Hoje ({hoje.strftime('%d/%m')}):</b><br>"
        
        if plantonistas:
            for p in plantonistas:
                res += f"👨‍🔧 <b>Técnico:</b> {p['tecnico']}<br>"
                res += f"⏰ <b>Plantão/Turno:</b> {p['horario']}<br>"
                res += f"📱 <b>Celular:</b> <a href='tel:{p['telefone']}' style='color:#38bdf8'>{p['telefone']}</a><br><br>"
        else:
            res += "⚠️ <i>Nenhum plantonista identificado para este DDD hoje.</i>"
        return res
    conn.close()
    return "Sigla não encontrada. Ex: 'Plantão SJC?'"
