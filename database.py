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
    # Tabela de Localidades (Aba 'padrao')
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
    # Tabela de Escala com colunas extras: Supervisor e CM
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS escala (
            id SERIAL PRIMARY KEY,
            ddd_aba TEXT,
            tecnico TEXT,
            contato_corp TEXT,
            supervisor TEXT,
            cm TEXT,
            dia_mes INT,
            mes_ano TEXT,
            horario TEXT,
            UNIQUE(ddd_aba, tecnico, dia_mes, mes_ano)
        )
    ''')
    # Tabela de Funcionários (Caso queira manter contatos fixos)
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
    aba_sites = 'padrao' if 'padrao' in xl.sheet_names else xl.sheet_names[0]
    df_sites = xl.parse(aba_sites).fillna('')
    for _, row in df_sites.iterrows():
        sigla = str(row.get('Sigla', '')).strip().upper()
        if not sigla: continue
        cursor.execute("""
            INSERT INTO sites (sigla, localidade, nome_da_localidade, area, ddd, telefone, cx, tx, ie)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sigla) DO UPDATE SET
                nome_da_localidade=EXCLUDED.nome_da_localidade, ddd=EXCLUDED.ddd, area=EXCLUDED.area
        """, (sigla, str(row.get('localidade','')), str(row.get('NomeDaLocalidade','')), 
              str(row.get('Area','')), str(row.get('DDD','')), str(row.get('Telefone','')),
              str(row.get('CX','')), str(row.get('TX','')), str(row.get('IE',''))))
    conn.commit()
    conn.close()

def process_excel_escala(file_path):
    xl = pd.ExcelFile(file_path)
    conn = get_connection()
    cursor = conn.cursor()
    
    # Limpa escala anterior para não duplicar dados do mês
    cursor.execute("DELETE FROM escala")
    
    mes_ano_atual = datetime.now().strftime('%m-%Y')
    # Filtra abas que começam com DDD ou nomes específicos de escala
    abas_escala = [s for s in xl.sheet_names if any(ddd in s for ddd in ['12','14','15','16','17','18','19'])]
    
    for aba in abas_escala:
        df = xl.parse(aba).fillna('')
        # Identifica colunas de dias (1 a 31)
        colunas_dias = [c for c in df.columns if str(c).isdigit()]
        
        for _, row in df.iterrows():
            # Mapeamento conforme sua descrição
            tecnico = str(row.get('Funcionários', '')).strip()
            if not tecnico: continue
            
            contato = str(row.get('ContatoCorp.', '')).strip()
            supervisor = str(row.get('Supervisor', '')).strip()
            cm = str(row.get('CM', '')).strip()
            
            for dia in colunas_dias:
                valor_escala = str(row[dia]).strip()
                # Ignora Folgas ('F') e células vazias
                if valor_escala and valor_escala.upper() != 'F':
                    cursor.execute("""
                        INSERT INTO escala (ddd_aba, tecnico, contato_corp, supervisor, cm, dia_mes, mes_ano, horario)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (aba, tecnico, contato, supervisor, cm, int(dia), mes_ano_atual, valor_escala))
    
    conn.commit()
    conn.close()

def query_data(user_text):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Pega o dia de HOJE automaticamente
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
        ddd_site = str(site_data['ddd'])
        
        # Busca escala baseada no DDD do site e no dia de HOJE
        # A query busca técnicos cujo DDD da aba contenha o DDD do site (ex: '19' em '19CAS')
        cursor.execute("""
            SELECT tecnico, contato_corp, supervisor, cm, horario
            FROM escala 
            WHERE ddd_aba LIKE %s AND dia_mes = %s AND mes_ano = %s
        """, (f'%{ddd_site}%', dia_hoje, mes_ano_hoje))
        
        plantonistas = cursor.fetchall()
        conn.close()

        res = f"📡 <b>NetQuery Terminal</b><br><hr>"
        res += f"📍 <b>Localidade:</b> {site_data['nome_da_localidade']} ({match_sigla})<br>"
        res += f"🏢 <b>Área/DDD:</b> {site_data['area']} / {site_data['ddd']}<br>"
        res += f"📅 <b>Plantonistas de Hoje ({hoje.strftime('%d/%m')}):</b><br><br>"
        
        if plantonistas:
            for p in plantonistas:
                res += f"👨‍🔧 <b>Técnico:</b> {p['tecnico']}<br>"
                res += f"⏰ <b>Horário:</b> {p['horario']}<br>"
                res += f"📞 <b>Contato:</b> <a href='tel:{p['contato_corp']}' style='color:#38bdf8'>{p['contato_corp']}</a><br>"
                res += f"👤 <b>Supervisor:</b> {p['supervisor']}<br>"
                res += f"🖥️ <b>CM:</b> {p['cm']}<br><hr style='border:0; border-top:1px dashed #334155; margin:10px 0;'>"
        else:
            res += "⚠️ <i>Nenhum plantão encontrado para este DDD hoje na escala.</i>"
        return res

    conn.close()
    return "Sigla não identificada. Tente: 'Quem atende em SJC?'"
