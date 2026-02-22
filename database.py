import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import pandas as pd
from datetime import datetime
from thefuzz import process

DB_URL = os.getenv("DATABASE_URL")

# Legenda completa para conversão automática dos códigos da planilha
LEGENDA_HORARIOS = {
    '1': '07:00 as 16:00', '2': '07:30 as 16:30', '3': '08:00 as 17:00',
    '4': '08:30 as 17:30', '5': '11:00 as 20:00', '6': '12:30 as 21:30',
    '7': '13:00 as 22:00', '8': '22:12 as 07:00', '9': '08:00 as 12:00 SABADO',
    '10': '08:00 as 17:00 SABADO', '11': '09:00 as 13:00 SABADO', 
    '12': '09:00 AS 18:00 SABADO', '13': '18:00 as 22:00 SABADO',
    '14': '07:42 as 18:00', '15': '10:00 as 19:00',
    'A': '7:01 ás 8:00', 'B': '7:01 ás 17:30', 
    'D': '7:01 ás 7:00', #
    'E': '16:01 ás 22:11', 'G': '16:01 ás 7:00', 'H': '16:31 ás 22:11',
    'I': '16:31 ás 7:00', 'J': '17:00 ás 22:11', 'K': '17:00 ás 7:00',
    'M': '17:31 ás 22:11', 'N': '17:31 ás 7:00', 'O': '20:01 ás 22:11',
    'P': '20:01 ás 7:00', 'Q': '21:31 ás 22:11', 'R': '21:31 ás 7:11',
    'S': '22:01 ás 7:00', 'T': '18:01 ás 7:00', 'U': '17:00:00 ás 8:00',
    'V': '18:00 ÁS 8:00', 'X': '22:01 ás 8:00', 'Z': '21:31 ás 8:00',
    'W': '08:00 as 18:00', 
    'Y': '07:00 as 22:11', #
    'AA': '22:01 as 9:00', 'AB': '08:01 as 08:00', 'AC': '12:01 ás 07:00', 'AD': '22:00 as 03:00'
}

def get_connection():
    return psycopg2.connect(DB_URL)

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    # Tabela de Sites com todas as colunas técnicas
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
    # Tabela de Escala estruturada para o plantão mensal
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS escala (
            id SERIAL PRIMARY KEY, 
            ddd_aba TEXT, 
            tecnico TEXT, 
            contato_corp TEXT, 
            supervisor TEXT, 
            cm TEXT, 
            dia_mes TEXT, 
            mes_ano TEXT, 
            horario TEXT
        )
    ''')
    # Migrações manuais para garantir que as colunas existam no banco do Render
    cursor.execute("ALTER TABLE escala ADD COLUMN IF NOT EXISTS contato_corp TEXT")
    cursor.execute("ALTER TABLE escala ADD COLUMN IF NOT EXISTS supervisor TEXT")
    cursor.execute("ALTER TABLE escala ADD COLUMN IF NOT EXISTS cm TEXT")
    cursor.execute("ALTER TABLE escala ADD COLUMN IF NOT EXISTS horario TEXT")
    cursor.execute("ALTER TABLE escala ADD COLUMN IF NOT EXISTS dia_mes TEXT")
    
    conn.commit()
    conn.close()

def process_excel_sites(file_path):
    xl = pd.ExcelFile(file_path)
    aba_alvo = 'padrao' if 'padrao' in xl.sheet_names else xl.sheet_names[0]
    df = xl.parse(aba_alvo).fillna('')
    
    conn = get_connection()
    cursor = conn.cursor()
    
    for index, row in df.iterrows():
        sigla = str(row.get('Sigla', '')).strip().upper()
        if sigla:
            cursor.execute("""
                INSERT INTO sites (sigla, nome_da_localidade, localidade, area, ddd, telefone, cx, tx, ie)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sigla) DO UPDATE SET 
                    nome_da_localidade=EXCLUDED.nome_da_localidade, 
                    ddd=EXCLUDED.ddd,
                    area=EXCLUDED.area
            """, (
                sigla, str(row.get('NomeDaLocalidade', '')), str(row.get('localidade', '')),
                str(row.get('Area', '')), str(row.get('DDD', '')), str(row.get('Telefone', '')),
                str(row.get('CX', '')), str(row.get('TX', '')), str(row.get('IE', ''))
            ))
    
    conn.commit()
    conn.close()

def process_excel_escala(file_path):
    xl = pd.ExcelFile(file_path, engine='openpyxl')
    mes_ano = datetime.now().strftime('%m-%Y')
    
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE escala") # Limpa para o novo upload
    
    # Filtra as abas de DDD: 12, 14, 15, 16, 17, 18, 19CAS, 19PAA
    abas_validas = [s for s in xl.sheet_names if any(ddd in s for ddd in ['12','14','15','16','17','18','19'])]
    
    for aba in abas_validas:
        df = xl.parse(aba).fillna('')
        
        # Localiza a linha de cabeçalho "Funcionários"
        header_idx = None
        for i, row in df.iterrows():
            if 'Funcionários' in [str(v).strip() for v in row.values]:
                header_idx = i
                break
        
        if header_idx is not None:
            df.columns = [str(c).strip() for c in df.iloc[header_idx]]
            df = df.iloc[header_idx + 1:]
            
            # Identifica colunas de dias (ex: "22/" ou "22")
            colunas_dias = []
            for col in df.columns:
                clean_col = str(col).replace('/', '').strip()
                if clean_col.isdigit():
                    colunas_dias.append(col)

            for _, row in df.iterrows():
                tecnico = str(row.get('Funcionários', '')).strip()
                if not tecnico or tecnico.lower() in ['nan', 'None', 'funcionários']:
                    continue
                
                contato = str(row.get('ContatoCorp.', ''))
                supervisor = str(row.get('Supervisor', ''))
                cm = str(row.get('CM', ''))
                
                for dia_original in colunas_dias:
                    dia_limpo = str(dia_original).replace('/', '').strip()
                    plantao_valor = str(row[dia_original]).strip().upper()
                    
                    # Salva se não for folga (F) ou vazio
                    if plantao_valor and plantao_valor not in ['F', 'NAN', '', 'C']:
                        cursor.execute("""
                            INSERT INTO escala (ddd_aba, tecnico, contato_corp, supervisor, cm, dia_mes, mes_ano, horario)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """, (aba, tecnico, contato, supervisor, cm, dia_limpo, mes_ano, plantao_valor))

    conn.commit()
    conn.close()

def query_data(user_text):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    hoje = datetime.now()
    dia_atual = str(hoje.day) # Ex: "22"
    
    # Busca siglas para o Match
    cursor.execute("SELECT sigla FROM sites")
    lista_siglas = [r['sigla'] for r in cursor.fetchall()]
    
    # Processamento da pergunta
    pergunta = user_text.upper().replace('?', '')
    match = None
    for word in pergunta.split():
        if word in lista_siglas:
            match = word
            break
    
    if not match:
        res = process.extractOne(pergunta, lista_siglas)
        if res and res[1] >= 80: match = res[0]

    if match:
        # Pega os dados do Site
        cursor.execute("SELECT * FROM sites WHERE sigla = %s", (match,))
        site_data = cursor.fetchone()
        
        # Busca TODOS os técnicos da região (DDD) para o dia de hoje
        # Filtra pelo ddd_aba (ex: aba '15' para site com DDD 15)
        cursor.execute("""
            SELECT tecnico, contato_corp, supervisor, cm, horario 
            FROM escala 
            WHERE (ddd_aba ILIKE %s OR ddd_aba ILIKE %s)
            AND dia_mes = %s AND mes_ano = %s
        """, (f"%{site_data['ddd']}%", f"%{site_data['area']}%", dia_atual, hoje.strftime('%m-%Y')))
        
        lista_plantonistas = cursor.fetchall()
        conn.close()

        # Montagem da Resposta Visual
        res_html = f"📡 <b>NetQuery Terminal</b><br><hr>"
        res_html += f"📍 <b>{site_data['nome_da_localidade']} ({match})</b><br>"
        res_html += f"🏢 DDD: {site_data['ddd']} | Dia: {hoje.strftime('%d/%m')}<br><br>"
        
        if lista_plantonistas:
            for p in lista_plantonistas:
                # Converte código (Y, D) para horário real
                horario_formatado = LEGENDA_HORARIOS.get(p['horario'], f"Escala: {p['horario']}")
                res_html += f"👨‍🔧 {p['tecnico']} (<b>{horario_formatado}</b>)<br>"
                res_html += f"📞 <a href='tel:{p['contato_corp']}' style='color:#38bdf8'>{p['contato_corp']}</a><br>"
                res_html += f"👤 Sup: {p['supervisor']}<br>"
                res_html += f"🖥️ CM: {p['cm']}<hr style='border:0; border-top:1px dashed #334155; margin:10px 0;'>"
        else:
            res_html += f"⚠️ <b>Atenção:</b> Nenhum técnico em plantão ativo para o DDD {site_data['ddd']} hoje."
        
        return res_html
    
    conn.close()
    return "Sigla não encontrada no banco de dados."
