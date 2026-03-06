import os
from flask import Flask, render_template, request, jsonify, session
from werkzeug.utils import secure_filename
import google.generativeai as genai
from database import init_db, process_excel_sites, process_excel_escala, query_data, save_suggestion, get_suggestions, get_historico, ping_user, get_online_users, get_all_tecnicos, get_autocomplete_data, set_aviso, get_aviso, get_visao_geral, atualizar_tecnico_dinamico

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "chave_secreta_spi_2026")
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

init_db()

# --- CONFIGURAÇÃO SEGURA DA IA DO GOOGLE ---
# Agora ele puxa a chave do cofre do Render, protegendo contra o bloqueio do GitHub!
GEMINI_KEY = os.environ.get("GEMINI_API_KEY")
if GEMINI_KEY:
    genai.configure(api_key=GEMINI_KEY)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/chat", methods=["POST"])
def chat():
    dados = request.json
    if not dados.get("message"): return jsonify({"error": "Mensagem vazia"}), 400
    resultado = query_data(dados.get("message"), dados.get("data"), dados.get("nome", "Anônimo"))
    return jsonify({"response": resultado})

# --- ROTA DE INTELIGÊNCIA ARTIFICIAL (AUTO-DISCOVERY DE MODELOS) ---
@app.route("/chat_ia", methods=["POST"])
def chat_ia():
    dados = request.json
    mensagem_usuario = dados.get("message")
    
    if not GEMINI_KEY:
        return jsonify({"texto": "A chave da API da IA não foi configurada nas variáveis de ambiente do servidor Render."})

    try:
        # 1. PERGUNTA AO GOOGLE QUAIS MODELOS ESSA CHAVE TEM ACESSO
        modelos_disponiveis = []
        for m in genai.list_models():
            if 'generateContent' in m.supported_generation_methods:
                nome_limpo = m.name.replace('models/', '')
                modelos_disponiveis.append(nome_limpo)
        
        if not modelos_disponiveis:
            return jsonify({"texto": "Erro: A sua chave de API do Google é válida, mas não tem permissão para usar nenhum modelo de texto no momento."})
        
        # 2. ESCOLHE O MELHOR MODELO AUTOMATICAMENTE
        modelo_escolhido = modelos_disponiveis[0] 
        
        preferencias = ['gemini-1.5-flash', 'gemini-1.5-flash-latest', 'gemini-flash-latest', 'gemini-1.5-pro', 'gemini-1.0-pro', 'gemini-pro']
        for pref in preferencias:
            if pref in modelos_disponiveis:
                modelo_escolhido = pref
                break
        
        # 3. INSTANCIA O MODELO GARANTIDO QUE EXISTE NA SUA CONTA
        model = genai.GenerativeModel(modelo_escolhido)
        
        prompt_sistema = """Você é um Assistente Sênior de NOC (Network Operations Center) especializado em Telecom e Infraestrutura.
        Sua missão é ajudar analistas a traduzirem logs complexos e acionarem as equipes de campo. Use formatação HTML <b> para negrito e <ul><li> para listas.

        1. ANÁLISE DE ALARMES: 
        Sempre que o usuário colar um alarme (log de equipamento, energia, temperatura, LOS, BGP, etc) ou perguntar sobre um, você DEVE estruturar sua resposta EXATAMENTE assim:
        
        <b>🔴 O que está acontecendo:</b><br>
        (Escreva 1 ou 2 parágrafos explicando de forma simples e direta o que o alarme significa, o que falhou e qual o possível impacto na rede).<br><br>
        
        <b>🛠️ O que falar para o técnico:</b><br>
        (Escreva uma mensagem pronta, educada e direta para o analista copiar e enviar para o técnico. Indique o site, o equipamento e sugira o que o técnico deve testar primeiro no local - ex: medir tensão, limpar fibra, checar disjuntor).

        2. ALTERAÇÃO DE ESCALA (AÇÃO NO BANCO DE DADOS): 
        Se o usuário pedir para alterar o plantão de algum técnico (ex: "Muda o João para Férias", "Coloca o Marcos na escala 8"), responda confirmando a ação, MAS a ÚLTIMA linha da sua resposta deve ser OBRIGATORIAMENTE este código:
        [UPDATE_DB|NOME_DO_TECNICO|NOVO_STATUS]
        Exemplo: [UPDATE_DB|Joao|Férias]
        """
        
        response = model.generate_content(prompt_sistema + "\n\nUsuário diz: " + mensagem_usuario)
        texto_ia = response.text
        
        # Função secreta de alterar a escala
        if "[UPDATE_DB|" in texto_ia:
            linhas = texto_ia.split('\n')
            comando = [l for l in linhas if "[UPDATE_DB|" in l][0]
            texto_limpo = texto_ia.replace(comando, "").strip()
            
            partes = comando.replace("[", "").replace("]", "").split("|")
            if len(partes) >= 3:
                nome_tec = partes[1]
                novo_status = partes[2]
                resultado_db = atualizar_tecnico_dinamico(nome_tec, novo_status)
                texto_limpo += f"<br><br><div style='background:var(--success); color:white; padding:10px; border-radius:8px;'><b>🤖 Ação da IA concluída:</b><br>{resultado_db}</div>"
            
            texto_html = texto_limpo.replace('\n', '<br>')
            return jsonify({"texto": texto_html})

        texto_html = texto_ia.replace('\n', '<br>')
        return jsonify({"texto": texto_html})
        
    except Exception as e:
        debug_info = f"<b>Falha de conexão com a IA.</b><br>Erro técnico: {str(e)}<br><br><b>Modelos liberados na sua chave do Google:</b><br>{', '.join(modelos_disponiveis) if 'modelos_disponiveis' in locals() else 'Nenhum modelo lido'}<br><br><i>A IA tentou usar o modelo: {modelo_escolhido if 'modelo_escolhido' in locals() else 'Desconhecido'}</i>"
        return jsonify({"texto": debug_info})

@app.route("/autocomplete", methods=["GET"])
def autocomplete(): return jsonify(get_autocomplete_data())

@app.route("/tecnicos", methods=["GET"])
def tecnicos(): return jsonify(get_all_tecnicos())

@app.route("/ping", methods=["POST"])
def ping():
    if request.json.get("nome"): ping_user(request.json.get("nome"))
    return jsonify({"status": "ok"})

@app.route("/admin/online", methods=["GET"])
def online():
    if not session.get('logged_in'): return jsonify([]), 401
    return jsonify(get_online_users())

@app.route("/historico", methods=["GET"])
def historico(): return jsonify(get_historico())

@app.route("/aviso", methods=["GET"])
def fetch_aviso(): return jsonify({"aviso": get_aviso()})

@app.route("/admin/aviso", methods=["POST"])
def update_aviso():
    if not session.get('logged_in'): return jsonify({"erro": "Não autorizado"}), 401
    set_aviso(request.json.get("texto", ""))
    return jsonify({"sucesso": True})

@app.route("/visao_geral", methods=["GET"])
def visao_geral(): return jsonify(get_visao_geral())

@app.route("/login", methods=["POST"])
def login():
    if request.json.get("usuario") == "81032045" and request.json.get("senha") == os.environ.get("ADMIN_PASSWORD"): 
        session['logged_in'] = True
        return jsonify({"sucesso": True}), 200
    return jsonify({"erro": "Credenciais inválidas"}), 401

@app.route("/sugestoes", methods=["POST"])
def post_sugestao():
    save_suggestion(request.json.get("usuario", "Anônimo"), request.json.get("texto"))
    return jsonify({"sucesso": True}), 200

@app.route("/admin/listar-sugestoes")
def listar_sugestoes():
    if not session.get('logged_in'): return jsonify({"erro": "Não autorizado"}), 401
    return jsonify(get_suggestions())

@app.route("/upload_sites", methods=["POST"])
def upload_sites():
    if not session.get('logged_in'): return jsonify({"erro": "Não autorizado"}), 401
    file = request.files.get("planilha")
    if file:
        filepath = os.path.join(UPLOAD_FOLDER, secure_filename(file.filename))
        file.save(filepath)
        try: process_excel_sites(filepath); return jsonify({"mensagem": "Sites atualizados!"}), 200
        except Exception as e: return jsonify({"erro": f"Erro: {str(e)}"}), 500
    return jsonify({"erro": "Nenhum arquivo"}), 400

@app.route("/upload_escala", methods=["POST"])
def upload_escala():
    if not session.get('logged_in'): return jsonify({"erro": "Não autorizado"}), 401
    file = request.files.get("planilha")
    if file:
        filepath = os.path.join(UPLOAD_FOLDER, secure_filename(file.filename))
        file.save(filepath)
        try: process_excel_escala(filepath); return jsonify({"mensagem": "Escala atualizada!"}), 200
        except Exception as e: return jsonify({"erro": f"Erro: {str(e)}"}), 500
    return jsonify({"erro": "Nenhum arquivo"}), 400

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
