import os
from flask import Flask, render_template, request, jsonify, session, redirect, url_for, flash
from werkzeug.utils import secure_filename
# Importando suas funções do arquivo database.py
from database import init_db, process_excel_sites, process_excel_escala, query_data, get_db_stats, save_suggestion, get_suggestions

app = Flask(__name__)
# Chave secreta para gerenciar sessões
app.secret_key = os.environ.get("SECRET_KEY", "netquery_secreto_2026")

UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Inicializa o banco de dados (Cria tabelas se não existirem)
init_db()

@app.route("/")
def index():
    return render_template("index.html")

# --- ROTA DO CHAT (AJUSTADA PARA O NOVO LAYOUT) ---
@app.route("/chat", methods=["POST"])
def chat():
    dados = request.json
    user_input = dados.get("message")
    # O frontend agora pode enviar a data formatada (ex: 2/3)
    data_consulta = dados.get("data") 

    if not user_input:
        return jsonify({"error": "Mensagem vazia"}), 400

    # Chama a função de busca no seu database.py
    # Se sua query_data aceitar data, passe aqui
    resultado = query_data(user_input, data_consulta) 
    
    return jsonify({"response": resultado})

# --- ROTA DE LOGIN (AJUSTADA PARA FETCH/JSON) ---
@app.route("/login", methods=["POST"])
def login():
    dados = request.json
    username = dados.get("usuario")
    password = dados.get("senha")
    
    SENHA_SECRETA = os.environ.get("ADMIN_PASSWORD")
    
    if username == "81032045" and password == SENHA_SECRETA: 
        session['logged_in'] = True
        return jsonify({"sucesso": True}), 200
    else:
        return jsonify({"erro": "Credenciais inválidas"}), 401

# --- ROTA DE SUGESTÕES (NOVA) ---
@app.route("/sugestoes", methods=["POST"])
def post_sugestao():
    dados = request.json
    usuario = dados.get("usuario", "Anônimo")
    texto = dados.get("texto")
    
    if not texto:
        return jsonify({"erro": "Texto vazio"}), 400
    
    # Salva no banco através da função que você deve ter no database.py
    save_suggestion(usuario, texto)
    return jsonify({"sucesso": True}), 200

@app.route("/admin/listar-sugestoes")
def listar_sugestoes():
    if not session.get('logged_in'):
        return jsonify({"erro": "Não autorizado"}), 401
    
    sugestoes = get_suggestions() # Busca do banco
    return jsonify(sugestoes)

# --- GERENCIAMENTO DE PLANILHAS ---
@app.route("/admin")
def admin():
    if not session.get('logged_in'):
        return redirect(url_for('login'))
    db_stats = get_db_stats()
    return render_template("admin.html", stats=db_stats)

@app.route("/upload_sites", methods=["POST"])
def upload_sites():
    if not session.get('logged_in'): return jsonify({"erro": "Não autorizado"}), 401
    
    file = request.files.get("planilha") # Ajustado para o nome do campo no novo HTML
    if file:
        filename = secure_filename(file.filename)
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        file.save(filepath)
        try:
            process_excel_sites(filepath)
            return jsonify({"mensagem": "Planilha de SITES atualizada!"}), 200
        except Exception as e:
            return jsonify({"erro": f"Erro: {e}"}), 500
    return jsonify({"erro": "Nenhum arquivo"}), 400

@app.route("/upload_escala", methods=["POST"])
def upload_escala():
    if not session.get('logged_in'): return jsonify({"erro": "Não autorizado"}), 401
    
    file = request.files.get("planilha") # Ajustado para bater com o novo HTML
    if file:
        filename = secure_filename(file.filename)
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        file.save(filepath)
        try:
            process_excel_escala(filepath)
            return jsonify({"mensagem": "Planilha de ESCALA atualizada!"}), 200
        except Exception as e:
            return jsonify({"erro": f"Erro: {e}"}), 500
    return jsonify({"erro": "Nenhum arquivo"}), 400

@app.route("/logout")
def logout():
    session.pop('logged_in', None)
    return redirect(url_for('index'))

if __name__ == "__main__":
    # Porta padrão para o Render (10000 ou variável de ambiente)
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
