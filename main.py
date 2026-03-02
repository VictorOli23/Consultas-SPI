import os
from flask import Flask, render_template, request, jsonify, session
from werkzeug.utils import secure_filename
from database import init_db, process_excel_sites, process_excel_escala, query_data, save_suggestion, get_suggestions

app = Flask(__name__)
# Chave secreta para a sessão de admin
app.secret_key = os.environ.get("SECRET_KEY", "chave_secreta_spi_2026")
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Inicializa o banco de dados (Cria tabelas no Neon.tech se não existirem)
init_db()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/chat", methods=["POST"])
def chat():
    dados = request.json
    user_input = dados.get("message")
    data_consulta = dados.get("data") # O front-end manda a data (ex: "2/3")
    
    if not user_input:
        return jsonify({"error": "Mensagem vazia"}), 400

    resultado = query_data(user_input, data_consulta)
    return jsonify({"response": resultado})

@app.route("/login", methods=["POST"])
def login():
    dados = request.json
    username = dados.get("usuario")
    password = dados.get("senha")
    
    # Valida contra a senha configurada no painel do Render
    SENHA_SECRETA = os.environ.get("ADMIN_PASSWORD")
    
    if username == "81032045" and password == SENHA_SECRETA: 
        session['logged_in'] = True
        return jsonify({"sucesso": True}), 200
    return jsonify({"erro": "Credenciais inválidas"}), 401

@app.route("/sugestoes", methods=["POST"])
def post_sugestao():
    dados = request.json
    save_suggestion(dados.get("usuario", "Anônimo"), dados.get("texto"))
    return jsonify({"sucesso": True}), 200

@app.route("/admin/listar-sugestoes")
def listar_sugestoes():
    if not session.get('logged_in'):
        return jsonify({"erro": "Não autorizado"}), 401
    return jsonify(get_suggestions())

@app.route("/upload_sites", methods=["POST"])
def upload_sites():
    if not session.get('logged_in'): return jsonify({"erro": "Não autorizado"}), 401
    file = request.files.get("planilha")
    if file:
        filepath = os.path.join(UPLOAD_FOLDER, secure_filename(file.filename))
        file.save(filepath)
        process_excel_sites(filepath)
        return jsonify({"mensagem": "Sites atualizados!"}), 200
    return jsonify({"erro": "Nenhum arquivo"}), 400

@app.route("/upload_escala", methods=["POST"])
def upload_escala():
    if not session.get('logged_in'): return jsonify({"erro": "Não autorizado"}), 401
    file = request.files.get("planilha")
    if file:
        filepath = os.path.join(UPLOAD_FOLDER, secure_filename(file.filename))
        file.save(filepath)
        process_excel_escala(filepath)
        return jsonify({"mensagem": "Escala atualizada!"}), 200
    return jsonify({"erro": "Nenhum arquivo"}), 400

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
