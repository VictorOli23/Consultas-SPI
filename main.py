import os
from flask import Flask, render_template, request, jsonify, session
from werkzeug.utils import secure_filename
from database import init_db, process_excel_sites, process_excel_escala, query_data, save_suggestion, get_suggestions, get_historico, ping_user, get_online_users, get_all_tecnicos

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "chave_secreta_spi_2026")
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

init_db()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/chat", methods=["POST"])
def chat():
    dados = request.json
    user_input = dados.get("message")
    data_consulta = dados.get("data")
    nome_usuario = dados.get("nome", "Anônimo")
    
    if not user_input:
        return jsonify({"error": "Mensagem vazia"}), 400

    resultado = query_data(user_input, data_consulta, nome_usuario)
    return jsonify({"response": resultado})

# --- ROTA DOS TÉCNICOS PARA AS MÁSCARAS ---
@app.route("/tecnicos", methods=["GET"])
def tecnicos():
    return jsonify(get_all_tecnicos())

@app.route("/ping", methods=["POST"])
def ping():
    nome = request.json.get("nome")
    if nome: ping_user(nome)
    return jsonify({"status": "ok"})

@app.route("/admin/online", methods=["GET"])
def online():
    if not session.get('logged_in'): return jsonify([]), 401
    return jsonify(get_online_users())

@app.route("/historico", methods=["GET"])
def historico():
    return jsonify(get_historico())

@app.route("/login", methods=["POST"])
def login():
    dados = request.json
    if dados.get("usuario") == "81032045" and dados.get("senha") == os.environ.get("ADMIN_PASSWORD"): 
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
    if not session.get('logged_in'): return jsonify({"erro": "Não autorizado"}), 401
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
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
