import os
from flask import Flask, render_template, request, jsonify, session
from werkzeug.utils import secure_filename
from database import init_db, process_excel_sites, process_excel_escala, query_data, save_suggestion, get_suggestions, get_db_stats

app = Flask(__name__)
app.secret_key = "chave_secreta_spi"
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

init_db()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/login", methods=["POST"])
def login():
    dados = request.json
    if dados.get("usuario") == "81032045" and dados.get("senha") == os.environ.get("ADMIN_PASSWORD"):
        session['logged_in'] = True
        return jsonify({"status": "ok"}), 200
    return jsonify({"status": "error"}), 401

@app.route("/chat", methods=["POST"])
def chat():
    dados = request.json
    resultado = query_data(dados.get("message"), dados.get("data"))
    return jsonify({"response": resultado})

@app.route("/upload_sites", methods=["POST"])
def upload_sites():
    file = request.files.get("planilha")
    if file:
        path = os.path.join(UPLOAD_FOLDER, secure_filename(file.filename))
        file.save(path)
        process_excel_sites(path)
        return jsonify({"msg": "ok"}), 200

@app.route("/upload_escala", methods=["POST"])
def upload_escala():
    file = request.files.get("planilha")
    if file:
        path = os.path.join(UPLOAD_FOLDER, secure_filename(file.filename))
        file.save(path)
        process_excel_escala(path)
        return jsonify({"msg": "ok"}), 200

@app.route("/admin/listar-sugestoes")
def listar():
    return jsonify(get_suggestions())

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
