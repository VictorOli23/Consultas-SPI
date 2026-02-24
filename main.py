import os
from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from werkzeug.utils import secure_filename
from database import init_db, process_excel_sites, process_excel_escala, query_data

app = Flask(__name__)
app.secret_key = 'netquery_secreto_2026'
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Cria as tabelas no banco de dados assim que o app inicia
init_db()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/chat", methods=["POST"])
def chat():
    user_input = request.json.get("message")
    if not user_input:
        return jsonify({"error": "Mensagem vazia"}), 400

    # A função query_data agora devolve um dicionário com os técnicos separados
    resultado = query_data(user_input)
    return jsonify({"response": resultado})

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        # Utilize sua matrícula e a senha padrão
        if username == "81032045" and password == "admin": 
            session['logged_in'] = True
            return redirect(url_for('admin'))
        else:
            return render_template("login.html", erro="Credenciais inválidas")
    return render_template("login.html")

@app.route("/admin")
def admin():
    if not session.get('logged_in'):
        return redirect(url_for('login'))
    return render_template("admin.html")

@app.route("/upload_sites", methods=["POST"])
def upload_sites():
    if not session.get('logged_in'): return redirect(url_for('login'))
    file = request.files.get("file")
    if file:
        filepath = os.path.join(UPLOAD_FOLDER, secure_filename(file.filename))
        file.save(filepath)
        try:
            process_excel_sites(filepath)
            return "Planilha de Sites processada com sucesso! <br><br><a href='/admin'>Voltar ao Painel</a>"
        except Exception as e:
            return f"Erro ao processar: {e} <br><br><a href='/admin'>Voltar ao Painel</a>"
    return "Nenhum arquivo enviado. <a href='/admin'>Voltar</a>"

@app.route("/upload_escala", methods=["POST"])
def upload_escala():
    if not session.get('logged_in'): return redirect(url_for('login'))
    file = request.files.get("file")
    if file:
        filepath = os.path.join(UPLOAD_FOLDER, secure_filename(file.filename))
        file.save(filepath)
        try:
            process_excel_escala(filepath)
            return "Planilha de Escala processada com sucesso! <br><br><a href='/admin'>Voltar ao Painel</a>"
        except Exception as e:
            return f"Erro ao processar: {e} <br><br><a href='/admin'>Voltar ao Painel</a>"
    return "Nenhum arquivo enviado. <a href='/admin'>Voltar</a>"

@app.route("/logout")
def logout():
    session.pop('logged_in', None)
    return redirect(url_for('index'))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
