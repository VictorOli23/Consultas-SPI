import os
from flask import Flask, render_template, request, jsonify, session, redirect, url_for, flash
from werkzeug.utils import secure_filename
from database import init_db, process_excel_sites, process_excel_escala, query_data, get_db_stats

app = Flask(__name__)
app.secret_key = 'netquery_secreto_2026'
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

init_db()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/chat", methods=["POST"])
def chat():
    user_input = request.json.get("message")
    if not user_input:
        return jsonify({"error": "Mensagem vazia"}), 400

    resultado = query_data(user_input)
    return jsonify({"response": resultado})

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        
        SENHA_SECRETA = os.environ.get("ADMIN_PASSWORD")
        
        if username == "81032045" and password == SENHA_SECRETA: 
            session['logged_in'] = True
            return redirect(url_for('admin'))
        else:
            return render_template("login.html", erro="Credenciais inválidas")
    return render_template("login.html")

@app.route("/admin")
def admin():
    if not session.get('logged_in'):
        return redirect(url_for('login'))
    
    # Pega os status do banco para mostrar na tela
    db_stats = get_db_stats()
    return render_template("admin.html", stats=db_stats)

@app.route("/upload_sites", methods=["POST"])
def upload_sites():
    if not session.get('logged_in'): return redirect(url_for('login'))
    file = request.files.get("file")
    if file:
        filepath = os.path.join(UPLOAD_FOLDER, secure_filename(file.filename))
        file.save(filepath)
        try:
            process_excel_sites(filepath)
            flash("✅ Planilha de SITES atualizada com sucesso no banco de dados!", "success")
        except Exception as e:
            flash(f"❌ Erro ao processar Sites: {e}", "error")
    return redirect(url_for('admin'))

@app.route("/upload_escala", methods=["POST"])
def upload_escala():
    if not session.get('logged_in'): return redirect(url_for('login'))
    file = request.files.get("file")
    if file:
        filepath = os.path.join(UPLOAD_FOLDER, secure_filename(file.filename))
        file.save(filepath)
        try:
            process_excel_escala(filepath)
            flash("✅ Planilha de ESCALA atualizada com sucesso no banco de dados!", "success")
        except Exception as e:
            flash(f"❌ Erro ao processar Escala: {e}", "error")
    return redirect(url_for('admin'))

@app.route("/logout")
def logout():
    session.pop('logged_in', None)
    return redirect(url_for('index'))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
