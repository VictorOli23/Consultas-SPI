from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from werkzeug.security import generate_password_hash, check_password_hash
import shutil
import os

# Importação das funções do database.py (certifica-te que o database.py está atualizado)
from database import init_db, query_data, process_excel_sites, process_excel_escala

# Cria a pasta de uploads caso não exista
os.makedirs("uploads", exist_ok=True)

app = FastAPI(title="NetQuery Operations")

# Configuração de Sessão (Middleware) para o login do Victor
app.add_middleware(SessionMiddleware, secret_key="victor_sistecom_2026_sp")
templates = Jinja2Templates(directory="templates")

# Inicializa as tabelas no PostgreSQL ao arrancar
init_db()

# Credenciais de Acesso Administrativo
ADMIN_USER = "81032045"
ADMIN_PASS_HASH = generate_password_hash("Py@thon26!")

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# Rota Health Check (UptimeRobot)
@app.head("/ping")
@app.get("/ping")
async def health_check():
    return {"status": "Online", "sistema": "NetQuery Terminal"}

@app.post("/query")
async def ask_query(request: Request):
    try:
        data = await request.json()
        question = data.get("question", "")
        # A função query_data no database.py tratará de ver o dia de hoje automaticamente
        answer = query_data(question)
        return JSONResponse({"answer": answer})
    except Exception as e:
        return JSONResponse({"answer": f"Erro no processamento da consulta: {str(e)}"})

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
async def login_post(request: Request, username: str = Form(...), password: str = Form(...)):
    if username == ADMIN_USER and check_password_hash(ADMIN_PASS_HASH, password):
        request.session["user"] = username
        return RedirectResponse(url="/admin", status_code=303)
    return templates.TemplateResponse("login.html", {"request": request, "error": "Credenciais Inválidas."})

@app.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request):
    # Verifica se o utilizador está logado
    if not request.session.get("user"):
        return RedirectResponse(url="/login", status_code=303)
    return templates.TemplateResponse("admin.html", {"request": request})

# Rota de Upload Dinâmica: diferencia se estás a subir a base de SITES ou a ESCALA
@app.post("/upload/{tipo}")
async def upload_file(request: Request, tipo: str, file: UploadFile = File(...)):
    if not request.session.get("user"):
        return RedirectResponse(url="/login", status_code=303)
    
    file_path = f"uploads/{file.filename}"
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        if tipo == "sites":
            # Processa a aba 'padrao' para atualizar siglas e dados técnicos
            process_excel_sites(file_path)
            msg = "Base de Localidades (Sites) atualizada com sucesso!"
        elif tipo == "escala":
            # Processa as abas de DDD (12, 14, 15, 16, 17, 18, 19CAS, 19PAA) e Funcionários
            process_excel_escala(file_path)
            msg = "Escala de Plantão Mensal carregada com sucesso!"
        else:
            raise Exception("Tipo de upload desconhecido.")
        
        return templates.TemplateResponse("admin.html", {"request": request, "msg": msg})
    
    except Exception as e:
        return templates.TemplateResponse("admin.html", {"request": request, "error": f"Falha no Excel: {str(e)}"})
    
    finally:
        # Remove o ficheiro temporário após o processamento
        if os.path.exists(file_path):
            os.remove(file_path)

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/", status_code=303)
