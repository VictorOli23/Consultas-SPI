from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from werkzeug.security import generate_password_hash, check_password_hash
import shutil
import os
# Importação das funções otimizadas do database.py
from database import init_db, query_data, process_excel_sites, process_excel_escala

# Cria a pasta de uploads temporários
os.makedirs("uploads", exist_ok=True)

app = FastAPI(title="NetQuery Terminal")

# Chave de sessão para manter o Victor logado
app.add_middleware(SessionMiddleware, secret_key="victor_sistecom_2026_safe_key")
templates = Jinja2Templates(directory="templates")

# Inicializa as tabelas e migrações do PostgreSQL
init_db()

# Credenciais do Victor Henrique de Oliveira
ADMIN_USER = "81032045"
ADMIN_PASS_HASH = generate_password_hash("Py@thon26!")

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# Rota para o UptimeRobot não dar erro
@app.head("/ping")
@app.get("/ping")
async def health_check():
    return {"status": "Sistemas Operacionais"}

@app.post("/query")
async def ask_query(request: Request):
    try:
        data = await request.json()
        question = data.get("question", "")
        # A busca agora é inteligente: identifica DDD e filtra folgas automaticamente
        answer = query_data(question)
        return JSONResponse({"answer": answer})
    except Exception as e:
        return JSONResponse({"answer": f"Erro interno: {str(e)}"})

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
async def login_post(request: Request, username: str = Form(...), password: str = Form(...)):
    if username == ADMIN_USER and check_password_hash(ADMIN_PASS_HASH, password):
        request.session["user"] = username
        return RedirectResponse(url="/admin", status_code=303)
    return templates.TemplateResponse("login.html", {"request": request, "error": "Acesso negado."})

@app.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request):
    if not request.session.get("user"):
        return RedirectResponse(url="/login", status_code=303)
    return templates.TemplateResponse("admin.html", {"request": request})

# Rota de Upload com Seletor de Tipo (Sites ou Escala)
@app.post("/upload/{tipo}")
async def upload_file(request: Request, tipo: str, file: UploadFile = File(...)):
    if not request.session.get("user"):
        return RedirectResponse(url="/login", status_code=303)
    
    file_path = f"uploads/{file.filename}"
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        if tipo == "sites":
            process_excel_sites(file_path)
            msg = "Base de SITES (Aba Padrao) atualizada com sucesso!"
        elif tipo == "escala":
            process_excel_escala(file_path)
            msg = "ESCALA MENSAL (Abas DDD) consolidada com sucesso!"
        else:
            raise Exception("Tipo de upload inválido.")
        
        return templates.TemplateResponse("admin.html", {"request": request, "msg": msg})
    
    except Exception as e:
        return templates.TemplateResponse("admin.html", {"request": request, "error": f"Falha no processamento: {str(e)}"})
    
    finally:
        # Remove o rastro do arquivo no servidor Render
        if os.path.exists(file_path):
            os.remove(file_path)

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/", status_code=303)
