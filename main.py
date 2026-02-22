from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from werkzeug.security import generate_password_hash, check_password_hash
import shutil
import os
# Importação das funções atualizadas no database.py
from database import init_db, query_data, process_excel_sites, process_excel_escala

# Garante que a pasta de uploads existe
os.makedirs("uploads", exist_ok=True)

app = FastAPI(title="NetQuery Operations")
# Chave de sessão para o login do Victor
app.add_middleware(SessionMiddleware, secret_key="victor_sistecom_2026")
templates = Jinja2Templates(directory="templates")

# Inicializa as tabelas no PostgreSQL
init_db()

# Credenciais de Acesso (Dados do Usuário)
ADMIN_USER = "81032045"
ADMIN_PASS_HASH = generate_password_hash("Py@thon26!")

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# Mantém o UptimeRobot ativo sem erro 405
@app.head("/ping")
@app.get("/ping")
async def health_check():
    return {"status": "Sistemas Online"}

@app.post("/query")
async def ask_query(request: Request):
    data = await request.json()
    question = data.get("question", "")
    answer = query_data(question)
    return JSONResponse({"answer": answer})

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
    if not request.session.get("user"):
        return RedirectResponse(url="/login", status_code=303)
    return templates.TemplateResponse("admin.html", {"request": request})

# Rota de Upload Duplo: identifica se é 'sites' ou 'escala'
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
            msg = "Base de Localidades atualizada!"
        elif tipo == "escala":
            process_excel_escala(file_path)
            msg = "Escala de Plantão atualizada com sucesso!"
        else:
            raise Exception("Tipo de arquivo desconhecido.")
        
        res = templates.TemplateResponse("admin.html", {"request": request, "msg": msg})
    except Exception as e:
        res = templates.TemplateResponse("admin.html", {"request": request, "error": f"Erro no processamento: {str(e)}"})
    
    if os.path.exists(file_path):
        os.remove(file_path)
    return res

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/", status_code=303)
