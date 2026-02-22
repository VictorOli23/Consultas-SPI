from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from werkzeug.security import generate_password_hash, check_password_hash
import shutil
import os
# Importação corrigida com os nomes das novas funções
from database import init_db, query_data, process_excel_sites, process_excel_escala

os.makedirs("uploads", exist_ok=True)
app = FastAPI(title="NetQuery Operations")
app.add_middleware(SessionMiddleware, secret_key="chave_secreta_victor")
templates = Jinja2Templates(directory="templates")

# Inicializa o banco de dados PostgreSQL
init_db()

# Credenciais Admin (Victor Henrique de Oliveira)
ADMIN_USER = "81032045"
ADMIN_PASS_HASH = generate_password_hash("Py@thon26!")

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# Rota para manter o UptimeRobot ativo
@app.head("/ping")
@app.get("/ping")
async def health_check():
    return {"status": "Sistema Online"}

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
    return templates.TemplateResponse("login.html", {"request": request, "error": "Credenciais inválidas."})

@app.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request):
    if not request.session.get("user"):
        return RedirectResponse(url="/login", status_code=303)
    return templates.TemplateResponse("admin.html", {"request": request})

# Rota Dinâmica que recebe 'sites' ou 'escala'
@app.post("/upload/{tipo}")
async def upload_file(request: Request, tipo: str, file: UploadFile = File(...)):
    if not request.session.get("user"):
        return RedirectResponse(url="/login", status_code=303)
    
    file_path = f"uploads/{file.filename}"
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        # Chama a função correta baseada na URL do botão clicado
        if tipo == "sites":
            process_excel_sites(file_path)
            msg = "Base de SITES (Localidades) atualizada com sucesso!"
        elif tipo == "escala":
            process_excel_escala(file_path)
            msg = "Escala de PLANTÃO mensal atualizada com sucesso!"
        else:
            raise Exception("Tipo de upload inválido.")
        
        res = templates.TemplateResponse("admin.html", {"request": request, "msg": msg})
    except Exception as e:
        res = templates.TemplateResponse("admin.html", {"request": request, "error": f"Erro: {str(e)}"})
    
    if os.path.exists(file_path):
        os.remove(file_path)
    return res

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/", status_code=303)
