from fastapi import FastAPI, Request, Form, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from werkzeug.security import generate_password_hash, check_password_hash
import shutil
import os
from database import init_db, process_excel, query_data

# Cria diretórios necessários
os.makedirs("templates", exist_ok=True)
os.makedirs("uploads", exist_ok=True)

app = FastAPI(title="Consultas de Região")
app.add_middleware(SessionMiddleware, secret_key="super_secret_key_app_consultas")
templates = Jinja2Templates(directory="templates")

# Inicializa Base de Dados
init_db()

# Credenciais Admin com Encriptação de Palavra-passe (Hash)
ADMIN_USER = "81032045"
# Numa aplicação real, este hash estaria guardado na base de dados.
# Aqui geramos o hash da palavra-passe "Py@thon26!" no arranque da aplicação.
ADMIN_PASS_HASH = generate_password_hash("Py@thon26!")

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# Rota Health Check para o UptimeRobot (Monitorização)
@app.head("/ping")
@app.get("/ping")
async def health_check():
    return {"status": "Sistema Operacional e Rodando!"}

@app.post("/query")
async def ask_query(request: Request):
    data = await request.json()
    question = data.get("question", "").strip()
    if not question:
        return JSONResponse({"answer": "Por favor, digite alguma coisa."})
    
    answer = query_data(question)
    return JSONResponse({"answer": answer})

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
async def login_post(request: Request, username: str = Form(...), password: str = Form(...)):
    # Verificação segura comparando o hash encriptado
    if username == ADMIN_USER and check_password_hash(ADMIN_PASS_HASH, password):
        request.session["user"] = username
        return RedirectResponse(url="/admin", status_code=303)
    return templates.TemplateResponse("login.html", {"request": request, "error": "Credenciais inválidas ou acesso negado."})

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/", status_code=303)

@app.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    return templates.TemplateResponse("admin.html", {"request": request})

@app.post("/upload")
async def upload_file(request: Request, file: UploadFile = File(...)):
    user = request.session.get("user")
    if not user:
        return RedirectResponse(url="/login", status_code=303)

    if not (file.filename.endswith('.xls') or file.filename.endswith('.xlsx')):
        return templates.TemplateResponse("admin.html", {"request": request, "error": "Apenas ficheiros .xls e .xlsx são permitidos."})

    file_path = f"uploads/{file.filename}"
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        process_excel(file_path)
        msg = "Ficheiro importado e base de dados atualizada com sucesso!"
        error = None
    except Exception as e:
        msg = None
        error = f"Erro ao processar ficheiro: Verifique as colunas. (Detalhe: {str(e)})"

    if os.path.exists(file_path):
        os.remove(file_path)

    return templates.TemplateResponse("admin.html", {"request": request, "msg": msg, "error": error})
