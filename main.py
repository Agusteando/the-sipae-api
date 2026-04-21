from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

# Importaciones Centralizadas
from core.config import settings
from core.scheduler import start_scheduler
from modules.testhub.router import router as testhub_router
from modules.husky.router import router as husky_router
from modules.attendance.router import router as attendance_router
from modules.employee_attendance.router import router as employee_router

# ==========================================
# LIFESPAN & SCHEDULER
# ==========================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Arrancar pre-cómputo y tareas de caché en segundo plano al iniciar
    start_scheduler()
    yield
    # Lógica de cierre futuro (si es necesario)

app = FastAPI(title="The SIPAE API Hub", version="1.5.0", lifespan=lifespan)

# ==========================================
# CORS CONFIGURATION
# ==========================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],  
    allow_headers=["*"],  
)

@app.get("/", include_in_schema=False)
async def redirect_root_to_hub():
    """Redirige automáticamente la raíz a nuestro Panel de Pruebas (Test Hub)."""
    return RedirectResponse(url="/test-hub")

# ==========================================
# ROUTER REGISTRATION
# ==========================================
app.include_router(testhub_router)
app.include_router(husky_router)
app.include_router(attendance_router)
app.include_router(employee_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)