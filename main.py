from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware

# Centralized imports
from core.config import settings
from modules.testhub.router import router as testhub_router
from modules.husky.router import router as husky_router
from modules.attendance.router import router as attendance_router
from modules.employee_attendance.router import router as employee_router

app = FastAPI(title="The SIPAE API Hub", version="1.4.0")

# ==========================================
# CORS CONFIGURATION
# ==========================================
# Permite a los clientes web (como Nuxt en localhost:3000 o producción) 
# consumir la API sin ser bloqueados por la política de seguridad del navegador.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite solicitudes desde cualquier origen
    allow_credentials=True,
    allow_methods=["*"],  # Permite todos los métodos HTTP (GET, POST, OPTIONS, etc.)
    allow_headers=["*"],  # Permite todos los headers (Authorization, Content-Type, etc.)
)

@app.get("/", include_in_schema=False)
async def redirect_root_to_hub():
    """Automatically redirect the base URL to our Test Hub."""
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