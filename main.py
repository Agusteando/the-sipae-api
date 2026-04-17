from fastapi import FastAPI
from fastapi.responses import RedirectResponse

# Centralized imports
from core.config import settings
from modules.testhub.router import router as testhub_router
from modules.husky.router import router as husky_router
from modules.attendance.router import router as attendance_router

app = FastAPI(title="The SIPAE API Hub", version="1.3.0")

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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)