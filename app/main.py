# app/main.py
from fastapi import FastAPI
from .api.endpoints import user, auth, versions
from .core.config import settings
from .api.endpoints import enhanced_versions

app = FastAPI(title=settings.PROJECT_NAME)



app.include_router(versions.router)
app.include_router(user.router)
app.include_router(auth.router)
app.include_router(enhanced_versions.router)


@app.get("/")
def root():
    return {"message": "Welcome 2 Data Versioning"}
