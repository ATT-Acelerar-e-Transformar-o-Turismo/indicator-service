from fastapi import FastAPI
from routes import router as api_router

app = FastAPI()

app.include_router(api_router)

# Command to run the application:
# uvicorn app.main:app --host 0.0.0.0 --port 8000
