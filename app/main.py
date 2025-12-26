from fastapi import FastAPI

# Create the FastAPI app instance
app = FastAPI()

# Basic test route (homepage)
@app.get("/")
def read_root():
    return {"message": "Hello from FastAPI!"}

# ✅ Health check route
@app.get("/health")
def health_check():
    """
    Simple endpoint to verify that the API is running.
    """
    return {"status": "ok"}
