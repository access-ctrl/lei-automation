from fastapi import FastAPI, Header, HTTPException, BackgroundTasks
import os
import threading
from dotenv import load_dotenv
import enricher_playwright
from enricher_playwright import main as run_enrichment_logic

load_dotenv()

app = FastAPI(title="Lead Enrichment API")

# Configuration
API_SERVER_KEY = os.getenv("API_SERVER_KEY", "your-default-secret-key")
is_running = False
last_error = None
lock = threading.Lock()

@app.get("/api/v1/status")
async def health_check():
    return {
        "status": "online", 
        "is_running": is_running,
        "last_error": last_error
    }

@app.get("/api/v1/logs")
async def get_logs(lines: int = 50, x_api_key: str = Header(None)):
    """
    Returns the last N lines of the most recent log file.
    """
    if x_api_key != API_SERVER_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
        
    log_files = sorted([f for f in os.listdir(".") if f.startswith("research_") and f.endswith(".log")], reverse=True)
    if not log_files:
        return {"message": "No log files found."}
    
    latest_log = log_files[0]
    try:
        with open(latest_log, "r") as f:
            content = f.readlines()
            return {
                "file": latest_log,
                "logs": content[-lines:]
            }
    except Exception as e:
        return {"error": f"Could not read log file: {e}"}

def run_task():
    global is_running, last_error
    with lock:
        if is_running:
            return
        is_running = True
        last_error = None
        enricher_playwright.STOP_REQUESTED = False # Reset signal
    
    try:
        run_enrichment_logic()
    except Exception as e:
        last_error = str(e)
    finally:
        with lock:
            is_running = False

@app.post("/api/v1/run")
async def trigger_enrichment(background_tasks: BackgroundTasks, x_api_key: str = Header(None)):
    if x_api_key != API_SERVER_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if is_running:
        return {"status": "error", "message": "Already running."}
    background_tasks.add_task(run_task)
    return {"status": "success", "message": "Enrichment started."}

@app.post("/api/v1/stop")
async def stop_enrichment(x_api_key: str = Header(None)):
    """
    Sends a stop signal to the enrichment script.
    """
    if x_api_key != API_SERVER_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    if not is_running:
        return {"status": "error", "message": "Enrichment is not currently running."}
    
    enricher_playwright.STOP_REQUESTED = True
    return {"status": "success", "message": "Stop signal sent. Script will finish the current lead and then exit."}

@app.post("/api/v1/rotate-serper")
async def manual_rotate_serper(x_api_key: str = Header(None)):
    """
    Manually forces the Serper API key to rotate to the next one in the list.
    """
    if x_api_key != API_SERVER_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    # We call the rotation logic directly in the module
    old_index = enricher_playwright.current_serper_key_index
    if enricher_playwright.rotate_serper_key(is_manual=True):
        new_index = enricher_playwright.current_serper_key_index
        return {
            "status": "success", 
            "message": f"Manually switched from Serper Key #{old_index + 1} to Key #{new_index + 1}"
        }
    else:
        return {
            "status": "error", 
            "message": "No more Serper keys available to rotate to."
        }

    """
    Triggers the lead enrichment script in the background.
    Requires 'X-API-Key' header matching API_SERVER_KEY in .env
    """
    if x_api_key != API_SERVER_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid API Key")

    if is_running:
        return {"status": "error", "message": "Enrichment is already in progress."}

    background_tasks.add_task(run_task)
    return {"status": "success", "message": "Enrichment started in background."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
