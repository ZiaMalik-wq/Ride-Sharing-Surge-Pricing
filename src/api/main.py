import json
import redis
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="Surge Pricing API",
    description="Real-time surge pricing engine powered by Apache Flink and Redis",
    version="1.0.0"
)

# Allow Cross-Origin requests from any frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Connect to Redis
try:
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    r.ping()
except Exception as e:
    print(f"Error connecting to Redis: {e}")

@app.get("/")
def root():
    return {"status": "online", "message": "Surge Pricing API is running."}

@app.get("/surge")
def get_all_surge_prices():
    """
    Returns the current surge multipliers for all active zones.
    """
    try:
        keys = r.keys("surge:*")
        if not keys:
            return {"data": []}
            
        # Fetch all values in one go
        values = r.mget(keys)
        
        results = []
        for key, val in zip(keys, values):
            if val:
                results.append(json.loads(val))
                
        return {"data": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/surge/{zone_id}")
def get_surge_by_zone(zone_id: str):
    """
    Returns the current surge multiplier for a specific zone.
    Example: /surge/zone_1
    """
    try:
        val = r.get(f"surge:{zone_id}")
        if val:
            return {"data": json.loads(val)}
        else:
            return {"data": {"zone_id": zone_id, "surge_multiplier": 1.0, "message": "No active surge"}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
