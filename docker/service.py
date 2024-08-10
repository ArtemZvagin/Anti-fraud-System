import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from pydantic import BaseModel

from model_functions import load_model, get_prob




class Transaction(BaseModel):
    transaction_id: int
    tx_datetime: str
    customer_id: int
    terminal_id: int
    tx_amount: float
    tx_time_seconds: int
    tx_time_days: int



@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load the ML model
    print('Load model')
    load_model()
    
    yield
    
    


app = FastAPI(lifespan=lifespan)

@app.get('/')
async def check():
    return 'OK'

@app.post('/predict')
async def predict(transaction: Transaction):
    transaction = transaction.model_dump()
    
    prob = get_prob(transaction)
    return prob
    


def main():
    uvicorn.run(app, host="0.0.0.0", port=8080)

if __name__ == '__main__':
    main()