from fastapi import FastAPI, Depends, HTTPException, Response
from fastapi.security import OAuth2PasswordBearer
from fastapi.responses import JSONResponse
from db import add_product, delete_product, email_schedule
import uvicorn
from models import Product

app = FastAPI()

@app.post("/addProduct", status_code=201)
async def AddProduct(product: Product):
    ret = add_product(product)
    if 'Error' in ret:
        raise HTTPException(status_code=404, detail=ret['Error'])
    return JSONResponse(content=ret)

@app.delete("/deleteProduct/{customerId}/{productName}/{domain}", status_code=200)
async def DeleteProduct(customerId: str, productName:str, domain:str):
    ret = delete_product(customerId, productName, domain)
    if 'Error' in ret:
        raise HTTPException(status_code=404, detail=ret['Error'])
    return JSONResponse(content=ret)

@app.get("/emailSchedule/", status_code=200)
async def EmailSchedule():
    ret = email_schedule()
    if 'Error' in ret:
        raise HTTPException(status_code=404, detail=ret['Error'])
    return JSONResponse(content=ret)

if __name__ == '__main__':
    uvicorn.run(app, port=8080, host='0.0.0.0')