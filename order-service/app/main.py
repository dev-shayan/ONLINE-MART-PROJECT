from fastapi import FastAPI


app = FastAPI()


@app.get("/")
async def read_root():
    return {"Hello": "from Order Service"}