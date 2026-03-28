from fastapi import FastAPI

app = FastAPI(title="Jellyfin Recommendation API")

@app.get("/")
def read_root():
    return {"message": "Hello from Serving Layer! 推荐系统 API 已启动。"}

@app.get("/recommend/{user_id}")
def get_recommendations(user_id: int):
    # 这是一个 Dummy 接口，返回假的推荐列表，供 Ze Xu 后续修改
    return {
        "user_id": user_id, 
        "recommendations": [101, 102, 103, 104, 105], 
        "status": "dummy_success"
    }