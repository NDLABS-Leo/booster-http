# booster-http

## 构建服务
```
make
```

## 设置环境变量
```bash
export PORT="8080"
```

## 启动服务
```bash
./booster-http
```

## 测试接口
```bash
curl -v http://127.0.0.1:8080/ipfs/bafykbzacedm76mm2cuojttw3micctnsy5cxrug4dcxqyef4owbyqubjkjxx4e?dag-scope=block
```

### 响应情况
- **200 OK**：返回指定 CAR 文件内容。
- **404 Not Found**：pieceCid 不存在于数据库中。
- **500 Internal Server Error**：文件或数据库读取时出现其他错误。
