## Git

git init
git add readme.md
git add .gitignore
git commit -m "first commit"
git branch -M main
git remote add origin https://github.com/dqtrung1702/airflow-docker.git
git push -u origin main
git push -f origin main # Thêm flag -f để force push

## Thành phần

- Apache Airflow 2.9.0
- Redis broker cho Celery Executor
- PostgreSQL cho Airflow metadata
- Oracle Database Express Edition 21c

## Cách sử dụng

### Yêu cầu

### Cài đặt và chạy

1. docker
docker compose build
docker compose up -d
docker compose down -v
docker volume prune
docker system prune

2. Lệnh này sẽ lấy được địa chỉ IP của WSL 
`
ip addr show eth0 | grep -oP '(?<=inet\s)\d+(\.\d+){3}'
`
