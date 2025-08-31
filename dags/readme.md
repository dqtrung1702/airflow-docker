### Giả định
```
Bạn đang chạy Docker trong WSL2
Các container được khởi chạy từ WSL2
Bạn muốn container trong Docker Compose kết nối tới một database PostgreSQL chạy trên WSL
```
> Cấu hình database trên host để chấp nhận kết nối từ container
- Chỉnh sửa file postgresql.conf
- Tìm file cấu hình (thường nằm ở /etc/postgresql/<version>/main/postgresql.conf).
- Conf cho phép PostgreSQL lắng nghe kết nối từ mọi địa chỉ IP: `listen_addresses = '*'`

- Chỉnh sửa file pg_hba.conf:
- Tìm file (thường ở /etc/postgresql/<version>/main/pg_hba.conf).
- Thêm dòng sau để cho phép kết nối từ container `host    all    all    <ip của container>    md5`

Lưu và khởi động lại PostgreSQL `sudo systemctl restart postgresql`
