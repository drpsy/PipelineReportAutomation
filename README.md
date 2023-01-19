# tla_thanh_pham


Luồng chạy cronjob:
 1. Gmail_attachment: Lấy các file excel từ khách hàng 
 2. Component db dump: Đẩy dữ liệu từ các file excel lấy được vào trong database
 3. Tự động gửi mail .
 4. Thiết lập cronjob: cronjob.py
 - lấy file excel: một ngày một lần
 - Gửi mail: Tùy thời gian - 5 ngày 1 lần
 
Để cài đặt các chương trình cần thiết:
- pip install -r requirements.txt

Để chạy cronjob:
- python cronjob.py
 
