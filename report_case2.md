


## 1. `simple_dag_local.py`
- DAG đơn giản với 3 task: `start`, `process`, `end`.
- Mỗi task sử dụng `PythonOperator` để in ra thông báo tương ứng.
- Mục tiêu là minh họa cấu trúc cơ bản của một DAG và cách các task được kết nối tuần tự.

---

## 2. `complex_dag_local.py`
- DAG mô phỏng quy trình xử lý dữ liệu và huấn luyện mô hình học máy.
- Gồm các task: `start`, `extract_data`, `transform_data`, `load_data`, `train_model`, `evaluate_model`, `end`.
- Cho thấy cách thiết kế một pipeline có nhánh và nhiều bước xử lý phức tạp hơn.

---

## 3. `sensor_local.py`
- Sử dụng `ExternalTaskSensor` để chờ một DAG khác hoàn thành trước khi thực hiện task tiếp theo.
- Gồm các task: `wait_for_external_dag` và `process_after_wait`.
- Minh họa cách đồng bộ hóa giữa các DAG trong Airflow.

---

## 4. `miai_dag.py`
- DAG chính mô phỏng quy trình xử lý dữ liệu AI thực tế.
- Gồm các task: `start`, `crawl_data`, `preprocess_data`, `train_model`, `evaluate_model`, `deploy_model`, `end`.
- Mô phỏng toàn bộ vòng đời dự án AI: từ thu thập dữ liệu đến triển khai mô hình.

---


1. Cài đặt Apache Airflow theo hướng dẫn chính thức.
2. Đặt các file `.py` vào thư mục `dags/` trong thư mục cài đặt Airflow.
3. Khởi động `airflow scheduler` và `airflow webserver`.
4. Truy cập tại [http://localhost:8080](http://localhost:8080) để theo dõi và chạy các DAG.
5. 3 task đầu là ví dụ để hiểu airflow ở mức cơ bản, Task 4 sẽ cào dữ liệu chứng khoáng từ trang vndirect từ ngày 1/1/2000
6. Sau khi dữ liệu cào được sẽ được lưu vào máy local và gửi từ gmail 1 tới gmail2 vào 3g chiều mỗi ngày (gmail cài đặt trong file miai_dag.py)
