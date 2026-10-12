# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Bùi Việt Anh` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602611` |
| GitHub | `VietAnh-AI2-UET` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

**Khác biệt:**

- Khác số record: drive_a 3021 bản, drive_b 3234 bản
- Khác format timestamp: drive_a dùng utc, drive_b dùng ms
- Khác đơn vị đo vận tốc: drive_a dùng mps, b dùng kmh
- drive_a dùng gas, b dùng throttle_pct

**Lý do không ép kiểu ngay:**

- Vì không phải trường nào chứa số cũng là số
- Ép kiểu có thể làm mất thông tin
- Có những ô không có giá trị
- Bronze chỉ nên lưu giá trị thô, việc xử lý sẽ ở Silver

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

**Cách Pipeline đưa hai nguồn về một schema:**

- Chuẩn hóa:
    - Đơn vị: tốc độ B chia 3.6 để đổi km/h → m/s; ga/phanh B chia 100 để về khoảng 0–1 như A.
    - Thời gian: A từ chuỗi ngày giờ, B từ số mili giây; cả hai chuyển thành kiểu thời gian có múi giờ.
    - Độ lệch đồng hồ: B trừ telemetry_clock_offset_ms trước khi chuyển thời gian để khớp với video; A không cần điều chỉnh.

- Đưa về cùng 1 bảng `silver_telemetry`:
    - Nếu record đó hợp lệ
    - Phân định nguồn bằng `drive_id`

**Bằng chứng:**
- Độ lệch đồng hồ:
    ![Thời gian bắt đầu của drive_b sau khi chuẩn hóa](image.png)
    - Thời gian bắt đầu camera trong data/sources.json: 2025-10-08T14:30:00+00:00
    - Pipeline đã trừ đi một lượng telemetry_clock_offset_ms = 1500

- Các trường khác được chuyển về cùng một đơn vị:
    ![bảng silver.silver.telemetry](image-2.png)

**Bảng quarantine:**
    ![quarantine](image-3.png)

**Tại sao dòng lỗi vào reject chứ không phải xóa:**
- Để không làm mất dữ liệu, có thể kiểm tra nguyên nhân và xử lý lại khi sửa quy tắc. Đồng thời, bảng silver sạch chỉ giữ các dòng hợp lệ để phân tích.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

(trả lời ở đây)

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

(trả lời ở đây)

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | | |
| 2 | | |
| 6 | | |

(trả lời ở đây)

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
