# Báo cáo Chuyên sâu: Hệ thống NBA Big Data Analytics

Bản báo cáo này mô tả toàn bộ vòng đời dữ liệu của dự án NBA Big Data Analytics, từ lúc nạp dữ liệu thô (Ingestion) cho đến lúc kết xuất trên các biểu đồ (Visualization).

## PHẦN 1: QUY TRÌNH ETL (Data Processing Pipeline)

Dự án xử lý dữ liệu Play-by-Play (PbP) của hơn 25 mùa giải NBA bằng **Apache Spark** theo mô hình ELT/ETL hiện đại.

### Bước 1: Ingestion & Schema Definition

- Đọc trực tiếp các file CSV thô. Thay vì để Spark tự động nội suy kiểu dữ liệu (gây tốn I/O), hệ thống định nghĩa sẵn `StructType` Schema từ đầu.
- Cấu hình `spark.sql.shuffle.partitions` được tinh chỉnh để tránh out-of-memory khi xử lý hàng triệu dòng dữ liệu.

### Bước 2: Event Cleaning & Normalization (`clean_events.py`)

- **Tọa độ (Coordinates):** Chuẩn hóa giá trị X, Y. Nếu sự kiện không phải là ném rổ (Ví dụ: Timeout, Foul), loại bỏ X, Y về `Null` để không làm nhiễu Shot Chart.
- **Thời gian (Clock):** Quy đổi định dạng chuỗi `PT12M00.00S` thành số giây đếm ngược `seconds_remaining`.
- **Tái tạo Điểm số:** Sử dụng Spark Window Functions (`max` theo từng game) để Forward-fill giá trị điểm của trận đấu vào từng sự kiện nhỏ. Tính `score_margin` (Cách biệt điểm số).
- **Gắn cờ Clutch Time (`is_clutch`):** Lần theo thời gian và điểm số để đánh dấu `is_clutch = 1` cho mọi sự kiện diễn ra ở **Hiệp 4 hoặc Hiệp phụ (OT), dưới 5 phút, và cách biệt điểm số $\leq$ 5**. Việc gắn cờ từ bước này đảm bảo Clutch Mode được áp dụng nhất quán trên toàn hệ thống.

### Bước 3: Possession Reconstruction (`reconstruct_possessions.py`)

Dữ liệu NBA thô **không có định nghĩa "Pha bóng" (Possession)**. Đây là kỹ thuật Big Data khó nhất trong đồ án:

- Sử dụng hàm `lag()` và `last()` của Window Functions để lướt qua luồng sự kiện.
- Bất cứ khi nào "Đội đang giữ bóng" thay đổi hoặc hết hiệp, hệ thống tự động chốt lại một pha bóng (`is_new_possession = 1`).
- Tính toán tổng thời gian diễn ra pha bóng, có bị mất bóng (Turnover) hay ghi được điểm (Score) hay không. `is_clutch` cũng được lưu lại cho pha bóng đó.

### Bước 4: Metrics Aggregation (`build_team_metrics.py`, `build_player_metrics.py`)

- **Lưu trữ Raw Counts:** Thay vì tính chết tỷ lệ TS% hay ORtg bằng Spark, hệ thống tiến hành **cộng dồn dữ liệu đếm thô (Raw Counts)** như: `total_possessions`, `points_scored`, `total_fga`, `fgm`... theo từng nhóm `(season, team/player, is_clutch)`.
- Điều này cung cấp nền tảng vững chắc cho lớp giao diện để linh hoạt tổng hợp dữ liệu thời gian thực.
- **Tính DRtg:** Sử dụng phép Self-Join (nội kết) trên các trận đấu để tự động xác định "Đối thủ" (Defending Team) của pha bóng đó, từ đó thống kê số liệu `points_allowed` (Điểm bị ghi) và số pha phòng ngự.

### Bước 5: Parquet Storage

Ghi kết quả thành các bảng Fact Tables (`fact_shots`, `fact_possessions`, `fact_team_metrics`, `fact_player_metrics`). Tất cả đều được **Partition (phân vùng) theo `season`** dưới định dạng Parquet để DuckDB có thể truy vấn cực nhanh nhờ cơ chế đọc bỏ qua phân vùng (Partition Pruning).

---

## PHẦN 2: TÍNH TOÁN DYNAMIC BẰNG DUCKDB & Ý NGHĨA CÁC CHỈ SỐ

Mọi công thức phân tích giờ đây được DuckDB tính toán on-the-fly (thời gian thực) ngay khi người dùng gạt Toggle **Clutch Mode**.

### 1. Offensive Rating (ORtg) & Defensive Rating (DRtg)

- **Công thức tính (DuckDB):**
  - `ORtg = (SUM(points_scored) / SUM(total_possessions)) * 100`
  - `DRtg = (SUM(points_allowed) / SUM(def_possessions)) * 100`
- **Ý nghĩa:** Đo lường hiệu quả tuyệt đối. Số điểm đội ghi được (hoặc bị ghi) trên mỗi 100 pha bóng. Nó công bằng hơn số điểm ghi được trung bình mỗi trận, vì loại bỏ sự thiên lệch nếu đội chơi nhịp độ nhanh (có nhiều pha bóng) hay chậm (ít pha bóng).

### 2. True Shooting Percentage (TS%)

- **Công thức:** `TS_pct = PTS / (2 * (FGA + 0.44 * FTA))`
- **Ý nghĩa:** Tỷ lệ ném rổ thực tế. Khác với FG% thông thường, TS% tính luôn cả giá trị của quả ném 3 điểm (mang lại 3 điểm thay vì 2) và cả những quả ném phạt (Free Throws).

### 3. Effective Field Goal Percentage (eFG%)

- **Công thức:** `eFG_pct = (FGM + 0.5 * 3PM) / FGA`
- **Ý nghĩa:** Đo lường độ sắc bén của các cú ném "sống" trên sân (bỏ qua ném phạt), trong đó cộng thêm giá trị thưởng 50% cho quả 3 điểm.

---

## PHẦN 3: Ý NGHĨA CỦA CÁC VISUALIZATION TRÊN DASHBOARD

### 1. Team ORtg vs DRtg Scatter Plot

- **Mô tả:** Biểu đồ phân tán với trục X là ORtg (Sức mạnh Tấn công) và trục Y là DRtg (Sức mạnh Phòng thủ). Trục Y bị đảo ngược vì DRtg càng thấp nghĩa là phòng thủ càng xuất sắc.
- **Ý nghĩa phân tích:** Trực quan hóa "Sức mạnh tổng hợp" của một đội bóng.
  - **Góc trên bên phải:** Đội tinh hoa (Elite), thủng lưới ít, ghi bàn nhiều. Các nhà vô địch thường nằm ở vùng này.
  - **Góc trên bên trái:** Phòng thủ kiên cố nhưng tấn công bế tắc.
  - **Góc dưới bên phải:** Hàng công hủy diệt nhưng hàng thủ dễ thủng lưới.
  - **Góc dưới bên trái:** Đội bóng yếu kém toàn diện.
- Khi người dùng lọc range nhiều năm, hệ thống tự động tính **Trung bình cộng (Mean)** để chấm duy nhất 1 điểm đại diện cho giai đoạn đó, tránh gây nhiễu biểu đồ.

### 2. Shot Profile Evolution (Stacked Bar Chart)

- **Mô tả:** Biểu đồ cột chồng hiển thị tỷ trọng phân bổ các cú ném ở 3 vùng: Cận rổ (Rim), Trung bình (Midrange), Ném 3 (3PT) thay đổi ra sao qua các mùa giải.
- **Ý nghĩa phân tích:** Cho thấy sự "Tiến hóa" trong triết lý thi đấu của một cầu thủ hoặc cả hệ thống. Rất hiệu quả để minh chứng cho cuộc cách mạng "Pace & Space", khi vùng Midrange ngày càng bị loại bỏ để nhường chỗ cho những quả ném 3 điểm.

### 3. Current Efficiency Metrics (Grouped Bar Chart)

- **Mô tả:** Biểu đồ hiển thị 3 cột kề nhau biểu diễn `TS%`, `eFG%`, và tỷ lệ ném 3 thành công `3PT%`.
- **Ý nghĩa phân tích:** Việc dùng biểu đồ cột thay cho các con số đơn điệu giúp so sánh ngay lập tức độ bén của siêu sao so với ngưỡng chuẩn của NBA (Ví dụ: TS% > 60% được coi là xuất chúng). Cực kỳ hữu dụng để đối chiếu hiệu suất của một cầu thủ trong giai đoạn bình thường so với khi gạt **Clutch Mode**.

### 4. Player/Team Shot Chart (Interactive Scatter)

- **Mô tả:** Thể hiện tọa độ thực tế của từng cú ném đè lên hình ảnh sa bàn nửa sân bóng rổ.
- **Ý nghĩa phân tích:** Công cụ quét (scan) sát thủ trên mặt sân. Bằng bộ lọc Toggles (All/Made/Missed), biểu đồ giúp chỉ ra đâu là những "vùng an toàn" (Hot Zones) mà cầu thủ có tỷ lệ dứt điểm cao nhất, và những "vùng chết" (Cold Zones). Khi kết hợp tính năng **Clutch Mode**, ta sẽ đoán biết được trong những giây cuối trận cầu sinh tử, quả bóng sẽ được siêu sao ném ở góc nào.

---

## PHẦN 4: KIẾN TRÚC CÔNG NGHỆ & HIỆU NĂNG (TECH STACK & BENCHMARKS)

### 1. Cấu trúc Công nghệ (Tech Stack) và Lý do lựa chọn
- **Apache Spark (PySpark):** Đóng vai trò là cỗ máy ETL (Trích xuất, Làm sạch, Nạp). Dữ liệu Play-by-Play của 26 năm lên tới hàng chục triệu sự kiện. Việc dùng thư viện Pandas truyền thống sẽ gây tràn RAM (Out-of-memory). PySpark giải quyết bài toán này nhờ cơ chế Distributed Computing (xử lý phân tán trên nhiều lõi CPU).
- **Định dạng Parquet:** Được dùng để lưu trữ dữ liệu sau khi làm sạch. Là dạng lưu trữ hình cột (Columnar Storage) tích hợp nén cao, cực kỳ tối ưu cho bài toán OLAP vì nó cho phép hệ thống chỉ đọc những cột cần thiết thay vì lướt qua toàn bộ dòng.
- **DuckDB:** Đóng vai trò là Query Engine (Serving Layer). Thay vì phải dựng lên một server CSDL cồng kềnh (PostgreSQL, MySQL), DuckDB là CSDL In-Memory siêu tốc độ, có thể thực thi SQL trực tiếp lên các file Parquet với tốc độ dưới micro-giây.
- **Streamlit & Plotly:** Nền tảng xây dựng Dashboard UI và đồ thị tương tác cao, giúp trình bày dữ liệu mượt mà, chuyên nghiệp.

### 2. Thông số Hiệu năng (Data Scale & System Performance)

| Metric | Value |
| :--- | :--- |
| **Kỷ nguyên dữ liệu (Seasons)** | 1997 – 2026 (29 năm) |
| **Số lượng trận đấu (Games)** | ~35,000+ trận |
| **Quy quy mô dữ liệu (Events)** | ~18 Triệu+ sự kiện |
| **Thời gian chạy ETL (Spark)** | ~4 phút (Xử lý toàn bộ lịch sử) |
| **Tốc độ truy vấn UI (DuckDB)** | < 150 ms / biểu đồ |
| **Độ trễ lọc Clutch Mode** | Real-time (Tức thì) |

---

## PHẦN 5: TRẢI NGHIỆM NGƯỜI DÙNG (UX) VÀ DATA MODELING TRADE-OFFS

### 1. Phân tích So sánh Đa chiều (Multi-Team & League Benchmarking)
- **Global Multiselect (Team Dashboard):** Hệ thống được thiết kế hợp nhất (Unified UX). Chỉ với một thanh chọn đội duy nhất ở đầu trang, toàn bộ bảng điều khiển sẽ biến đổi đồng bộ. Khi chọn nhiều đội (VD: BOS, LAL, GSW), biểu đồ *Pace Evolution* sẽ vẽ các đường rượt đuổi nhau, *Offensive Radar* sẽ chồng các lớp đa giác (Overlay) để so sánh điểm mạnh yếu, và *Shot Heatmap* sẽ gộp chung tọa độ ném của toàn bộ đội được chọn thành một bản đồ nhiệt khổng lồ để tìm ra xu hướng ném rổ của một nhóm thế hệ.
- **Contextual Benchmarking (Player Dashboard):** Mọi biểu đồ đánh giá cá nhân (Efficiency Bars, Career Curves) đều được ép thêm lớp dữ liệu **League Average (Trung bình giải đấu)** bằng các đường đứt nét màu xám. Điều này giúp người xem đánh giá được chính xác cầu thủ này đang thực sự vươn tầm hay chỉ là "ếch ngồi đáy giếng".

### 2. Sự đánh đổi giữa Pre-aggregation và Dynamic OLAP
Trong thiết kế Data Warehouse cho Big Data, việc tính toán tức thì trên 15 triệu sự kiện đòi hỏi sự phân chia nhiệm vụ chiến lược:
- **Tối ưu tốc độ (Pre-aggregation):** Đối với các chỉ số tính toán phức tạp đòi hỏi Join nhiều bảng (như DRtg, Pace), dữ liệu được cộng dồn (aggregated) sẵn ở tầng Spark ETL dựa trên định nghĩa Clutch tiêu chuẩn của NBA (5 phút cuối trận, cách biệt $\leq$ 5 điểm). Nhờ vậy, `fact_team_metrics` trở nên siêu nhỏ gọn, giúp biểu đồ tải mượt mà trong chớp mắt.
- **Tối đa linh hoạt (On-the-fly Calculation):** Đối với biểu đồ **Shot Chart** và **Shot Profile**, hệ thống giữ lại các cột thô `seconds_remaining` và `score_margin` vào bảng `fact_shots`. Cho phép hiển thị thêm thanh trượt (Sliders) để người dùng tự do định nghĩa khái niệm "Clutch" (VD: 10 giây cuối, cách biệt 1 điểm). DuckDB sẽ lãnh ấn quét trực tiếp hàng triệu tọa độ thô, mang lại khả năng tùy biến cực độ cho phân tích chiến thuật chi tiết.
