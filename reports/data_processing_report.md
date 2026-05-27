# Báo cáo Chuyên sâu: Hệ thống NBA Big Data Analytics

Bản báo cáo này mô tả toàn bộ vòng đời dữ liệu của dự án NBA Big Data Analytics, từ lúc nạp dữ liệu thô (Ingestion) cho đến lúc kết xuất trên các biểu đồ (Visualization).

> Xem sơ đồ kiến trúc và ER Diagram tại file [diagrams.md](./diagrams.md)

## PHẦN 1: QUY TRÌNH ETL (Data Processing Pipeline)

Dự án xử lý dữ liệu Play-by-Play (PbP) của 29 mùa giải NBA bằng **Apache Spark** theo mô hình ELT/ETL hiện đại.

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

### Bước 5: Lưu trữ dữ liệu (Data Storage Architecture)

**Dự án KHÔNG sử dụng cơ sở dữ liệu truyền thống** (Không có PostgreSQL, MySQL hay MongoDB). Thay vào đó, kiến trúc lưu trữ hoàn toàn dựa trên **Parquet Data Lake** — một mô hình phổ biến trong các hệ thống Big Data hiện đại (Databricks, AWS Athena, Google BigQuery).

Toàn bộ dữ liệu sau ETL được ghi thành 4 bảng Fact Tables dưới dạng file Parquet, **phân vùng (Partitioned) theo `season`**:

```
data/processed/
├── fact_shots/
│   ├── season=1997/
│   ├── season=1998/
│   └── ...season=2026/
├── fact_possessions/
│   ├── season=1997/
│   └── ...
├── fact_team_metrics/
│   ├── season=1997/
│   └── ...
└── fact_player_metrics/
    ├── season=1997/
    └── ...
```

**Lý do không dùng Database truyền thống:**

- Parquet là định dạng **lưu trữ cột (Columnar)** với nén Snappy/Gzip tích hợp, giảm dung lượng lưu trữ tới 80% so với CSV.
- DuckDB có khả năng **đọc trực tiếp Parquet** mà không cần import/load vào database, giúp loại bỏ hoàn toàn chi phí vận hành một DBMS Server.
- Cơ chế **Partition Pruning**: Khi người dùng lọc `season >= 2020`, DuckDB chỉ đọc các thư mục `season=2020/` đến `season=2026/`, bỏ qua hoàn toàn 23 thư mục còn lại → tốc độ truy vấn tăng gấp nhiều lần.

---

## PHẦN 1.5: SCHEMA CỦA CÁC BẢNG DỮ LIỆU (DATA DICTIONARY)

### Bảng 1: `fact_shots` — Dữ liệu từng cú ném rổ

| Cột                 | Kiểu   | Mô tả                                                                |
| :------------------ | :----- | :------------------------------------------------------------------- |
| `shooter`           | STRING | Tên cầu thủ ném rổ                                                   |
| `team`              | STRING | Viết tắt đội bóng (VD: `BOS`, `LAL`)                                 |
| `x`                 | FLOAT  | Tọa độ X trên sân (đã chuẩn hóa)                                     |
| `y`                 | FLOAT  | Tọa độ Y trên sân (đã chuẩn hóa)                                     |
| `shot_zone`         | STRING | Vùng ném: `Restricted Area`, `Midrange`, `Corner 3`, `Above Break 3` |
| `made_flag`         | INT    | `1` = ghi điểm, `0` = trượt                                          |
| `shot_value`        | INT    | Giá trị cú ném: `2` hoặc `3`                                         |
| `season`            | INT    | Mùa giải (partition key)                                             |
| `is_clutch`         | INT    | `1` = pha bóng sinh tử (Clutch), `0` = bình thường                   |
| `period`            | INT    | Hiệp đấu (1-4, 5+ = OT)                                              |
| `seconds_remaining` | FLOAT  | Số giây còn lại trong hiệp                                           |
| `score_margin`      | INT    | Cách biệt điểm số tại thời điểm ném                                  |

### Bảng 2: `fact_possessions` — Dữ liệu từng pha bóng đã tái cấu trúc

| Cột             | Kiểu   | Mô tả                           |
| :-------------- | :----- | :------------------------------ |
| `possession_id` | STRING | ID duy nhất của pha bóng        |
| `gameid`        | STRING | ID trận đấu                     |
| `team`          | STRING | Đội đang giữ bóng               |
| `points_scored` | INT    | Số điểm ghi được trong pha bóng |
| `is_turnover`   | INT    | `1` = mất bóng, `0` = không     |
| `is_clutch`     | INT    | `1` = Clutch, `0` = bình thường |

### Bảng 3: `fact_team_metrics` — Chỉ số đội tổng hợp (Raw Counts)

| Cột                 | Kiểu   | Mô tả                                |
| :------------------ | :----- | :----------------------------------- |
| `team`              | STRING | Viết tắt đội bóng                    |
| `season`            | INT    | Mùa giải (partition key)             |
| `is_clutch`         | INT    | `1` = Clutch, `0` = bình thường      |
| `games_played`      | INT    | Số trận đã chơi                      |
| `total_possessions` | INT    | Tổng số pha bóng tấn công            |
| `points_scored`     | INT    | Tổng điểm ghi được                   |
| `def_possessions`   | INT    | Tổng số pha bóng phòng ngự           |
| `points_allowed`    | INT    | Tổng điểm bị đối thủ ghi             |
| `total_fga`         | INT    | Tổng số cú ném (Field Goal Attempts) |
| `three_pt_attempts` | INT    | Tổng số cú ném 3 điểm                |
| `rim_attempts`      | INT    | Tổng số cú ném cận rổ                |
| `midrange_attempts` | INT    | Tổng số cú ném tầm trung             |

> **Lưu ý thiết kế:** Bảng này lưu **Raw Counts** (Số đếm thô), KHÔNG lưu tỷ lệ phần trăm. Lý do: DuckDB sẽ tính ORtg, DRtg, Pace... on-the-fly khi người dùng thay đổi bộ lọc, đảm bảo tính toán luôn chính xác khi aggregation qua nhiều mùa giải.

### Bảng 4: `fact_player_metrics` — Chỉ số cầu thủ tổng hợp (Raw Counts)

| Cột         | Kiểu   | Mô tả                                     |
| :---------- | :----- | :---------------------------------------- |
| `player`    | STRING | Tên cầu thủ                               |
| `season`    | INT    | Mùa giải (partition key)                  |
| `is_clutch` | INT    | `1` = Clutch, `0` = bình thường           |
| `pts`       | INT    | Tổng điểm ghi được                        |
| `fga`       | INT    | Tổng cú ném (Field Goal Attempts)         |
| `fgm`       | INT    | Tổng cú ném thành công (Field Goals Made) |
| `fg3m`      | INT    | Tổng cú ném 3 điểm thành công             |
| `fg3a`      | INT    | Tổng cú ném 3 điểm                        |
| `fta`       | INT    | Tổng cú ném phạt (Free Throw Attempts)    |
| `tov`       | INT    | Tổng số lần mất bóng (Turnovers)          |

---

## PHẦN 1.6: QUERY → DASHBOARD MAPPING (Truy vấn phục vụ từng biểu đồ)

### Team Analytics Dashboard

| Biểu đồ                     | Hàm Query                                         | Bảng nguồn          | Chỉ số tính toán                                                         |
| :-------------------------- | :------------------------------------------------ | :------------------ | :----------------------------------------------------------------------- |
| Shot Distribution Evolution | `get_league_evolution()`                          | `fact_team_metrics` | `three_point_rate`, `rim_rate`, `midrange_rate` = tỷ lệ cú ném theo vùng |
| Pace Evolution              | `get_league_evolution()` + `get_team_evolution()` | `fact_team_metrics` | `pace` = `SUM(possessions) / MAX(games)`                                 |
| Team Shot Chart             | `get_team_shot_heatmap()`                         | `fact_shots`        | Tọa độ `x, y` + `made_flag` cho scatter                                  |
| Offensive Radar             | `get_team_evolution()`                            | `fact_team_metrics` | Radar 5 trục: `pace`, `ORtg`, `3PT%`, `rim%`, `mid%`                     |
| ORtg vs DRtg Scatter        | `get_team_evolution()`                            | `fact_team_metrics` | `ORtg` = `pts/poss*100`, `DRtg` = `pts_allowed/def_poss*100`             |
| Efficiency Table            | `get_team_evolution()`                            | `fact_team_metrics` | Trung bình tất cả chỉ số theo `GROUP BY team`                            |

### Player Analytics Dashboard

| Biểu đồ                  | Hàm Query                                          | Bảng nguồn            | Chỉ số tính toán                                              |
| :----------------------- | :------------------------------------------------- | :-------------------- | :------------------------------------------------------------ |
| Efficiency Bars          | `get_player_stats()` + `get_league_player_stats()` | `fact_player_metrics` | `TS%`, `eFG%`, `3PT%` (mùa gần nhất)                          |
| Shot Profile Stacked Bar | `get_player_shot_profile()`                        | `fact_shots`          | `rim_rate`, `midrange_rate`, `three_point_rate` theo `season` |
| Career Efficiency        | `get_player_stats()` + `get_league_player_stats()` | `fact_player_metrics` | `TS%`, `eFG%` theo `season` (đường Player + đường League Avg) |
| Player Shot Chart        | `get_player_shot_heatmap()`                        | `fact_shots`          | Tọa độ `x, y` + `made_flag` cho scatter                       |

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

- **Apache Spark (PySpark):** Đóng vai trò là cỗ máy ETL (Trích xuất, Làm sạch, Nạp). Dữ liệu Play-by-Play của 29 năm lên tới hàng chục triệu sự kiện. Việc dùng thư viện Pandas truyền thống sẽ gây tràn RAM (Out-of-memory). PySpark giải quyết bài toán này nhờ cơ chế Distributed Computing (xử lý phân tán trên nhiều lõi CPU).
- **Định dạng Parquet:** Được dùng để lưu trữ dữ liệu sau khi làm sạch. Là dạng lưu trữ hình cột (Columnar Storage) tích hợp nén cao, cực kỳ tối ưu cho bài toán OLAP vì nó cho phép hệ thống chỉ đọc những cột cần thiết thay vì lướt qua toàn bộ dòng.
- **DuckDB:** Đóng vai trò là Query Engine (Serving Layer). Thay vì phải dựng lên một server CSDL cồng kềnh (PostgreSQL, MySQL), DuckDB là CSDL In-Memory siêu tốc độ, có thể thực thi SQL trực tiếp lên các file Parquet với tốc độ dưới micro-giây.
- **Streamlit & Plotly:** Nền tảng xây dựng Dashboard UI và đồ thị tương tác cao. Kết hợp cùng các bộ thư viện vệ tinh (`streamlit-antd-components`, `streamlit-extras`) để nâng cấp trải nghiệm người dùng (UX) và giao diện (UI) đạt chuẩn các sản phẩm BI thương mại.

### 2. Thông số Hiệu năng (Data Scale & System Performance)

| Metric                          | Value                           |
| :------------------------------ | :------------------------------ |
| **Kỷ nguyên dữ liệu (Seasons)** | 1997 – 2026 (29 năm)            |
| **Số lượng trận đấu (Games)**   | ~35,000+ trận                   |
| **Quy mô dữ liệu (Events)**     | ~18 Triệu+ sự kiện              |
| **Thời gian chạy ETL (Spark)**  | ~3 phút (Xử lý toàn bộ lịch sử) |
| **Tốc độ truy vấn UI (DuckDB)** | < 500 ms / biểu đồ              |
| **Độ trễ lọc Clutch Mode**      | Real-time (Tức thì)             |

---

## PHẦN 5: TRẢI NGHIỆM NGƯỜI DÙNG (UX) VÀ DATA MODELING TRADE-OFFS

### 1. Phân tích So sánh Đa chiều (Multi-Team & League Benchmarking)

- **Global Multiselect (Team Dashboard):** Hệ thống được thiết kế hợp nhất (Unified UX). Chỉ với một thanh chọn đội duy nhất ở đầu trang, toàn bộ bảng điều khiển sẽ biến đổi đồng bộ. Khi chọn nhiều đội (VD: BOS, LAL, GSW), biểu đồ _Pace Evolution_ sẽ vẽ các đường rượt đuổi nhau, _Offensive Radar_ sẽ chồng các lớp đa giác (Overlay) để so sánh điểm mạnh yếu, và _Shot Heatmap_ sẽ gộp chung tọa độ ném của toàn bộ đội được chọn thành một bản đồ nhiệt khổng lồ để tìm ra xu hướng ném rổ của một nhóm thế hệ.
- **Contextual Benchmarking (Player Dashboard):** Mọi biểu đồ đánh giá cá nhân (Efficiency Bars, Career Curves) đều được ép thêm lớp dữ liệu **League Average (Trung bình giải đấu)** bằng các đường đứt nét màu xám. Điều này giúp người xem đánh giá được chính xác cầu thủ này đang thực sự vươn tầm hay chỉ là "ếch ngồi đáy giếng".

### 2. Sự đánh đổi giữa Pre-aggregation và Dynamic OLAP

Trong thiết kế hệ thống Big Data, việc tính toán tức thì trên 18 triệu sự kiện đòi hỏi sự phân chia nhiệm vụ chiến lược:

- **Tối ưu tốc độ (Pre-aggregation):** Đối với các chỉ số tính toán phức tạp đòi hỏi Join nhiều bảng (như DRtg, Pace), dữ liệu được cộng dồn (aggregated) sẵn ở tầng Spark ETL dựa trên định nghĩa Clutch tiêu chuẩn của NBA (5 phút cuối trận, cách biệt $\leq$ 5 điểm). Nhờ vậy, `fact_team_metrics` trở nên siêu nhỏ gọn, giúp biểu đồ tải mượt mà trong chớp mắt.
- **Tối đa linh hoạt (On-the-fly Calculation):** Đối với biểu đồ **Shot Chart** và **Shot Profile**, hệ thống giữ lại các cột thô `seconds_remaining` và `score_margin` vào bảng `fact_shots`. Cho phép hiển thị thêm thanh trượt (Sliders) để người dùng tự do định nghĩa khái niệm "Clutch" (VD: 10 giây cuối, cách biệt 1 điểm). DuckDB sẽ lãnh ấn quét trực tiếp hàng triệu tọa độ thô, mang lại khả năng tùy biến cực độ cho phân tích chiến thuật chi tiết.

---

## PHẦN 6: TÀI LIỆU THAM KHẢO & NGUỒN CHUẨN (REFERENCES)

Để đảm bảo tính toàn vẹn và độ tin cậy của mô hình dữ liệu (Data Integrity), mọi công thức toán học và hệ quy chiếu hình học trong đồ án đều được tham chiếu từ các tài liệu chuẩn của ngành công nghiệp phân tích bóng rổ (NBA Analytics).

### 1. Nguồn Công thức Chỉ số Nâng cao (Advanced Stats Formulas)

- **True Shooting Percentage (TS%) & Effective Field Goal Percentage (eFG%)**:
  - _Nguồn:_ **Basketball-Reference Glossary** (Trang thống kê lưu trữ dữ liệu lịch sử chuẩn nhất thế giới).
  - _Link tham chiếu:_ [https://www.basketball-reference.com/about/glossary.html](https://www.basketball-reference.com/about/glossary.html)
  - _Ghi chú áp dụng:_ Đồ án sử dụng chính xác hệ số bù `0.44` cho ném phạt (FTA) và `0.5` cho ném 3 (3PM) theo đúng quy chuẩn phân tích bóng rổ hiện đại.
- **Offensive Rating (ORtg) / Defensive Rating (DRtg) / Pace**:
  - _Nguồn:_ **NBA Advanced Stats Glossary** (Từ điển thống kê chính thức của NBA).
  - _Link tham chiếu:_ [https://www.nba.com/stats/help/glossary](https://www.nba.com/stats/help/glossary)
  - _Ghi chú áp dụng:_ Chỉ số được quy chuẩn hóa trên 100 pha bóng (per 100 possessions) thay vì tính trung bình mỗi trận, nhằm loại bỏ hoàn toàn độ nhiễu của nhịp độ thi đấu (Pace).

### 2. Nguồn Hệ Tọa độ Sân bóng (Court Dimensions & Coordinates)

- **Hệ trục tọa độ (X: -250 đến 250, Y: -47.5 đến 422.5)**:
  - _Nguồn:_ **Tài liệu Endpoint API của NBA (`shotchartdetail`)**.
  - _Tài liệu mã nguồn mở:_ Thư viện cộng đồng Khoa học Dữ liệu **swar/nba_api** trên GitHub.
  - _Link tham chiếu:_ [nba_api / shotchartdetail.md](https://github.com/swar/nba_api/blob/master/docs/nba_api/stats/endpoints/shotchartdetail.md)
  - _Ghi chú áp dụng:_ Tọa độ trong đồ án giữ nguyên chuẩn raw data của tổ chức NBA: `1 unit = 0.1 feet`. Trục X rộng 500 units (50 feet), tâm rổ đặt tại gốc `(0,0)`. Do đó, đồ án sử dụng biểu đồ **Nửa sân (Half-court)** làm chuẩn hiển thị cho mọi phân tích Shot Chart để tối ưu hóa mật độ dữ liệu tại khu vực ném (Shot zones), chuẩn hóa y hệt các hệ thống Analytics nội bộ của các đội bóng NBA.
