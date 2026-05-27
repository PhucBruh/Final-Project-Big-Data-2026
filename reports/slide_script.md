# Script Slide Thuyết Trình: NBA Big Data Analytics Platform

> Tổng cộng: **10 slides** | Thời lượng khuyến nghị: ~15 phút

---

## Slide 1: Trang bìa (Title Slide)

**Tiêu đề:** NBA Big Data Analytics Platform

**Nội dung hiển thị:**

- Tên đồ án, tên sinh viên, lớp, GVHD
- Logo NBA / hình nền bóng rổ

---

## Slide 2: Đặt vấn đề & Mục tiêu

**Tiêu đề:** Đặt vấn đề

**Nội dung:**

- Dữ liệu NBA Play-by-Play từ 1997–2026 chứa **~18 triệu sự kiện** → quá lớn cho Excel/Pandas
- **Vấn đề:** Làm sao xử lý, phân tích và trực quan hóa dữ liệu bóng rổ ở quy mô Big Data một cách hiệu quả?
- **Mục tiêu:**
  1. Xây dựng pipeline ETL xử lý phân tán bằng Apache Spark
  2. Tái cấu trúc pha bóng (Possession) từ dữ liệu thô
  3. Dashboard tương tác real-time với độ trễ < 150ms

**Ghi chú thuyết trình:** Nhấn mạnh con số 18 triệu sự kiện để thể hiện tính Big Data.

---

## Slide 3: Kiến trúc hệ thống (System Architecture)

**Tiêu đề:** Kiến trúc tổng quan

**Nội dung:** Sơ đồ luồng dữ liệu (vẽ diagram):

```
CSV (Kaggle) → PySpark ETL → Parquet Data Lake → DuckDB (Query) → Streamlit Dashboard
```

- **PySpark:** Xử lý phân tán 18M+ events
- **Parquet:** Lưu trữ cột, nén 80%, phân vùng theo season
- **DuckDB:** Query engine in-memory, đọc trực tiếp Parquet
- **Streamlit + Plotly:** Dashboard tương tác

**Ghi chú thuyết trình:** Giải thích vì sao KHÔNG dùng database truyền thống → Parquet + DuckDB = zero-server, tối ưu cho OLAP.

---

## Slide 4: Quy trình ETL chi tiết

**Tiêu đề:** Data Processing Pipeline

**Nội dung:** 5 bước (dùng biểu đồ dạng timeline/flowchart):

| Bước | Tên         | Mô tả ngắn                                                         |
| ---- | ----------- | ------------------------------------------------------------------ |
| 1    | Ingestion   | Đọc CSV với Schema cứng (StructType)                               |
| 2    | Cleaning    | Chuẩn hóa tọa độ, parse game clock, tái tạo điểm số, gắn cờ Clutch |
| 3    | Possession  | Window Functions (`lag`, `last`) để xác định ranh giới pha bóng    |
| 4    | Aggregation | Cộng dồn Raw Counts theo (team/player, season, is_clutch)          |
| 5    | Storage     | Ghi Parquet phân vùng theo season → 4 Fact Tables                  |

**Ghi chú thuyết trình:** Nhấn mạnh bước 3 (Possession Reconstruction) là phần kỹ thuật khó nhất — dữ liệu NBA thô không có khái niệm "pha bóng".

---

## Slide 5: Mô hình dữ liệu (Data Model)

**Tiêu đề:** Schema & Data Dictionary

**Nội dung:** Hiển thị 4 bảng Fact:

| Bảng                  | Số cột | Vai trò                              |
| --------------------- | ------ | ------------------------------------ |
| `fact_shots`          | 12     | Tọa độ từng cú ném (cho Shot Chart)  |
| `fact_possessions`    | 6      | Từng pha bóng đã tái cấu trúc        |
| `fact_team_metrics`   | 12     | Chỉ số đội tổng hợp (Raw Counts)     |
| `fact_player_metrics` | 10     | Chỉ số cầu thủ tổng hợp (Raw Counts) |

- Thiết kế lưu **Raw Counts** → DuckDB tính tỷ lệ on-the-fly
- Partition theo `season` → Partition Pruning tăng tốc query

**Ghi chú thuyết trình:** Giải thích trade-off: lưu số thô thay vì % để DuckDB linh hoạt tổng hợp khi user thay đổi bộ lọc.

---

## Slide 6: Các chỉ số phân tích nâng cao

**Tiêu đề:** Advanced Analytics Metrics

**Nội dung:**

| Chỉ số | Công thức                        | Ý nghĩa                               |
| ------ | -------------------------------- | ------------------------------------- |
| ORtg   | `(PTS / Possessions) × 100`      | Hiệu quả tấn công (điểm/100 pha bóng) |
| DRtg   | `(PTS_Allowed / Def_Poss) × 100` | Hiệu quả phòng thủ                    |
| TS%    | `PTS / (2 × (FGA + 0.44×FTA))`   | Tỷ lệ ném rổ thực tế                  |
| eFG%   | `(FGM + 0.5×3PM) / FGA`          | Tỷ lệ ném hiệu chỉnh cho 3 điểm       |
| Pace   | `Possessions / Games`            | Nhịp độ thi đấu                       |

**Ghi chú thuyết trình:** Giải thích ngắn gọn vì sao TS% tốt hơn FG% truyền thống (tính luôn giá trị 3 điểm + ném phạt).

---

## Slide 7: Demo Dashboard — Team Analytics

**Tiêu đề:** Team Analytics Dashboard

**Nội dung:** Screenshot thực tế hoặc **DEMO TRỰC TIẾP** gồm:

1. **Shot Distribution Evolution** — Stacked bar: thấy rõ xu hướng ném 3 điểm tăng qua các năm
2. **Pace Evolution** — Line chart: so sánh tốc độ chơi giữa các đội
3. **Shot Chart** — Scatter trên sân bóng: Hot Zone / Cold Zone
4. **Offensive Radar** — Radar overlay so sánh nhiều đội
5. **ORtg vs DRtg Scatter** — Đánh giá sức mạnh tổng hợp
6. **Efficiency Table** — Progress bar trực quan

**Ghi chú thuyết trình:** Demo live: chọn 2-3 đội (VD: BOS, GSW, LAL), bật Clutch Mode, kéo slider thời gian → toàn bộ dashboard thay đổi đồng bộ.

---

## Slide 8: Demo Dashboard — Player Analytics

**Tiêu đề:** Player Analytics Dashboard

**Nội dung:** Screenshot hoặc **DEMO TRỰC TIẾP** gồm:

1. **KPI Metric Cards** — TS%, eFG%, 3PT% với delta so với League Average
2. **Efficiency Bars** — So sánh cầu thủ vs trung bình giải (đường đứt nét xám)
3. **Shot Profile Stacked Bar** — Rim / Midrange / 3PT qua các mùa
4. **Career Efficiency Curve** — Đường cong sự nghiệp + League baseline
5. **Player Shot Chart** — Toggle Made/Missed

**Ghi chú thuyết trình:** Demo: chọn một siêu sao (LeBron), bật Clutch Mode → so sánh hiệu suất bình thường vs lúc áp lực. Chỉ ra sự khác biệt trên Shot Chart.

---

## Slide 9: Hiệu năng hệ thống

**Tiêu đề:** System Performance & Benchmarks

**Nội dung:**

| Metric                 | Value              |
| ---------------------- | ------------------ |
| Kỷ nguyên dữ liệu      | 1997–2026 (29 năm) |
| Quy mô                 | ~18 Triệu+ sự kiện |
| ETL Processing Time    | ~3 phút            |
| Query Latency (DuckDB) | < 500ms / biểu đồ  |
| Clutch Mode Toggle     | Real-time          |

**Điểm nhấn kỹ thuật:**

- Parquet giảm 80% dung lượng so với CSV gốc
- Partition Pruning: chỉ đọc thư mục cần thiết
- DuckDB In-Memory: không cần database server

**Ghi chú thuyết trình:** Có thể demo bấm Clutch Mode toggle và đo thời gian phản hồi thực tế trước mặt giám khảo.

---

## Slide 10: Tổng kết & Hướng phát triển

**Tiêu đề:** Kết luận

**Đã đạt được:**

- ✅ Pipeline ETL phân tán xử lý 18M+ events
- ✅ Tái cấu trúc Possession từ dữ liệu thô (kỹ thuật Big Data nâng cao)
- ✅ Dashboard tương tác real-time với Clutch Mode động
- ✅ So sánh đa chiều: Multi-team, Player vs League Average

**Hướng phát triển:**

- 🔮 Thêm mô hình ML dự đoán kết quả trận đấu
- 🔮 Deploy lên Cloud (Docker + AWS/GCP)
- 🔮 Tích hợp dữ liệu real-time qua API

**Ghi chú thuyết trình:** Kết thúc bằng câu: "Hệ thống này chứng minh rằng với kiến trúc Big Data đúng đắn, chúng ta có thể biến 18 triệu dòng dữ liệu thô thành những insight chiến thuật có giá trị trong chưa đầy 150 mili-giây."

---

## Phụ lục: Mẹo thuyết trình

1. **Slide 7-8 nên demo live** thay vì screenshot nếu có thể — giám khảo sẽ ấn tượng hơn nhiều
2. **Chuẩn bị sẵn** Streamlit đang chạy trước khi lên thuyết trình
3. **Kịch bản demo:** Chọn LeBron James → bật Clutch Mode → thay đổi slider → chỉ ra sự khác biệt
4. Nếu bị hỏi "Sao không dùng PostgreSQL?" → trả lời: "Parquet + DuckDB = zero-server, tối ưu cho OLAP read-heavy workload, không cần quản trị DB"
