# Sơ đồ Kiến trúc & Mô hình Dữ liệu

## 1. Kiến trúc Tổng quan (System Architecture)

> 👉 **[Xem Sơ đồ Kiến trúc Hệ thống](./d1_system_architechture.mmd)**

## 2. Mô hình Dữ liệu Parquet (Data Model)

> 👉 **[Xem Sơ đồ Khái niệm Mô hình Dữ liệu](./d2_data.mmd)**
> 👉 **[Xem Sơ đồ ER (Schema chi tiết các bảng)](./d5_schema.mmd)**

> Các bảng Fact chia sẻ 3 chiều dùng chung (Shared Dimensions): `team`, `season`, `is_clutch`. Parquet không hỗ trợ Foreign Key — quan hệ được đảm bảo bởi logic ETL.

## 3. Luồng dữ liệu trên Dashboard (Data Flow)

> 👉 **[Xem Sơ đồ Luồng dữ liệu Dashboard](./d3_dashboard_data.mmd)**

## 4. Ví dụ trực quan Vòng đời Dữ liệu (Data Lifecycle Example)

**Bảng 1: Trích xuất 3 dòng sự kiện thô (Ví dụ)**

| Dòng  | PLAYER1 | EVENTMSGTYPE    | PCTIMESTRING | PERIOD | SCOREMARGIN | LOC_X | LOC_Y |
| :---- | :------ | :-------------- | :----------- | :----- | :---------- | :---- | :---- |
| **1** | LeBron  | 2 (Missed Shot) | 02:30        | 4      | 3           | 100   | 200   |
| **2** | Davis   | 4 (Rebound)     | -            | -      | -           | -     | -     |
| **3** | LeBron  | 1 (Made Shot)   | 02:25        | 4      | 3           | 150   | 50    |

> 👉 **[Xem Sơ đồ Trực quan Vòng đời Dữ liệu](./d4_data_lifecycle.mmd)**
