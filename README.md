# Pro Data Analyst Skill

Senior data analyst và SQL engineer chuyên về enterprise data warehouses. Chuyển đổi câu hỏi kinh doanh thành các truy vấn SQL được tối ưu hóa và có tài liệu đầy đủ thông qua quy trình làm việc có hệ thống 7 giai đoạn.

## Tính năng

- ✅ **Hỗ trợ đa database**: Oracle, MySQL, PostgreSQL, SQL Server
- 🔍 **Data Discovery**: Tìm kiếm tables/columns qua metadata và comments
- 📊 **Query Optimization**: Phân tích EXPLAIN plans và tối ưu hóa performance
- ✅ **Safety First**: Tất cả queries được chạy với limits và timeouts
- 📝 **Tài liệu đầy đủ**: Mỗi query đều có comments và documentation
- 🤝 **Human-in-the-Loop**: Checkpoints để xác nhận với user trước khi tiến hành

## Yêu cầu hệ thống

### Python Packages

```bash
# Core dependencies
pip install python-dotenv

# Database drivers (chọn theo database bạn sử dụng)
pip install oracledb              # Cho Oracle
pip install mysql-connector-python # Cho MySQL
pip install psycopg2-binary       # Cho PostgreSQL
pip install pyodbc                # Cho SQL Server

# Optional (cho Excel document search)
pip install openpyxl pandas
```

### SQL Server - ODBC Driver

SQL Server yêu cầu ODBC driver. Cài đặt theo hệ điều hành:

**Windows:**
- Download từ [Microsoft](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

**macOS:**
```bash
# Cài đặt unixODBC
brew install unixodbc

# Cài đặt Microsoft ODBC Driver
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew install msodbcsql17
```

**Linux (Ubuntu/Debian):**
```bash
# Cài đặt unixODBC
sudo apt-get install unixodbc-dev

# Cài đặt Microsoft ODBC Driver 17
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17
```

## Cấu hình

### 1. Tạo file .env

Copy file `.env.example` thành `.env` và điền thông tin kết nối:

```bash
cp .env.example .env
```

### 2. Cấu hình database connections

#### Oracle
```env
DWH_TYPE=oracle
DWH_USERNAME=your_username
DWH_PASSWORD=your_password
DWH_DSN=hostname:port/service_name
```

#### MySQL
```env
MYSQL_DEV_TYPE=mysql
MYSQL_DEV_USERNAME=your_username
MYSQL_DEV_PASSWORD=your_password
MYSQL_DEV_HOST=localhost
MYSQL_DEV_PORT=3306
MYSQL_DEV_DATABASE=your_database
```

#### PostgreSQL
```env
PG_DEV_TYPE=postgresql
PG_DEV_USERNAME=your_username
PG_DEV_PASSWORD=your_password
PG_DEV_HOST=localhost
PG_DEV_PORT=5432
PG_DEV_DATABASE=your_database
```

#### SQL Server
```env
MSSQL_DEV_TYPE=sqlserver
MSSQL_DEV_USERNAME=your_username
MSSQL_DEV_PASSWORD=your_password
MSSQL_DEV_HOST=localhost
MSSQL_DEV_PORT=1433
MSSQL_DEV_DATABASE=your_database
# Optional: MSSQL_DEV_DRIVER={ODBC Driver 18 for SQL Server}
```

## Scripts sẵn có

### 1. Search Schema Metadata
Tìm kiếm tables và columns theo tên hoặc comment:

```bash
# Tìm kiếm trong comments và names
python scripts/search_schema.py --keyword "customer" --db DWH

# Chỉ tìm trong comments
python scripts/search_schema.py --keyword "khách hàng" --search-in comments --db DWH

# Tìm với regex
python scripts/search_schema.py --keyword "CUST_|CUSTOMER_" --regex --db DWH

# Lọc theo schema
python scripts/search_schema.py --keyword "revenue" --schema SALES --db DWH
```

### 2. Check Table Structure
Kiểm tra cấu trúc table, indexes, partitions, và statistics:

```bash
# Oracle
python scripts/check_table.py OWNER TABLE_NAME --db DWH

# SQL Server
python scripts/check_table.py dbo Customers --db MSSQL_DEV

# Xuất ra JSON
python scripts/check_table.py SCHEMA TABLE --db DWH --format json

# Xuất ra Markdown
python scripts/check_table.py SCHEMA TABLE --db DWH --format markdown
```

### 3. Run Query Safely
Chạy SELECT queries với row limits và timeouts:

```bash
# Chạy query từ string
python scripts/run_query_safe.py --sql "SELECT * FROM SCHEMA.TABLE" --db DWH

# Chạy query từ file
python scripts/run_query_safe.py --file query.sql --db DWH --limit 50

# Chỉ đếm số rows
python scripts/run_query_safe.py --file query.sql --db DWH --count-only

# Xuất ra JSON
python scripts/run_query_safe.py --file query.sql --db DWH --format json
```

### 4. EXPLAIN Plan Analysis
Phân tích execution plan để tối ưu hóa performance:

```bash
# Chạy EXPLAIN trên query
python scripts/explain_query.py --file query.sql --db DWH

# Oracle
python scripts/explain_query.py --sql "SELECT * FROM TABLE" --db DWH

# SQL Server (sử dụng SHOWPLAN)
python scripts/explain_query.py --file query.sql --db MSSQL_DEV

# Xuất ra JSON
python scripts/explain_query.py --file query.sql --db DWH --format json
```

### 5. Find Relationships
Tìm foreign keys và join paths:

```bash
# Tìm relationships của 1 table
python scripts/find_relationships.py --schema SCHEMA --table TABLE_NAME --db DWH

# Tìm join path giữa nhiều tables
python scripts/find_relationships.py --schema SCHEMA --tables TABLE1,TABLE2,TABLE3 --db DWH
```

### 6. Sample Data
Lấy sample data và profiling:

```bash
# Lấy 10 rows sample
python scripts/sample_data.py --schema SCHEMA --table TABLE_NAME --db DWH

# Lấy 50 rows
python scripts/sample_data.py --schema SCHEMA --table TABLE_NAME --db DWH --rows 50

# Data profiling (phân tích phân phối dữ liệu)
python scripts/sample_data.py --schema SCHEMA --table TABLE_NAME --db DWH --profile
```

### 7. Search Documents
Tìm kiếm trong Excel documentation (nếu có):

```bash
# Tìm trong folder documents/
python scripts/search_documents.py --keyword "customer" --folder documents/

# Tìm với regex
python scripts/search_documents.py --keyword "CUST|CUSTOMER" --folder documents/ --regex
```

## Quy trình làm việc 7 giai đoạn

Khi sử dụng skill này với Claude, quy trình sẽ được thực hiện theo 7 giai đoạn:

1. **Requirement Analysis**: Phân tích yêu cầu kinh doanh
2. **Data Discovery**: Tìm kiếm tables/columns phù hợp
3. **Data Mapping**: Lập bản đồ dữ liệu và join conditions
4. **Query Design**: Thiết kế query với CTEs và comments
5. **Query Testing**: Test với EXPLAIN và safe execution
6. **Optimization**: Tối ưu hóa dựa trên EXPLAIN plan
7. **Documentation**: Lưu query và tài liệu

### Checkpoints

Workflow có 4 checkpoints để xác nhận với user:
- **CP1**: Sau Requirement Analysis
- **CP2**: Sau Data Discovery (xác nhận tables/columns)
- **CP3**: Sau Data Mapping (xác nhận joins/filters)
- **CP4**: Trước Query Testing (xác nhận query logic)

Bạn có thể skip checkpoints bằng cách nói "skip checkpoints" hoặc "auto mode".

## Database-Specific Notes

### Oracle
- Sử dụng `ROWNUM` cho pagination
- Hỗ trợ `CONNECT BY` cho hierarchical queries
- Partition pruning với `WHERE partition_key >= ...`

### MySQL
- Case-insensitive string comparison (default)
- Sử dụng `LIMIT` cho pagination
- `GROUP_CONCAT` cho string aggregation

### PostgreSQL
- Case-sensitive string comparison (default)
- Sử dụng `LIMIT` cho pagination
- Native JSON/JSONB support

### SQL Server
- Sử dụng `TOP` hoặc `OFFSET...FETCH NEXT` cho pagination
- `STRING_AGG` cho string aggregation (SQL Server 2017+)
- Extended properties cho table/column comments

## References

Trong folder `references/` có các tài liệu tham khảo:

- `dialect-differences.md`: Khác biệt giữa Oracle, MySQL, PostgreSQL, SQL Server
- `query-patterns.md`: Các pattern SQL phổ biến
- `window-functions.md`: Hướng dẫn window functions
- `optimization.md`: Kỹ thuật tối ưu hóa query
- `database-design.md`: Thiết kế database
- `dwh-patterns.md`: Data warehouse patterns

## Troubleshooting

### SQL Server Connection Issues

**Lỗi: "Can't open lib 'ODBC Driver 17 for SQL Server'"**
- Cài đặt ODBC driver (xem phần yêu cầu hệ thống)
- Hoặc chỉ định driver khác: `MSSQL_DEV_DRIVER={ODBC Driver 18 for SQL Server}`

**Lỗi: "Login failed for user"**
- Kiểm tra username/password
- Kiểm tra SQL Server Authentication mode (Windows Auth vs SQL Auth)
- Đảm bảo user có quyền truy cập database

**Lỗi: "SSL Security error"**
- Thêm `TrustServerCertificate=yes` vào connection string
- Hoặc cấu hình SSL certificate đúng cách

### Oracle Connection Issues

**Lỗi: "TNS:could not resolve the connect identifier"**
- Kiểm tra DSN format: `hostname:port/service_name`
- Kiểm tra tnsnames.ora nếu dùng alias

### MySQL Connection Issues

**Lỗi: "Access denied for user"**
- Kiểm tra username/password
- Kiểm tra host access permissions: `GRANT ALL ON db.* TO 'user'@'host'`

## License

MIT

## Version History

- **2.2.0**: Thêm hỗ trợ SQL Server
- **2.1.0**: Cải thiện checkpoints và workflow
- **2.0.0**: Thêm hỗ trợ PostgreSQL
- **1.0.0**: Release đầu tiên với Oracle và MySQL
