# Đề xuất tách nội dung SKILL.md ra file riêng

Mục tiêu: Giữ **SKILL.md** ngắn gọn (khoảng 150–200 dòng), dễ quét; chi tiết đưa vào các file .md riêng và load khi cần.

---

## 1. Giữ trong SKILL.md (bản rút gọn)

| Phần | Nội dung giữ lại |
|------|------------------|
| **Frontmatter** | Toàn bộ (name, description, metadata, triggers) — bắt buộc |
| **Role Definition** | 2–3 câu + bullet ngắn (expertise), 1 câu differentiator |
| **When to Use** | Giữ nguyên (đã ngắn) |
| **Human-in-the-Loop** | 1 đoạn: có checkpoints, skip mode tồn tại → "Chi tiết: `references/checkpoints.md`" |
| **7-Phase Workflow** | Liệt kê 7 phase + 4 checkpoint; mỗi phase 1 dòng mô tả + "Chi tiết: `references/workflow.md`" (hoặc từng file phase) |
| **Reference Guide** | Bảng "Topic | Reference | Load When" — giữ nguyên |
| **Scripts** | Bảng rút gọn (Script | Purpose) hoặc "Chi tiết: `references/scripts-reference.md`" |
| **Database Connections** | 1–2 câu + "Chi tiết: `references/database-connections.md`" |
| **Folder Structure** | 1 đoạn tóm tắt ngắn + "Chi tiết: `references/folder-structure.md`" |
| **Constraints** | Giữ **MUST DO** / **MUST NOT DO** dạng bullet ngắn (có thể rút từ full list) — vì agent cần thấy rõ ràng |
| **Knowledge Reference** | Giữ nguyên (đã ngắn) |

---

## 2. Chuyển sang file riêng — đề xuất từng file

### 2.1. `references/checkpoints.md` (mới)

**Nội dung chuyển từ SKILL.md:**
- Toàn bộ mục **Human-in-the-Loop Checkpoints** (Skip Mode, Partial skip, Checkpoint rules).
- Mẫu câu hỏi có cấu trúc (multiple-choice + free-text).
- Quy tắc: NEVER proceed without user response, re-present khi user sửa, label [CHECKPOINT N].

**Trong SKILL.md chỉ còn:** 1 đoạn tóm tắt + "See `references/checkpoints.md` for skip mode and checkpoint rules."

---

### 2.2. `references/workflow.md` (mới) — hoặc tách từng phase

**Option A: Một file `workflow.md`**
- Gộp toàn bộ chi tiết 7 phase + 4 checkpoint (Phase 1–7, template brief, template data mapping, template query, Phase 5 checklists, Phase 6 bullets, Phase 7 knowledge-base + security).

**Option B: Nhiều file (workflow ngắn hơn từng file)**
- `references/workflow-phase-1.md` — Requirement analysis, glossary, **template brief**.
- `references/workflow-phase-2.md` — Data discovery (2a–2d), Excel doc types, schema search, deep inspection commands.
- `references/workflow-phase-3.md` — Data mapping doc + **template data mapping**.
- `references/workflow-phase-4.md` — Query design principles, PII, **query structure template**.
- `references/workflow-phase-5.md` — EXPLAIN + run_query_safe, checklists.
- `references/workflow-phase-6.md` — Optimization bullets (có thể gộp với `references/optimization.md` hoặc để ngắn).
- `references/workflow-phase-7.md` — Save & document + **Session knowledge distillation** (single-table / multiple-tables naming, format) + **Security — knowledge base content** (đoạn dài về không ghi real data, PII, internal identifiers...).

**Đề xuất:** Option B để dễ bảo trì và load đúng phase đang làm. Trong SKILL.md chỉ liệt kê phase + checkpoint và ghi: "Load phase detail from `references/workflow-phase-N.md` when executing Phase N."

---

### 2.3. `references/scripts-reference.md` (mới)

**Nội dung chuyển từ SKILL.md:**
- Bảng đầy đủ **Scripts Reference** (Script | Purpose | Key Usage) như hiện tại.

**Trong SKILL.md:** Giữ bảng rút gọn (Script | Purpose) hoặc chỉ dẫn "See `references/scripts-reference.md` for full usage."

---

### 2.4. `references/database-connections.md` (mới)

**Nội dung chuyển từ SKILL.md:**
- **Database Connections**: env vars (`{ALIAS}_TYPE`, `_USERNAME`, `_PASSWORD`, `_DSN` / `_HOST`, `_PORT`, `_DATABASE`, `_DRIVER`).
- Default alias `DWH`.
- Phần **SQL Server specific** (port 1433, pyodbc, DRIVER).

**Trong SKILL.md:** 1–2 câu: connections qua env, default DWH; chi tiết trong `references/database-connections.md`.

---

### 2.5. `references/folder-structure.md` (mới)

**Nội dung chuyển từ SKILL.md:**
- Toàn bộ **Folder Structure** (documents/, queries/, references/, scripts/, single-table/, multiple-tables/) kèm mô tả và quy ước đặt tên.

**Trong SKILL.md:** 1 đoạn tóm tắt (đường dẫn chính + mục đích) + "See `references/folder-structure.md`."

---

### 2.6. `references/constraints.md` (tùy chọn)

**Nội dung chuyển từ SKILL.md:**
- Toàn bộ bullet **MUST DO** và **MUST NOT DO** (đầy đủ như hiện tại).

**Lưu ý:** Constraints thường nên **vẫn xuất hiện trong SKILL.md** (dạng rút gọn) để agent luôn thấy. Có thể:
- Giữ trong SKILL.md bản rút gọn (5–7 MUST DO, 5–7 MUST NOT DO quan trọng nhất), và
- File `references/constraints.md` chứa bản đầy đủ + giải thích.

---

## 3. Cấu trúc thư mục sau refactor

```
pro-data-analyst/
├── SKILL.md                    # Ngắn, compact: role, when-to-use, workflow tóm tắt, ref guide, constraints tóm tắt
├── references/
│   ├── checkpoints.md          # (mới) Checkpoint & skip mode
│   ├── workflow-phase-1.md    # (mới) Phase 1 + brief template
│   ├── workflow-phase-2.md    # (mới) Phase 2 discovery
│   ├── workflow-phase-3.md    # (mới) Phase 3 + data mapping template
│   ├── workflow-phase-4.md    # (mới) Phase 4 + query template
│   ├── workflow-phase-5.md    # (mới) Phase 5 testing
│   ├── workflow-phase-6.md    # (mới) Phase 6 optimization (hoặc merge vào optimization.md)
│   ├── workflow-phase-7.md    # (mới) Phase 7 + knowledge base + security
│   ├── scripts-reference.md   # (mới) Scripts đầy đủ
│   ├── database-connections.md # (mới) Env & connections
│   ├── folder-structure.md    # (mới) Folder layout
│   ├── constraints.md         # (tùy chọn) Full MUST DO / MUST NOT DO
│   ├── query-patterns.md      # (đã có)
│   ├── window-functions.md   # (đã có)
│   ├── optimization.md       # (đã có)
│   ├── database-design.md    # (đã có)
│   ├── dialect-differences.md # (đã có)
│   └── dwh-patterns.md        # (đã có)
└── docs/
    └── SKILL-REFACTOR-PROPOSAL.md  # File này
```

---

## 4. Bảng Reference Guide trong SKILL.md (cập nhật)

Sau khi tách, bảng **Reference Guide** trong SKILL.md nên thêm các dòng:

| Topic | Reference | Load When |
|-------|-----------|-----------|
| Checkpoints & skip mode | `references/checkpoints.md` | Hiểu quy tắc checkpoint, skip, re-confirm |
| Phase 1–7 detail | `references/workflow-phase-N.md` | Đang thực hiện Phase N |
| Scripts full usage | `references/scripts-reference.md` | Gọi script, cần cú pháp đầy đủ |
| DB connections | `references/database-connections.md` | Cấu hình / troubleshoot kết nối |
| Folder layout | `references/folder-structure.md` | Đường dẫn file, đặt tên output |
| (Optional) Full constraints | `references/constraints.md` | Cần danh sách đầy đủ MUST/MUST NOT |

---

## 5. Tóm tắt lợi ích

- **SKILL.md** chỉ còn ~150–200 dòng: dễ đọc, dễ cập nhật version/trigger.
- Chi tiết theo ngữ cảnh: load `workflow-phase-N.md` khi làm phase N, `checkpoints.md` khi cần quy tắc checkpoint.
- Template (brief, data mapping, query) nằm trong workflow phase → một nơi sửa, tránh lặp.
- Phần Security (knowledge base) và PII nằm trong `workflow-phase-7.md` → rõ ràng, dễ bổ sung chính sách.

Nếu bạn muốn, bước tiếp theo có thể là: (1) tạo lần lượt các file `references/*.md` và chuyển nội dung từ SKILL.md, (2) viết lại SKILL.md bản rút gọn theo bảng trên.
