# 临床实验室质量控制与结果拦截

只使用Python标准库和SQLite的模块化服务，默认端口`8339`。覆盖检测项目、质控品批次、质控规则、允许范围、仪器校准、连续偏差、趋势、失控、结果拦截、复测、调查、批次切换和历史更正。

## 模块结构

- `app.py`：参数解析、依赖组装和服务生命周期。
- `src/domain.py`：角色、领域异常和数据对象。
- `src/rules.py`：QC规则计算、状态机、校准与放行约束。
- `src/repository.py`：SQLite持久化、乐观锁、幂等和审计查询。
- `src/service.py`：用例编排、权限校验和版本控制。
- `src/http_api.py`：JSON接口和统一错误响应。
- `src/audit.py`：操作审计。
- `static/index.html`：最小演示页面。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8339
```

## 核心对象

`assay`为检测项目，`qc_lot`为质控品批次，`instrument`为仪器，`qc_run`为质控结果，`result_batch`为患者结果批次，`qc_review`为失控回顾范围。

## 失控回顾

`qc_run`评价为`rejected`时，系统会沿同一仪器、同一检测项目的质控时间线，以最近一次`accepted`质控为起点、本次失控为终点自动建立`qc_review`。范围内所有已`released`患者结果批次都会进入回顾清单；没有时间依据的旧记录按该结果批次记录的仪器/项目兼容关系纳入。

两名主管对重叠区间（或同一次失控）重复建单时，SQLite事务只保留一个有效范围，后提交的单据置为`merged`并接续原范围。复核项使用：

- `outcome: retain`：复核仍合格，患者批次保持`released`。
- `outcome: recall`：受影响批次改为`recall_pending`。

逐批次提交并记录检查点；`resume_review`会继续未完成的`pending`项，已处理项跳过，不重复改版本、状态或审计。也可直接调用`POST /api/qc_reviews`创建显式窗口（无失控质控时必须提供`start_at`、`end_at`）。

## 接口

- `GET /health`
- `GET /api/<kind>`，可用`?status=`过滤
- `GET /api/entities/<id>`
- `POST /api/<kind>`
- `POST /api/entities/<id>/actions`
- `GET /api/audit`

身份通过`X-User-Id`和`X-Role`请求头传入。可选`Idempotency-Key`防止重复创建。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

质控规则为可运行的简化模型，包含1-3s、连续偏移和趋势检查，但不替代CLIA、ISO 15189、Westgard完整规则集或实验室信息系统接口。
