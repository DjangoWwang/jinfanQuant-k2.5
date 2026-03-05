# 晋帆投研 FOF平台 - 项目上下文

## 项目概述
鹭岛晋帆团队（1-5人）内部FOF投研平台，包含基金数据库、基金研究、产品运营三大模块。

## 技术栈
- **前端**: Next.js 14+ (App Router) + TypeScript + Tailwind CSS v4 + Shadcn/ui + ECharts
- **后端**: Python 3.11 + FastAPI + SQLAlchemy async + Alembic
- **数据库**: PostgreSQL 16
- **金融计算**: numpy + pandas + scipy
- **部署**: Docker Compose

## 关键路径
- 后端: `backend/app/`
- 前端: `frontend/src/`
- 金融计算引擎: `backend/app/engine/`
- 数据采集: `backend/app/crawler/`
- 估值表解析: `backend/app/importer/`
- 数据库迁移: `backend/alembic/`

## 开发进度

### Phase 0: 基础设施搭建
- [x] 环境检测 + Poetry安装
- [x] 后端骨架 (FastAPI + 16个ORM模型 + 10个API路由桩)
- [x] Poetry依赖安装
- [x] Alembic迁移配置 (async)
- [x] 前端初始化 (Next.js + Tailwind v4 + Shadcn/ui + ECharts)
- [x] 定时任务桩 (tasks/scheduler.py)
- [ ] engine/ 模块桩 (metrics, backtest, freq_align, calendar, allocation)
- [ ] crawler/ + importer/ + schemas/ + services/ 桩
- [ ] 前端UI壳 (侧边栏+仪表盘+基金列表)
- [ ] Git仓库初始化 + 首次提交
- [ ] Docker Desktop安装 (用户操作中)
- [ ] 数据库迁移运行
- [ ] 交易日历填充

### Phase 1-5: 待开始

## 编码约定
- 后端: Python async/await, type hints, ruff格式化, 120字符行宽
- 前端: TypeScript strict, "use client" for interactive components
- Git commit: `[Phase X] 模块: 具体内容`
- 中文UI文本, 英文代码变量名

## 重要决策
- 数据频率: funds.nav_frequency 标记日频/周频
- 实盘/模拟: products.product_type + portfolios.portfolio_type 区分
- 无风险利率: 可配置 (默认固定2.5%)
- 频率对齐: 默认降频至周频, 可选日频插值
- 数据源: 爬虫为可扩展模块, 当前仅火富牛

## 火富牛登录凭据
- 账号: 见 backend/.env
- 登录地址: fof99.com
