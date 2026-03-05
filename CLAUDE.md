# 晋帆投研 FOF平台 - 项目上下文

## 项目概述
鹭岛晋帆团队（1-5人）内部FOF投研平台，包含基金数据库、基金研究、产品运营三大模块。

## 技术栈
- **前端**: Next.js 14+ (App Router) + TypeScript + Tailwind CSS v4 + Shadcn/ui + ECharts
- **后端**: Python 3.11 + FastAPI + SQLAlchemy async + Alembic
- **数据库**: PostgreSQL 17 (本地安装, trust认证)
- **金融计算**: numpy + pandas + scipy
- **部署**: Docker Compose (未来)

## 数据库连接
- URL: `postgresql+asyncpg://fof_user:jinfan2026@localhost:5432/fof_platform`
- 超级用户: postgres (trust认证, 无需密码)
- 应用用户: fof_user / jinfan2026
- pg_hba.conf: 本地连接已改为trust模式

## 关键路径
- 后端: `backend/app/`
- 前端: `frontend/src/`
- 金融计算引擎: `backend/app/engine/`
- 数据采集: `backend/app/crawler/`
- 估值表解析: `backend/app/importer/`
- 数据库迁移: `backend/alembic/`

## 开发进度

### Phase 0: 基础设施搭建 - 完成
- [x] 环境检测 + Poetry安装
- [x] 后端骨架 (FastAPI + 16个ORM模型 + API路由)
- [x] Poetry依赖安装
- [x] Alembic迁移配置 (async) + 初始迁移已执行
- [x] 前端初始化 (Next.js + Tailwind v4 + Shadcn/ui + ECharts)
- [x] PostgreSQL 17安装 (winget) + 数据库创建
- [x] 数据库迁移运行 (17个表已创建)
- [x] 交易日历填充 (2016-2026, 4018行, 2672个交易日)
- [x] Git仓库 (nardwang2026/jinfan-quant, 私有)

### Phase 1: 数据基础 — 爬虫模块 - 完成
- [x] crawler/fof99/client.py: 登录、认证、DES解密
- [x] crawler/fof99/fund_scraper.py: advancedList(39.8万基金)+公司基金+策略分类
- [x] crawler/fof99/nav_scraper.py: viewv2(已解密)+view(v1,DES)+指标获取
- [x] crawler/fof99/index_scraper.py: 指数列表+历史净值+牛牛自建指数
- [x] 集成测试通过 (19/24项)

### Phase 2: 基金研究 — 指标+基金池+比较 - 进行中
- [x] engine/metrics.py: 收益/回撤/波动率/Sharpe/Sortino/Calmar/区间计算/归一化NAV
- [x] engine/freq_align.py: 频率检测/降频/插值/多序列交集对齐
- [x] schemas/fund.py: Fund/Nav/Pool/Comparison请求响应Schema
- [x] services/fund_service.py: CRUD + NAV upsert + NAV Series
- [x] services/pool_service.py: 基金池增删查
- [x] services/comparison_service.py: 多基金比较(频率对齐+指标计算)
- [x] API路由: funds(CRUD+NAV+指标), pools(CRUD), comparison(POST)
- [x] FastAPI启动验证通过 (health + funds + pools 端点正常)
- [ ] 爬虫数据入库服务 (crawler→DB pipeline)
- [ ] 前端: 基金列表+详情+基金池+比较页面

### Phase 3-5: 待开始

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
- 交易日历: exchange_calendars最远支持到2026年底

## 火富牛登录凭据
- 账号: 见 backend/.env
- 登录地址: fof99.com
- 正使用账号2 (机构账号, 全量39万+基金)
