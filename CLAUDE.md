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
- [x] Git仓库 (DjangoWwang/jinfan-quant, 私有)

### Phase 1: 数据基础 — 爬虫模块 - 完成
- [x] crawler/fof99/client.py: 登录、认证、DES解密
- [x] crawler/fof99/fund_scraper.py: advancedList(39.8万基金)+公司基金+策略分类
- [x] crawler/fof99/nav_scraper.py: viewv2(已解密)+view(v1,DES)+指标获取
- [x] crawler/fof99/index_scraper.py: 指数列表+历史净值+牛牛自建指数
- [x] 集成测试通过 (19/24项)

### Phase 2: 基金研究 — 指标+基金池+比较 - 完成
- [x] engine/metrics.py: 收益/回撤/波动率/Sharpe/Sortino/Calmar/区间计算/归一化NAV
- [x] engine/freq_align.py: 频率检测/降频/插值/多序列交集对齐
- [x] schemas/fund.py: Fund/Nav/Pool/Comparison请求响应Schema
- [x] services/fund_service.py: CRUD + NAV upsert + NAV Series
- [x] services/pool_service.py: 基金池增删查
- [x] services/comparison_service.py: 多基金比较(频率对齐+指标计算)
- [x] API路由: funds(CRUD+NAV+指标), pools(CRUD), comparison(POST)
- [x] services/ingestion_service.py: 爬虫数据入库(基金列表+净值+指数)
- [x] 前端: 基金列表(动态多选筛选)+详情(ECharts图表区间联动)+基金池+比较

### Phase 3: 组合研究与回测 - 完成
- [x] engine/backtest.py: 回测引擎(intersection/dynamic_entry/truncate三模式)
- [x] engine/backtest准确性验证: 合成数据0差异 + 真实指数数据通过
- [x] schemas/portfolio.py: Portfolio/Backtest请求响应Schema(支持基金+指数混合)
- [x] API: portfolios(CRUD), backtest/run(执行), backtest/results(查看), backtest/history(历史)
- [x] API: backtest/search-assets(统一搜索, 支持asset_type过滤)
- [x] 前端: 组合创建向导(3步骤: 配置→参数→回测)
- [x] 前端: 组合列表页 + 组合详情页(权重饼图+回测结果+历史记录)
- [x] 前端: 保存组合弹窗、删除组合
- [x] DB迁移: portfolio_allocations支持index_code(基金+指数混合)
- [x] fetchApi: 解析服务端错误详情、处理204响应
- [x] 比较页: 错误提示优化(显示缺数据基金名)、搜索支持单字

### Phase 4-5: 待开始
- [ ] 产品运营 + 估值表导入
- [ ] 定时任务(daily_update.py)
- [ ] 仪表盘完善 + 前端体验优化

## 数据状态
- 基金: 918只(有fof99_fund_id的915只可爬取)
- 有NAV数据的基金: 113只(39只日频, 73只周频, 1只未知), 50,985条净值记录
- 无NAV数据: 805只(fof99 API不返回净值历史, 可能因私募限制)
- 指数: 3只核心指数(沪深300/中证500/中证1000), 98,423条数据
- NAV数据范围: 2002-01-04 ~ 2026-02-25
- 全量拉取完成(2026-03-06)

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
- 权重系统: 前端百分比(0-100), API传输比例(0-1)
- 资产ID格式: fund_123(基金), idx_000300(指数)

## 火富牛登录凭据
- 账号: 见 backend/.env
- 登录地址: fof99.com
- 正使用账号2 (机构账号, 全量39万+基金)
