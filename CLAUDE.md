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

### Phase 3.5: 数据质量优化 - 完成
- [x] client.py: RateLimitError + 自适应退避重试 + 速率控制
- [x] fund.py: nav_status + data_quality_score + data_quality_tags 字段
- [x] fund_service.get_nav_series: 优先cumulative_nav + 交替模式检测
- [x] ingest_nav_robust.py: 背压控制 + 状态标记 + 断点续传
- [x] check_data_quality.py: 4维度评分(数据量/连续性/稳定性/完整性)
- [x] verify_nav_data.py: API vs DB抽样比对校验
- [x] 测试: 72个全通过(33 API + 4回测 + 7复权 + 28边界)

### Phase 4: 产品运营 - 完成
- [x] 产品运营模块 + 估值表导入 + 移动端API

### Phase 5: 仪表盘+归因 - 完成
- [x] dashboard+nav+crawl: 首页联动修复 + 产品净值接入 + 并发爬虫
- [x] FOF策略归因 + 因子暴露分析
- [x] PDF报告自动生成 + Brinson归因分析

### P0: 基础设施优化 - 完成
- [x] JWT认证(bcrypt+PyJWT) + RBAC(admin/analyst/viewer)
- [x] 基金份额关联(parent_fund_id自引用, 319个A/B/C/D份额自动检测)
- [x] 组合服务完整实现(CRUD+回测编排)
- [x] Redis缓存层(优雅降级+60秒自动重连)
- [x] Celery异步任务(回测/因子分析/报告)
- [x] 数据库索引(DESC复合索引+GIN三元组)
- [x] 三方交叉评审(Codex+MiniMax+Kimi) + 全部Critical/Major修复

### P1: 风险预警+归因+ETL+报告+前端 - 完成
- [x] P1-6: 风险预警中心 (RiskRule/AlertEvent + 4种规则 + dashboard + 9 API)
- [x] P1-7: 归因增强 (相关系数矩阵 + 基金贡献度 + 批量查询优化)
- [x] P1-8: 增量ETL管道 (增量/全量/每日刷新 Celery任务 + 管理员API)
- [x] P1-9: Excel报告导出 (openpyxl 5工作表 + 异步Celery生成)
- [x] P1-10: 前端交互增强 (风险预警页面 + ECharts归因图 + 相关性热力图)
- [x] 三方评审: 14个问题修复 (IDOR/路径遍历/契约不一致/N+1/XSS等)

### P2: 下一阶段
- [ ] P2-1: 数据增量更新自动化 (定时调度 + 断点续传 + 进度监控)
- [ ] P2-2: 前端完善 (ETL管理页 + 报告中心页 + 归因分析页接入)
- [ ] P2-3: 用户管理 (用户列表/创建/禁用 + 操作日志)
- [ ] P2-4: 产品净值计算 (基于持仓估值自动计算产品净值曲线)
- [ ] P2-5: 系统监控 (健康检查 + Celery监控 + 数据新鲜度告警)

## 三方交叉评审机制（每个开发阶段必须执行）

### 流程
1. 阶段开发完成后，**先评审再推进下一阶段**
2. 用 `_code_review.py` 自动生成 prompt 并发送三方评审
3. 汇总三方结果，修复所有 Critical + Major 后方可继续

### 评审命令
```bash
# 1. 准备 prompt 文件（将变更代码打包 + 评审维度说明）
# 2. 发送三方评审
python _code_review.py <prompt_file> --tag <phase>

# 示例
python _code_review.py _p1_review_prompt.txt --tag p1
python _code_review.py _p2_review_prompt.txt --tag p2 --only codex,kimi
```

### 三方评审人
| 评审人 | 特点 | 调用方式 |
|--------|------|----------|
| **Codex (GPT-5.4)** | 最严厉，从攻击者视角审视安全问题 | API streaming |
| **MiniMax (MiniMax-M1)** | 务实均衡，修复建议直接可用 | API |
| **Kimi** | 关注运行时风险，异步/资源泄漏敏感 | CLI (`--quiet`) |

### Prompt 模板结构
```
1. 变更概述（本阶段做了什么，几个模块）
2. 评审维度（安全性/数据完整性/性能/代码质量/架构）
3. 输出要求（总分/PASS-FAIL/问题清单含级别+位置+修复建议）
4. 代码原文（=== file_path === 格式拼接所有变更文件）
```

### 评审标准
- **Critical**: 必须修复，阻断上线
- **Major**: 强烈建议修复，影响稳定性/安全/性能
- **Minor**: 可选优化，不阻断
- 三方中任一给出 Critical → 必须修复后重新评审或说明理由
- 目标分数：三方均 ≥ 70 分

## 数据状态
- 基金: 3,628只, 份额关联: 319组(A/B/C/D类自动检测)
- NAV数据: 885,147条净值记录
- 指数: 3只核心指数(沪深300/中证500/中证1000)
- 用户: 2个(admin + analyst1), Alembic head: b2c3d4e5f6g7
- 服务端口: backend 8003, frontend 3000

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
