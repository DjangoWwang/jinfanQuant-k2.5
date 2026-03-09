"""邮箱抓取四级估值表服务框架。

该服务用于从指定邮箱中抓取包含四级估值表的邮件,提取估值数据并存储。
四级估值表包含: 资产总值、负债总值、单位净值、累计净值、日涨跌幅等详细信息。
"""

from __future__ import annotations

import email
import imaplib
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.fund_service import fund_service

logger = logging.getLogger(__name__)


@dataclass
class ValuationRecord:
    """四级估值表数据记录。"""

    fund_name: str
    nav_date: date
    unit_nav: float
    cumulative_nav: float
    total_assets: Optional[float] = None
    total_liabilities: Optional[float] = None
    net_asset_value: Optional[float] = None
    daily_return: Optional[float] = None
    shares_outstanding: Optional[float] = None
    data_source: str = "email"


@dataclass
class EmailConfig:
    """邮箱配置。"""

    imap_server: str
    imap_port: int
    username: str
    password: str
    use_ssl: bool = True


class EmailValuationService:
    """邮箱四级估值表抓取服务。

    功能:
    1. 连接邮箱服务器(IMAP)
    2. 搜索包含估值表的邮件
    3. 解析邮件内容提取估值数据
    4. 存储到数据库
    """

    # 常见估值表邮件主题关键词
    VALUATION_KEYWORDS = [
        "估值表",
        "净值",
        "估值",
        "四层估值",
        "四级估值",
        "valuation",
        "NAV",
    ]

    # 常见发件人域名白名单(可选的安全过滤)
    TRUSTED_DOMAINS: list[str] = []

    def __init__(self, config: Optional[EmailConfig] = None) -> None:
        """初始化服务。

        Args:
            config: 邮箱配置,如果为None则从环境变量读取
        """
        self.config = config or self._load_config_from_env()
        self._imap: Optional[imaplib.IMAP4] = None

    def _load_config_from_env(self) -> EmailConfig:
        """从环境变量加载邮箱配置。"""
        import os

        return EmailConfig(
            imap_server=os.getenv("EMAIL_IMAP_SERVER", "imap.qq.com"),
            imap_port=int(os.getenv("EMAIL_IMAP_PORT", "993")),
            username=os.getenv("EMAIL_USERNAME", ""),
            password=os.getenv("EMAIL_PASSWORD", ""),  # 或授权码
            use_ssl=os.getenv("EMAIL_USE_SSL", "true").lower() == "true",
        )

    async def connect(self) -> bool:
        """连接邮箱服务器。

        Returns:
            连接成功返回True
        """
        try:
            if self.config.use_ssl:
                self._imap = imaplib.IMAP4_SSL(
                    self.config.imap_server, self.config.imap_port
                )
            else:
                self._imap = imaplib.IMAP4(
                    self.config.imap_server, self.config.imap_port
                )

            self._imap.login(self.config.username, self.config.password)
            logger.info("邮箱连接成功: %s", self.config.username)
            return True

        except Exception as e:
            logger.error("邮箱连接失败: %s", e)
            return False

    async def disconnect(self) -> None:
        """断开邮箱连接。"""
        if self._imap:
            try:
                self._imap.close()
                self._imap.logout()
            except Exception as e:
                logger.warning("邮箱断开连接时出错: %s", e)
            finally:
                self._imap = None

    async def fetch_valuation_emails(
        self,
        since_date: Optional[date] = None,
        fund_name_filter: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """抓取包含估值表的邮件。

        Args:
            since_date: 起始日期,只抓取该日期之后的邮件
            fund_name_filter: 基金名称过滤(可选)

        Returns:
            邮件列表,每个邮件包含主题、日期、正文等信息
        """
        if not self._imap:
            raise RuntimeError("邮箱未连接,请先调用connect()")

        emails = []
        try:
            # 选择收件箱
            self._imap.select("INBOX")

            # 构建搜索条件
            search_criteria = []
            if since_date:
                # IMAP日期格式: 01-Jan-2024
                date_str = since_date.strftime("%d-%b-%Y")
                search_criteria.append(f'SINCE "{date_str}"')

            # 搜索所有邮件(或按条件搜索)
            search_query = " ".join(search_criteria) if search_criteria else "ALL"
            _, message_numbers = self._imap.search(None, search_query)

            for num in message_numbers[0].split():
                try:
                    _, msg_data = self._imap.fetch(num, "(RFC822)")
                    msg = email.message_from_bytes(msg_data[0][1])

                    subject = self._decode_header(msg["Subject"])
                    from_addr = self._decode_header(msg["From"])
                    date_str = msg["Date"]

                    # 检查是否包含估值表关键词
                    if self._is_valuation_email(subject, from_addr):
                        body = self._extract_body(msg)

                        # 基金名称过滤
                        if fund_name_filter and fund_name_filter not in subject:
                            continue

                        emails.append({
                            "subject": subject,
                            "from": from_addr,
                            "date": date_str,
                            "body": body,
                            "message_id": msg["Message-ID"],
                        })

                except Exception as e:
                    logger.warning("解析邮件失败: %s", e)
                    continue

            logger.info("找到 %d 封估值表邮件", len(emails))
            return emails

        except Exception as e:
            logger.error("抓取邮件失败: %s", e)
            return []

    def _is_valuation_email(self, subject: str, from_addr: str) -> bool:
        """检查邮件是否为估值表邮件。

        Args:
            subject: 邮件主题
            from_addr: 发件人地址

        Returns:
            是否匹配估值表特征
        """
        subject_lower = subject.lower()
        for keyword in self.VALUATION_KEYWORDS:
            if keyword.lower() in subject_lower:
                return True
        return False

    def _decode_header(self, header: Optional[str]) -> str:
        """解码邮件头。"""
        if not header:
            return ""
        decoded_parts = email.header.decode_header(header)
        result = []
        for part, charset in decoded_parts:
            if isinstance(part, bytes):
                result.append(part.decode(charset or "utf-8", errors="replace"))
            else:
                result.append(part)
        return "".join(result)

    def _extract_body(self, msg: email.message.Message) -> str:
        """提取邮件正文。"""
        body_parts = []

        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                if content_type == "text/plain" or content_type == "text/html":
                    try:
                        payload = part.get_payload(decode=True)
                        if payload:
                            charset = part.get_content_charset() or "utf-8"
                            body_parts.append(payload.decode(charset, errors="replace"))
                    except Exception as e:
                        logger.warning("提取邮件内容失败: %s", e)
        else:
            try:
                payload = msg.get_payload(decode=True)
                if payload:
                    charset = msg.get_content_charset() or "utf-8"
                    body_parts.append(payload.decode(charset, errors="replace"))
            except Exception as e:
                logger.warning("提取邮件内容失败: %s", e)

        return "\n".join(body_parts)

    def parse_valuation_data(self, email_data: dict[str, Any]) -> Optional[ValuationRecord]:
        """从邮件内容解析估值数据。

        需要根据不同的估值表格式实现具体的解析逻辑。
        这是一个框架方法,具体解析规则需要根据实际邮件格式定制。

        Args:
            email_data: 邮件数据

        Returns:
            解析后的估值记录,如果解析失败返回None
        """
        # TODO: 根据实际估值表格式实现具体解析逻辑
        # 示例: 使用正则表达式提取关键数据

        subject = email_data.get("subject", "")
        body = email_data.get("body", "")

        # 尝试从主题提取基金名称和日期
        fund_name = self._extract_fund_name(subject)
        nav_date = self._extract_date(subject, body)

        if not fund_name or not nav_date:
            logger.warning("无法从邮件提取基金名称或日期: %s", subject)
            return None

        # 尝试提取净值数据(需要根据实际格式调整正则表达式)
        unit_nav = self._extract_number(body, r"单位净值[：:]\s*([\d.]+)")
        cumulative_nav = self._extract_number(body, r"累计净值[：:]\s*([\d.]+)")
        total_assets = self._extract_number(body, r"资产总值[：:]\s*([\d,]+)", remove_comma=True)
        total_liabilities = self._extract_number(body, r"负债总值[：:]\s*([\d,]+)", remove_comma=True)

        if unit_nav is None or cumulative_nav is None:
            logger.warning("无法从邮件提取净值数据: %s", subject)
            return None

        return ValuationRecord(
            fund_name=fund_name,
            nav_date=nav_date,
            unit_nav=unit_nav,
            cumulative_nav=cumulative_nav,
            total_assets=total_assets,
            total_liabilities=total_liabilities,
            data_source="email",
        )

    def _extract_fund_name(self, subject: str) -> Optional[str]:
        """从主题提取基金名称。

        这是一个简单的示例实现,实际需要根据主题格式调整。
        """
        # 移除常见前缀/后缀,提取基金名称
        patterns = [
            r"【(.+?)】",
            r"\[(.+?)\]",
            r"(.+?)[-—]估值",
            r"(.+?)[-—]净值",
        ]
        for pattern in patterns:
            match = re.search(pattern, subject)
            if match:
                return match.group(1).strip()
        return None

    def _extract_date(self, subject: str, body: str) -> Optional[date]:
        """从邮件提取日期。"""
        # 尝试各种日期格式
        date_patterns = [
            r"(\d{4}[-/年]\d{1,2}[-/月]\d{1,2})",
            r"(\d{4}\d{2}\d{2})",
        ]
        for text in [subject, body]:
            for pattern in date_patterns:
                match = re.search(pattern, text)
                if match:
                    date_str = match.group(1).replace("年", "-").replace("月", "-").replace("/", "-")
                    try:
                        return datetime.strptime(date_str, "%Y-%m-%d").date()
                    except ValueError:
                        try:
                            return datetime.strptime(date_str, "%Y%m%d").date()
                        except ValueError:
                            continue
        return None

    def _extract_number(
        self, text: str, pattern: str, remove_comma: bool = False
    ) -> Optional[float]:
        """使用正则表达式提取数字。"""
        match = re.search(pattern, text)
        if match:
            num_str = match.group(1)
            if remove_comma:
                num_str = num_str.replace(",", "")
            try:
                return float(num_str)
            except ValueError:
                return None
        return None

    async def save_valuation_record(
        self,
        db: AsyncSession,
        fund_id: int,
        record: ValuationRecord,
    ) -> bool:
        """保存估值记录到数据库。

        Args:
            db: 数据库会话
            fund_id: 基金ID
            record: 估值记录

        Returns:
            保存成功返回True
        """
        try:
            nav_record = {
                "nav_date": record.nav_date.isoformat(),
                "unit_nav": record.unit_nav,
                "cumulative_nav": record.cumulative_nav,
                "daily_return": record.daily_return,
                "data_source": record.data_source,
            }

            await fund_service.upsert_nav_records(
                db, fund_id, [nav_record], default_source="email"
            )
            logger.info(
                "保存估值记录成功: fund_id=%s, date=%s, nav=%s",
                fund_id, record.nav_date, record.unit_nav
            )
            return True

        except Exception as e:
            logger.error("保存估值记录失败: %s", e)
            return False

    async def sync_valuations_from_email(
        self,
        db: AsyncSession,
        since_date: Optional[date] = None,
        fund_mapping: Optional[dict[str, int]] = None,
    ) -> dict[str, Any]:
        """从邮箱同步估值数据到数据库。

        主入口方法,执行完整的同步流程。

        Args:
            db: 数据库会话
            since_date: 起始日期
            fund_mapping: 基金名称到ID的映射 {基金名称: fund_id}

        Returns:
            同步结果统计
        """
        result = {
            "total_emails": 0,
            "parsed_success": 0,
            "parsed_failed": 0,
            "saved_success": 0,
            "saved_failed": 0,
            "errors": [],
        }

        # 连接邮箱
        if not await self.connect():
            result["errors"].append("邮箱连接失败")
            return result

        try:
            # 抓取邮件
            emails = await self.fetch_valuation_emails(since_date)
            result["total_emails"] = len(emails)

            # 解析并保存
            for email_data in emails:
                try:
                    record = self.parse_valuation_data(email_data)
                    if not record:
                        result["parsed_failed"] += 1
                        continue

                    result["parsed_success"] += 1

                    # 查找基金ID
                    fund_id = None
                    if fund_mapping:
                        fund_id = fund_mapping.get(record.fund_name)

                    if not fund_id:
                        logger.warning(
                            "未找到基金ID,跳过保存: %s", record.fund_name
                        )
                        result["saved_failed"] += 1
                        continue

                    # 保存到数据库
                    success = await self.save_valuation_record(db, fund_id, record)
                    if success:
                        result["saved_success"] += 1
                    else:
                        result["saved_failed"] += 1

                except Exception as e:
                    logger.error("处理邮件失败: %s", e)
                    result["errors"].append(str(e))
                    result["saved_failed"] += 1

        finally:
            await self.disconnect()

        return result


# 全局服务实例
email_valuation_service = EmailValuationService()
