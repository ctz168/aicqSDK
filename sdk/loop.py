"""
aicq.loop — 智能体迭代 Loop 快速接入模块

提供 ``LoopInAICQ`` 和 ``mySecret`` 两个核心函数，让任何 AI 智能体
只需在循环末尾调用一行代码，即可通过 AICQ 与人类主人双向通信。

核心设计
--------

智能体本质上都是通过 loop 循环调用工具，直到工具结束就停止。
``LoopInAICQ`` 在每一次 Loop 的最后被调用：

1.  输入：这一轮迭代的大模型输出（含工具调用情况和文本）+ 智能体公钥
2.  输出：通过 AICQ 服务器获取人类主人账号发来的新消息
3.  输出非空 → 注入大模型下一轮迭代

身份管理
--------

- 优先从内存缓存加载
- 缓存未命中 → 从本地文件加载（``~/.aicq-sdk/loop/``）
- 文件不存在 → 新生成本地密钥对并保存

``mySecret`` 函数
-----------------

生成私钥二维码图片，AICQ 扫一扫即可绑定主人关系。
二维码格式：``aicq-master-v1:{signing_sec_hex}:{account_id}:{signing_pub_hex}``
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import logging
from pathlib import Path
from typing import Optional, Dict, Any

import aiohttp

from . import crypto
from .db import Database

logger = logging.getLogger("aicq.loop")

# ─── 默认配置 ──────────────────────────────────────────────────────

DEFAULT_SERVER = "https://aicq.online"
LOOP_DIR = os.path.expanduser("~/.aicq-sdk/loop")
IDENTITY_FILE = os.path.join(LOOP_DIR, "identity.json")


# ─── 身份管理（内存 → 文件 → 创建） ──────────────────────────────

_identity_cache: Optional[Dict[str, Any]] = None


def _load_identity_from_file() -> Optional[Dict[str, Any]]:
    """从本地文件加载身份信息。"""
    if not os.path.exists(IDENTITY_FILE):
        return None
    try:
        with open(IDENTITY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info("从文件加载身份: %s", data.get("account_id", "?")[:16])
        return data
    except Exception as e:
        logger.warning("加载身份文件失败: %s", e)
        return None


def _save_identity_to_file(identity: Dict[str, Any]) -> None:
    """保存身份信息到本地文件。"""
    os.makedirs(LOOP_DIR, exist_ok=True)
    try:
        with open(IDENTITY_FILE, "w", encoding="utf-8") as f:
            json.dump(identity, f, ensure_ascii=False, indent=2)
        # 限制文件权限仅所有者可读写（包含私钥）
        try:
            os.chmod(IDENTITY_FILE, 0o600)
        except Exception:
            pass
        logger.info("身份已保存到: %s", IDENTITY_FILE)
    except Exception as e:
        logger.warning("保存身份文件失败: %s", e)


def _get_or_create_identity(public_key: str = "") -> Dict[str, Any]:
    """获取或创建智能体身份（内存 → 文件 → 创建）。

    Args:
        public_key: 如果已有公钥，尝试使用它加载身份

    Returns:
        身份字典，包含 account_id, signing_pub, signing_sec, exchange_pub, exchange_sec
    """
    global _identity_cache

    # 1. 内存缓存
    if _identity_cache is not None:
        if not public_key or _identity_cache.get("signing_pub") == public_key:
            return _identity_cache

    # 2. 文件加载
    file_identity = _load_identity_from_file()
    if file_identity is not None:
        if not public_key or file_identity.get("signing_pub") == public_key:
            _identity_cache = file_identity
            return file_identity

    # 3. 创建新身份
    signing_pub, signing_sec = crypto.generate_signing_keypair()
    exchange_pub, exchange_sec = crypto.generate_exchange_keypair()

    new_identity = {
        "account_id": "",
        "signing_pub": signing_pub,
        "signing_sec": signing_sec,
        "exchange_pub": exchange_pub,
        "exchange_sec": exchange_sec,
        "created_at": time.time(),
    }

    _save_identity_to_file(new_identity)
    _identity_cache = new_identity
    logger.info("新身份已创建，公钥: %s...", signing_pub[:16])
    return new_identity


def _update_identity_cache(update: Dict[str, Any]) -> None:
    """更新内存和文件中的身份信息。"""
    global _identity_cache
    if _identity_cache is not None:
        _identity_cache.update(update)
        _save_identity_to_file(_identity_cache)


# ─── LoopState — Loop 状态管理 ────────────────────────────────────

class _LoopState:
    """管理 LoopInAICQ 的连接状态。

    维护注册、登录、主人查找、消息收发等生命周期状态，
    支持自动 Token 刷新和重连机制。
    """

    def __init__(self, server: str = DEFAULT_SERVER):
        self.server = server.rstrip("/")
        self.identity: Optional[Dict[str, Any]] = None
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.master_id: Optional[str] = None  # 主人的账户 ID
        self._session: Optional[aiohttp.ClientSession] = None
        self._last_msg_time: str = ""  # 上次拉取消息的 ISO 时间戳（与数据库 created_at 格式一致）
        self._logged_in: bool = False
        self._registered: bool = False
        self._token_expiry: float = 0  # access_token 过期时间（epoch 秒）
        self._login_attempts: int = 0  # 连续登录尝试次数（防无限重试）

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _http_post(self, path: str, data: Dict[str, Any], _retry: bool = True) -> Dict[str, Any]:
        """发送 HTTP POST 请求，支持 401 自动重试。"""
        session = await self._get_session()
        url = f"{self.server}{path}"
        headers = {"Content-Type": "application/json"}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"

        async with session.post(url, json=data, headers=headers) as resp:
            try:
                result = await resp.json()
            except Exception:
                text = await resp.text()
                result = {"error": f"非JSON响应 (HTTP {resp.status})", "raw": text[:200]}

            # 401 时尝试自动刷新 token 并重试一次
            if resp.status == 401 and _retry:
                logger.info("收到 401，尝试刷新认证...")
                try:
                    # 先尝试 refresh token
                    if self.refresh_token:
                        await self._refresh_token()
                    else:
                        # 没有 refresh token，重新登录
                        self._logged_in = False
                        await self.ensure_logged_in()
                    # 重试请求
                    return await self._http_post(path, data, _retry=False)
                except Exception as retry_err:
                    logger.warning("认证刷新失败: %s", retry_err)
                    raise Exception(f"HTTP 401: 认证失败且刷新也失败")

            if resp.status >= 400:
                raise Exception(f"HTTP {resp.status}: {result}")
            return result

    async def _http_get(self, path: str, _retry: bool = True) -> Dict[str, Any]:
        """发送 HTTP GET 请求，支持 401 自动重试。"""
        session = await self._get_session()
        url = f"{self.server}{path}"
        headers = {}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"

        async with session.get(url, headers=headers) as resp:
            try:
                result = await resp.json()
            except Exception:
                text = await resp.text()
                result = {"error": f"非JSON响应 (HTTP {resp.status})", "raw": text[:200]}

            if resp.status == 401 and _retry:
                logger.info("GET 收到 401，尝试刷新认证...")
                try:
                    if self.refresh_token:
                        await self._refresh_token()
                    else:
                        self._logged_in = False
                        await self.ensure_logged_in()
                    return await self._http_get(path, _retry=False)
                except Exception as retry_err:
                    logger.warning("认证刷新失败: %s", retry_err)
                    raise Exception(f"HTTP 401: 认证失败且刷新也失败")

            if resp.status >= 400:
                raise Exception(f"HTTP {resp.status}: {result}")
            return result

    async def _refresh_token(self) -> None:
        """刷新 access_token。"""
        if not self.refresh_token:
            raise Exception("没有 refresh_token")

        session = await self._get_session()
        url = f"{self.server}/api/v1/auth/refresh"
        async with session.post(url, json={"refresh_token": self.refresh_token}) as resp:
            try:
                data = await resp.json()
            except Exception:
                raise Exception(f"Token 刷新返回非JSON (HTTP {resp.status})")
            if resp.status >= 400:
                raise Exception(f"Token 刷新失败: HTTP {resp.status}: {data}")

        self.access_token = data.get("access_token") or data.get("accessToken")
        self.refresh_token = data.get("refresh_token") or data.get("refreshToken", self.refresh_token)
        self._token_expiry = time.time() + 3600  # 假设1小时过期
        logger.info("Token 已刷新")

    async def ensure_registered(self) -> None:
        """确保智能体已注册到 AICQ 服务器。"""
        if self._registered and self.identity.get("account_id"):
            return

        if self.identity is None:
            raise Exception("身份未初始化")

        signing_pub = self.identity["signing_pub"]

        try:
            result = await self._http_post("/api/v1/auth/register/ai", {
                "public_key": signing_pub,
                "agent_name": f"LoopAgent-{signing_pub[:8]}",
            })
            account_id = (
                result.get("account", {}).get("id")
                or result.get("account_id")
                or result.get("accountId")
                or ""
            )
            # 如果注册成功，可能直接返回了 token
            if result.get("access_token"):
                self.access_token = result["access_token"]
                self.refresh_token = result.get("refresh_token", "")
                self._logged_in = True
                self._token_expiry = time.time() + 3600

            if account_id:
                _update_identity_cache({"account_id": account_id})
                self.identity["account_id"] = account_id
                self._registered = True
                logger.info("智能体已注册: %s", account_id)
        except Exception as e:
            # 可能已注册，尝试查找
            logger.warning("注册失败，尝试查找: %s", e)
            try:
                lookup = await self._http_get(
                    f"/api/v1/accounts/lookup?public_key={signing_pub}"
                )
                account_id = lookup.get("account_id") or lookup.get("accountId") or lookup.get("id", "")
                if account_id:
                    _update_identity_cache({"account_id": account_id})
                    self.identity["account_id"] = account_id
                    self._registered = True
                    logger.info("智能体已存在: %s", account_id)
            except Exception as e2:
                logger.error("查找也失败: %s", e2)

    async def ensure_logged_in(self) -> None:
        """确保智能体已登录。支持自动重试和 token 刷新。"""
        # 已登录且 token 未过期
        if self._logged_in and self.access_token and time.time() < self._token_expiry:
            return

        # 尝试先刷新 token
        if self._logged_in and self.refresh_token and self.access_token:
            try:
                await self._refresh_token()
                return
            except Exception as e:
                logger.warning("Token 刷新失败，重新登录: %s", e)
                self._logged_in = False

        if not self.identity or not self.identity.get("signing_sec"):
            raise Exception("无法登录：缺少私钥")

        # 防止无限重试
        self._login_attempts += 1
        if self._login_attempts > 5:
            self._login_attempts = 0
            raise Exception("登录重试次数过多，请检查网络或服务器状态")

        signing_pub = self.identity["signing_pub"]
        signing_sec = self.identity["signing_sec"]

        # 1. 获取挑战
        try:
            challenge_resp = await self._http_post("/api/v1/auth/challenge", {
                "public_key": signing_pub,
            }, _retry=False)
        except Exception as e:
            raise Exception(f"获取挑战失败: {e}")

        challenge = challenge_resp.get("challenge", "")
        if not challenge:
            raise Exception("服务器返回空挑战")

        # 2. 签名挑战
        signature = crypto.sign(challenge, signing_sec)

        # 3. 提交签名
        try:
            login_resp = await self._http_post("/api/v1/auth/login/agent", {
                "public_key": signing_pub,
                "signature": signature,
                "challenge": challenge,
            }, _retry=False)
        except Exception as e:
            raise Exception(f"登录失败: {e}")

        self.access_token = login_resp.get("access_token") or login_resp.get("accessToken")
        self.refresh_token = login_resp.get("refresh_token") or login_resp.get("refreshToken")

        if not self.access_token:
            raise Exception("登录响应中未包含 access_token")

        self._logged_in = True
        self._token_expiry = time.time() + 3600  # 1小时过期
        self._login_attempts = 0  # 重置计数器
        logger.info("智能体已登录: %s", self.identity.get("account_id", "?")[:16])

    async def find_master(self) -> Optional[str]:
        """查找主人（第一个好友，类型为 human）。"""
        if self.master_id:
            return self.master_id

        try:
            result = await self._http_get("/api/v1/friends")
            friends = result.get("friends", [])
            if friends:
                # 优先选择 human 类型的好友
                for f in friends:
                    friend_type = f.get("type", "")
                    friend_id = f.get("friend_id") or f.get("id") or f.get("accountId", "")
                    if friend_type == "human" and friend_id:
                        self.master_id = friend_id
                        return friend_id
                # 如果没有 human 类型，选择第一个
                first = friends[0]
                self.master_id = first.get("friend_id") or first.get("id") or first.get("accountId", "")
                return self.master_id
        except Exception as e:
            logger.warning("查找主人失败: %s", e)

        return None

    async def send_to_master(self, content: str) -> None:
        """发送消息给主人。通过 HTTP API 发送。

        优先使用专用的 /api/v1/agent/loopMessage 端点；
        如失败则尝试通过标准消息 API 发送。
        """
        master_id = await self.find_master()
        if not master_id:
            logger.debug("尚未绑定主人，跳过发送")
            return

        # 截断过长内容（避免超出 HTTP body 限制）
        if len(content) > 10000:
            content = content[:9900] + "\n... (内容过长已截断)"

        try:
            await self._http_post("/api/v1/agent/loopMessage", {
                "agent_public_key": self.identity["signing_pub"],
                "to_id": master_id,
                "content": content,
                "msg_type": "text",
            })
        except Exception as e:
            logger.warning("loopMessage 发送失败: %s，尝试标准消息 API", e)
            # 回退到标准消息端点
            try:
                await self._http_post("/api/v1/friends/message", {
                    "to_id": master_id,
                    "content": content,
                    "type": "text",
                })
            except Exception as e2:
                logger.warning("标准消息 API 也失败: %s", e2)

    async def poll_master_messages(self) -> str:
        """从主人获取新消息。

        时间戳处理：服务器使用 ISO 格式字符串作为 created_at，
        本模块也使用 ISO 格式字符串来跟踪上次拉取位置，确保类型一致。
        首次拉取时 _last_msg_time 为空字符串，获取最近 20 条消息。
        后续拉取只获取上次时间戳之后的新消息。
        """
        master_id = await self.find_master()
        if not master_id:
            return ""

        try:
            payload: Dict[str, Any] = {
                "agent_public_key": self.identity["signing_pub"],
            }
            # 只在有上次拉取时间时才发送 since 参数
            if self._last_msg_time:
                payload["since"] = self._last_msg_time

            result = await self._http_post("/api/v1/agent/loopPoll", payload)
            messages = result.get("messages", [])
            if messages:
                # 提取消息内容
                contents = []
                latest_time = self._last_msg_time
                for msg in messages:
                    content = msg.get("content", "")
                    from_id = msg.get("from_id", "")
                    msg_time = msg.get("timestamp", "")  # ISO 格式字符串

                    if from_id == master_id and content:
                        contents.append(content)

                    # 更新最新时间（ISO 字符串按字典序比较即可）
                    if msg_time and (not latest_time or msg_time > latest_time):
                        latest_time = msg_time

                if latest_time:
                    self._last_msg_time = latest_time

                if contents:
                    return "\n".join(contents)

            return ""
        except Exception as e:
            logger.warning("轮询主人消息失败: %s", e)
            return ""

    async def close(self) -> None:
        """关闭 HTTP 会话。"""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None


# ─── 模块级状态 ────────────────────────────────────────────────────

_loop_state: Optional[_LoopState] = None


async def LoopInAICQ(
    llm_output: str,
    public_key: str = "",
    server: str = DEFAULT_SERVER,
) -> str:
    """智能体迭代 Loop 快速接入函数。

    在每一次 Loop 里面的最后被调用。输入这一轮迭代的大模型输出
    （含工具调用情况和文本）和智能体的公钥，输出通过 AICQ 服务器
    获取人类主人账号发来的新消息。输出非空则注入大模型下一轮迭代。

    身份管理：内存缓存 → 本地文件 → 新建。

    典型用法::

        while True:
            # 大模型推理 + 工具调用
            output = llm.run(...)

            # Loop 末尾调用
            human_msg = await LoopInAICQ(output, agent_pubkey)
            if human_msg:
                # 注入下一轮迭代
                context.append({"role": "user", "content": human_msg})

    Args:
        llm_output: 这一轮迭代的大模型输出（含工具调用情况和文本）
        public_key: 智能体的公钥（为空则自动管理）
        server: AICQ 服务器地址

    Returns:
        主人发来的新消息，空字符串表示无新消息
    """
    global _loop_state

    try:
        # 1. 加载或创建身份
        identity = _get_or_create_identity(public_key)

        # 2. 初始化 Loop 状态
        if _loop_state is None:
            _loop_state = _LoopState(server=server)
        _loop_state.identity = identity

        # 3. 确保已注册并登录
        await _loop_state.ensure_registered()
        await _loop_state.ensure_logged_in()

        # 4. 发送大模型输出给主人
        if llm_output:
            await _loop_state.send_to_master(llm_output)

        # 5. 轮询主人的新消息
        messages = await _loop_state.poll_master_messages()

        return messages

    except Exception as e:
        logger.error("LoopInAICQ 出错: %s", e, exc_info=True)
        return ""


def mySecret(
    output_dir: str = ".",
    server: str = DEFAULT_SERVER,
    agent_name: str = "",
) -> Dict[str, Any]:
    """生成私钥二维码图片，用于 AICQ 扫一扫绑定主人。

    生成包含 ``aicq-master-v1:{signing_sec}:{account_id}:{signing_pub}`` 格式的二维码，
    AICQ 客户端扫描后自动绑定主人关系。

    Args:
        output_dir: 二维码图片保存目录
        server: AICQ 服务器地址（仅记录，不用于网络请求）
        agent_name: 智能体名称（可选，用于文件名和二维码标注）

    Returns:
        包含以下键的字典:
        - ``qr_path``: 二维码图片文件路径
        - ``public_key``: 智能体公钥
        - ``account_id``: 智能体账户 ID（可能为空，需先注册）
        - ``qr_content``: 二维码内容
        - ``fingerprint``: 公钥指纹
    """
    import qrcode

    # 1. 加载或创建身份
    identity = _get_or_create_identity("")

    signing_pub = identity["signing_pub"]
    signing_sec = identity["signing_sec"]
    account_id = identity.get("account_id", "")

    # 2. 构建二维码内容
    #    格式: aicq-master-v1:{signing_sec_hex}:{account_id}:{signing_pub_hex}
    #    扫码时，AICQ 客户端解析此格式，自动：
    #    - 通过 signing_pub 验证身份
    #    - 将扫码者设为该智能体的主人
    qr_content = f"aicq-master-v1:{signing_sec}:{account_id}:{signing_pub}"

    # 3. 生成二维码图片
    os.makedirs(output_dir, exist_ok=True)

    name_part = agent_name or f"agent-{signing_pub[:8]}"
    filename = f"aicq-secret-{name_part}.png"
    qr_path = os.path.join(output_dir, filename)

    qr = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=10,
        border=4,
    )
    qr.add_data(qr_content)
    qr.make(fit=True)

    img = qr.make_image(fill_color="#2D2A26", back_color="#FAF9F6")

    # 在图片底部添加文字标注
    from PIL import Image, ImageDraw, ImageFont

    img = img.convert("RGB")

    # 添加底部空间（增加高度以容纳中英文文字）
    width, height = img.size
    new_height = height + 80
    new_img = Image.new("RGB", (width, new_height), "#FAF9F6")
    new_img.paste(img, (0, 0))

    draw = ImageDraw.Draw(new_img)

    # 第一行：智能体名称 + 账户ID
    label = f"AICQ Agent: {name_part}"
    if account_id:
        label += f" | ID: {account_id}"

    # 尝试加载支持中文的字体
    font = _load_font(14)
    hint_font = _load_font(11)

    bbox = draw.textbbox((0, 0), label, font=font)
    text_width = bbox[2] - bbox[0]
    text_x = (width - text_width) // 2
    draw.text((text_x, height + 8), label, fill="#2D2A26", font=font)

    # 第二行提示（中英双语）
    hint = "AICQ 扫一扫绑定主人 | Scan to bind as master"
    bbox2 = draw.textbbox((0, 0), hint, font=hint_font)
    hint_width = bbox2[2] - bbox2[0]
    hint_x = (width - hint_width) // 2
    draw.text((hint_x, height + 30), hint, fill="#9B958E", font=hint_font)

    # 第三行：服务器地址
    server_hint = f"Server: {server}"
    bbox3 = draw.textbbox((0, 0), server_hint, font=hint_font)
    server_width = bbox3[2] - bbox3[0]
    server_x = (width - server_width) // 2
    draw.text((server_x, height + 50), server_hint, fill="#B8B2AA", font=hint_font)

    new_img.save(qr_path, "PNG")

    # 限制二维码文件权限
    try:
        os.chmod(qr_path, 0o600)
    except Exception:
        pass

    result = {
        "qr_path": os.path.abspath(qr_path),
        "public_key": signing_pub,
        "account_id": account_id,
        "qr_content": qr_content,
        "fingerprint": crypto.compute_fingerprint(signing_pub),
    }

    logger.info("私钥二维码已生成: %s", qr_path)
    print(f"\n{'='*50}")
    print(f"AICQ 智能体私钥二维码")
    print(f"{'='*50}")
    print(f"  公钥:     {signing_pub[:32]}...")
    print(f"  指纹:     {result['fingerprint']}")
    print(f"  账户 ID:  {account_id or '(需先注册)'}")
    print(f"  服务器:   {server}")
    print(f"  二维码:   {os.path.abspath(qr_path)}")
    print(f"{'='*50}")
    print(f"  请在 AICQ 中扫一扫此二维码绑定主人")
    print(f"{'='*50}\n")

    return result


def _load_font(size: int):
    """加载合适的字体，优先支持中文的字体。"""
    from PIL import ImageFont as _ImageFont

    font_paths = [
        "/usr/share/fonts/truetype/chinese/NotoSansSC[wght].ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    ]
    for path in font_paths:
        try:
            return _ImageFont.truetype(path, size)
        except Exception:
            continue
    return _ImageFont.load_default()


async def register_loop_agent(server: str = DEFAULT_SERVER) -> Dict[str, Any]:
    """注册 Loop 智能体到 AICQ 服务器（通常由 LoopInAICQ 自动调用）。

    也可手动调用以提前获取 account_id。

    Args:
        server: AICQ 服务器地址

    Returns:
        包含 account_id 和 public_key 的字典
    """
    identity = _get_or_create_identity("")

    async with aiohttp.ClientSession() as session:
        url = f"{server.rstrip('/')}/api/v1/auth/register/ai"
        payload = {
            "public_key": identity["signing_pub"],
            "agent_name": f"LoopAgent-{identity['signing_pub'][:8]}",
        }
        async with session.post(url, json=payload) as resp:
            result = await resp.json()

        account_id = (
            result.get("account", {}).get("id")
            or result.get("account_id")
            or result.get("accountId")
            or ""
        )

        if account_id:
            _update_identity_cache({"account_id": account_id})
            identity["account_id"] = account_id

    return {
        "account_id": account_id,
        "public_key": identity["signing_pub"],
        "fingerprint": crypto.compute_fingerprint(identity["signing_pub"]),
    }
