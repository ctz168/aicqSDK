"""
aicq.loop — 智能体 Loop 快速接入模块

提供 ``startLoop`` 和 ``mySecret`` 两个核心函数，让任何 AI 智能体
只需一行代码即可通过 WebSocket 实时接入 AICQ，自动上线、收发消息。

核心设计
--------

``startLoop`` 是最简洁的接入方式::

    from aicq import startLoop

    async def on_message(content, from_id):
        return "收到: " + content   # 返回值自动回复

    asyncio.run(startLoop(on_message))   # 一行启动!

四行代码接入法
--------------

1.  ``from aicq import startLoop`` — import
2.  ``async def on_message(content, from_id):`` — 定义回调
3.  ``return "回复内容"`` — 返回值自动回复 (返回 None 则不回复)
4.  ``asyncio.run(startLoop(on_message))`` — 启动!

工作原理
--------

调用 ``startLoop(on_message)`` 后，SDK 自动完成：

1.  加载或创建身份（内存 → 文件 → 新建密钥对）
2.  注册到 AICQ 服务器
3.  挑战-应答登录
4.  建立 WebSocket 连接
5.  发送 ``online`` 消息上线
6.  进入消息循环

收到好友消息时，调用你的 ``on_message(content, from_id)`` 异步回调，
返回值（字符串）自动通过 WebSocket 发送回消息来源。
返回 ``None`` 则不自动回复。

内置 30 秒心跳 ping 保活，断线自动重连。

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
import uuid as _uuid
from collections import OrderedDict
from typing import Optional, Dict, Any, Callable, Awaitable

import aiohttp

from . import crypto
from .db import Database

logger = logging.getLogger("aicq.loop")

# ─── 默认配置 ──────────────────────────────────────────────────────

DEFAULT_SERVER = "https://aicq.online"
LOOP_DIR = os.path.expanduser("~/.aicq-sdk/loop")
IDENTITY_FILE = os.path.join(LOOP_DIR, "identity.json")

# 心跳间隔（秒）
PING_INTERVAL = 30

# 重连间隔（秒），指数退避
RECONNECT_BASE_DELAY = 2
RECONNECT_MAX_DELAY = 60


# ─── 消息去重 LRU 缓存 ────────────────────────────────────────────

class _LRUSet:
    """固定容量的 LRU 集合，用于消息去重。"""

    def __init__(self, maxsize: int = 1000):
        self._data: OrderedDict = OrderedDict()
        self._maxsize = maxsize

    def add(self, key: str) -> None:
        """添加 key，如已存在则移到末尾。"""
        if key in self._data:
            self._data.move_to_end(key)
        else:
            self._data[key] = True
            if len(self._data) > self._maxsize:
                self._data.popitem(last=False)  # 移除最旧的

    def __contains__(self, key: str) -> bool:
        return key in self._data


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


# ─── HTTP 辅助函数（注册/登录用） ────────────────────────────────

async def _http_post(
    session: aiohttp.ClientSession,
    url: str,
    data: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """发送 HTTP POST 请求。"""
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)
    async with session.post(url, json=data, headers=hdrs) as resp:
        try:
            result = await resp.json()
        except Exception:
            text = await resp.text()
            result = {"error": f"非JSON响应 (HTTP {resp.status})", "raw": text[:200]}
        if resp.status >= 400:
            raise Exception(f"HTTP {resp.status}: {result}")
        return result


async def _http_get(
    session: aiohttp.ClientSession,
    url: str,
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """发送 HTTP GET 请求。"""
    hdrs = {}
    if headers:
        hdrs.update(headers)
    async with session.get(url, headers=hdrs) as resp:
        try:
            result = await resp.json()
        except Exception:
            text = await resp.text()
            result = {"error": f"非JSON响应 (HTTP {resp.status})", "raw": text[:200]}
        if resp.status >= 400:
            raise Exception(f"HTTP {resp.status}: {result}")
        return result


# ─── 注册 + 登录 ────────────────────────────────────────────────

async def _ensure_registered(
    session: aiohttp.ClientSession,
    server: str,
    identity: Dict[str, Any],
) -> None:
    """确保智能体已注册到 AICQ 服务器。"""
    if identity.get("account_id"):
        return

    signing_pub = identity["signing_pub"]

    try:
        result = await _http_post(session, f"{server}/api/v1/auth/register/ai", {
            "public_key": signing_pub,
            "agent_name": f"LoopAgent-{signing_pub[:8]}",
        })
        account_id = (
            result.get("account", {}).get("id")
            or result.get("account_id")
            or result.get("accountId")
            or ""
        )
        if account_id:
            _update_identity_cache({"account_id": account_id})
            identity["account_id"] = account_id
            logger.info("智能体已注册: %s", account_id)
    except Exception as e:
        # 可能已注册，尝试查找
        logger.warning("注册失败，尝试查找: %s", e)
        try:
            lookup = await _http_get(
                session, f"{server}/api/v1/accounts/lookup?public_key={signing_pub}"
            )
            account_id = lookup.get("account_id") or lookup.get("accountId") or lookup.get("id", "")
            if account_id:
                _update_identity_cache({"account_id": account_id})
                identity["account_id"] = account_id
                logger.info("智能体已存在: %s", account_id)
        except Exception as e2:
            logger.error("查找也失败: %s", e2)


async def _login(
    session: aiohttp.ClientSession,
    server: str,
    identity: Dict[str, Any],
) -> str:
    """挑战-应答登录，返回 access_token。"""
    signing_pub = identity["signing_pub"]
    signing_sec = identity["signing_sec"]

    # 1. 获取挑战
    challenge_resp = await _http_post(session, f"{server}/api/v1/auth/challenge", {
        "public_key": signing_pub,
    })
    challenge = challenge_resp.get("challenge", "")
    if not challenge:
        raise Exception("服务器返回空挑战")

    # 2. 签名挑战
    signature = crypto.sign(challenge, signing_sec)

    # 3. 提交签名
    login_resp = await _http_post(session, f"{server}/api/v1/auth/login/agent", {
        "public_key": signing_pub,
        "signature": signature,
        "challenge": challenge,
    })

    access_token = login_resp.get("access_token") or login_resp.get("accessToken")
    if not access_token:
        raise Exception("登录响应中未包含 access_token")

    logger.info("智能体已登录: %s", identity.get("account_id", "?")[:16])
    return access_token


# ─── startLoop — WebSocket 实时模式 ──────────────────────────────

async def startLoop(
    on_message: Callable[[str, str], Awaitable[Optional[str]]],
    identity: Optional[Dict[str, Any]] = None,
    public_key: str = "",
    server: str = DEFAULT_SERVER,
    on_group_message: Optional[Callable[[str, str, str], Awaitable[None]]] = None,
    on_error: Optional[Callable[[Exception], Awaitable[None]]] = None,
    on_presence: Optional[Callable[[str, str], Awaitable[None]]] = None,
    auto_reconnect: bool = True,
) -> None:
    """启动 WebSocket 实时模式，智能体自动上线，收到消息时调用回调。

    这是 aicqSDK 最简洁的接入方式 —— 四行代码接入法::

        from aicq import startLoop

        async def on_message(content, from_id):
            return "收到: " + content   # 返回值自动回复

        asyncio.run(startLoop(on_message))   # 一行启动!

    工作原理
    ~~~~~~~~

    调用 ``startLoop(on_message)`` 后，SDK 自动完成：

    1. 加载或创建身份（内存 → 文件 → 新建密钥对）
    2. 注册到 AICQ 服务器
    3. 挑战-应答登录
    4. 建立 WebSocket 连接
    5. 发送 ``online`` 消息上线
    6. 进入消息循环

    收到好友消息时，调用你的 ``on_message(content, from_id)`` 异步回调，
    返回值（字符串）自动通过 WebSocket 发送回消息来源。
    返回 ``None`` 则不自动回复。

    内置 30 秒心跳 ping 保活，断线自动重连（指数退避，2s→4s→8s→...→60s）。

    高级用法
    ~~~~~~~~

    支持群组消息回调::

        async def on_group_msg(content, from_id, group_id):
            print(f"[群:{group_id}] {from_id}: {content}")

        asyncio.run(startLoop(on_message, on_group_message=on_group_msg))

    支持错误回调::

        async def on_error(exc):
            logger.error("SDK错误: %s", exc)

        asyncio.run(startLoop(on_message, on_error=on_error))

    Args:
        on_message: 异步回调函数，签名 ``async def on_message(content: str, from_id: str) -> str|None``
            - content: 收到的消息内容
            - from_id: 发送者的账户 ID
            - 返回字符串则自动回复，返回 None 则不回复
        identity: 智能体身份字典（为空则自动管理，首次运行自动创建）。
            格式：{account_id, signing_pub, signing_sec, exchange_pub, exchange_sec}
        public_key: 智能体公钥（identity 和 public_key 都为空则自动管理）
        server: AICQ 服务器地址
        on_group_message: 群组消息异步回调，签名 ``async def on_group_message(content, from_id, group_id)``
        on_error: 错误异步回调，签名 ``async def on_error(exception)``
        on_presence: 好友上下线回调，签名 ``async def on_presence(account_id, status)``
        auto_reconnect: 是否在断线后自动重连（默认 True）

    Returns:
        该函数会阻塞运行直到 WebSocket 断开且不再重连。
        如果需要后台运行，可使用 ``asyncio.create_task(startLoop(...))``。
    """
    server = server.rstrip("/")

    # ── 1. 加载或创建身份 ──
    if identity:
        # 使用传入的身份
        if not identity.get("account_id") or not identity.get("signing_sec"):
            # 传入的身份不完整，尝试从文件加载或创建
            agent_identity = _get_or_create_identity(public_key)
            # 合并传入的字段
            for k, v in identity.items():
                if v:
                    agent_identity[k] = v
            identity = agent_identity
    else:
        identity = _get_or_create_identity(public_key)

    # 写入全局缓存，确保后续调用能复用
    global _identity_cache
    _identity_cache = identity

    if not identity.get("signing_sec"):
        raise Exception("身份缺少 signing_sec，无法登录")

    logger.info(
        "智能体身份: account_id=%s pub=%s...",
        identity.get("account_id", "?")[:16],
        identity["signing_pub"][:16],
    )

    # ── 2. 注册 + 登录 + WebSocket 连接（含重连循环） ──
    reconnect_delay = RECONNECT_BASE_DELAY

    while True:
        access_token = None
        try:
            # 注册 + 登录
            async with aiohttp.ClientSession() as http_session:
                await _ensure_registered(http_session, server, identity)
                access_token = await _login(http_session, server, identity)

            # WebSocket 连接和消息循环
            ws_url = server.replace("https://", "wss://").replace("http://", "ws://")
            ws_url = f"{ws_url}/ws"

            async with aiohttp.ClientSession() as ws_session:
                logger.info("正在连接 WebSocket: %s", ws_url)
                try:
                    ws = await ws_session.ws_connect(ws_url)
                except Exception as e:
                    raise Exception(f"WebSocket 连接失败: {e}")

                # 发送上线消息
                online_msg = {
                    "type": "online",
                    "nodeId": identity["account_id"],
                    "token": access_token,
                }
                await ws.send_json(online_msg)
                logger.info("已发送 online 消息，智能体上线: %s", identity["account_id"])

                # 重置重连延迟
                reconnect_delay = RECONNECT_BASE_DELAY

                # ── 消息循环 ──
                last_ping = time.time()
                message_ids_seen = _LRUSet(maxsize=1000)

                try:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                            except json.JSONDecodeError:
                                logger.warning("收到非 JSON 消息: %s", msg.data[:100])
                                continue

                            msg_type = data.get("type", "")

                            # ── 处理私聊消息 ──
                            if msg_type in ("message", "private_message"):
                                from_id = data.get("from") or data.get("fromId") or data.get("senderId", "")
                                msg_payload = data.get("data", {})
                                if isinstance(msg_payload, dict):
                                    content = msg_payload.get("content", "")
                                else:
                                    content = str(msg_payload) if msg_payload else ""
                                if not content:
                                    content = data.get("content") or data.get("message", "")

                                # 消息去重
                                msg_id = msg_payload.get("id", "") if isinstance(msg_payload, dict) else ""
                                if msg_id and msg_id in message_ids_seen:
                                    continue
                                if msg_id:
                                    message_ids_seen.add(msg_id)

                                if content and from_id:
                                    try:
                                        reply = await on_message(content, from_id)
                                        if reply and isinstance(reply, str):
                                            reply_msg = {
                                                "type": "message",
                                                "to": from_id,
                                                "data": {
                                                    "id": f"msg_{int(time.time() * 1000)}_{_uuid.uuid4().hex[:8]}",
                                                    "from_id": identity["account_id"],
                                                    "to_id": from_id,
                                                    "type": "text",
                                                    "content": reply,
                                                    "created_at": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
                                                    "status": "sent",
                                                },
                                            }
                                            await ws.send_json(reply_msg)
                                            logger.info("自动回复给 %s: %s", from_id[:16], reply[:80])
                                    except Exception as e:
                                        logger.error("回调处理出错: %s", e, exc_info=True)
                                        if on_error:
                                            try:
                                                await on_error(e)
                                            except Exception:
                                                pass

                            # ── 处理群组消息 ──
                            elif msg_type == "group_message":
                                from_id = data.get("from") or data.get("fromId") or data.get("senderId", "")
                                group_id = data.get("groupId") or data.get("group_id") or data.get("room", "")
                                msg_payload = data.get("data", {})
                                if isinstance(msg_payload, dict):
                                    content = msg_payload.get("content", "")
                                else:
                                    content = ""
                                if not content:
                                    content = data.get("content") or data.get("message", "")

                                if content and on_group_message:
                                    try:
                                        await on_group_message(content, from_id, group_id)
                                    except Exception as e:
                                        logger.error("群组消息回调出错: %s", e, exc_info=True)
                                        if on_error:
                                            try:
                                                await on_error(e)
                                            except Exception:
                                                pass

                            # ── 处理 presence 事件（好友上下线） ──
                            elif msg_type == "presence":
                                from_id = data.get("from") or data.get("nodeId", "")
                                status = data.get("status", "")
                                logger.info("好友 %s %s", from_id[:16] if from_id else "?", status)
                                if on_presence:
                                    try:
                                        await on_presence(from_id, status)
                                    except Exception as e:
                                        logger.error("presence 回调出错: %s", e)

                            # ── 心跳 ping 响应 ──
                            elif msg_type == "pong":
                                logger.debug("收到 pong")

                            else:
                                logger.debug("收到未处理的消息类型: %s", msg_type)

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logger.error("WebSocket 错误: %s", ws.exception())
                            break

                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
                            break

                        # ── 心跳 ping ──
                        now = time.time()
                        if now - last_ping >= PING_INTERVAL:
                            try:
                                await ws.send_json({"type": "ping"})
                                last_ping = now
                                logger.debug("发送 ping")
                            except Exception:
                                break

                except asyncio.CancelledError:
                    logger.info("startLoop 被取消")
                    return  # 被取消时不重连
                finally:
                    if not ws.closed:
                        await ws.close()
                    logger.info("WebSocket 已断开: %s", identity.get("account_id", "?")[:16])

        except asyncio.CancelledError:
            logger.info("startLoop 被取消")
            return  # 被取消时不重连
        except Exception as e:
            logger.error("startLoop 出错: %s", e, exc_info=True)
            if on_error:
                try:
                    await on_error(e)
                except Exception:
                    pass

        # ── 断线重连 ──
        if not auto_reconnect:
            logger.info("auto_reconnect=False，退出")
            return

        logger.info("将在 %d 秒后重连...", reconnect_delay)
        try:
            await asyncio.sleep(reconnect_delay)
        except asyncio.CancelledError:
            logger.info("重连等待被取消")
            return

        # 指数退避
        reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_DELAY)


# ─── mySecret 函数 ──────────────────────────────────────────────

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
    """注册 Loop 智能体到 AICQ 服务器（通常由 startLoop 自动调用）。

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
