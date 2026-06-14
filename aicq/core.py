"""
aicq.core — 核心逻辑模块

提供身份管理、认证、WebSocket 通信、消息收发等核心功能。
支持「我的智能体」（完整密钥对）和「好友智能体」（仅公钥）两种模式，
以及临时房间加入。

同时提供 ``AICQAgentClient`` — 纯 HTTP 的 Agent 工具调用客户端，
适合 LLM 通过 tool-call 链参与临时房间聊天，无需 WebSocket。
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import logging
from typing import Optional, Callable, Dict, Any, List

import aiohttp

from . import crypto
from .db import Database

logger = logging.getLogger("aicq")

# ─── 临时房间密钥持久化 ─────────────────────────────────────────────

EPHEMERAL_DIR = os.path.expanduser("~/.aicq-sdk/ephemeral")


def _ephemeral_key_path(invite_code: str) -> str:
    """根据邀请码返回对应的密钥文件路径。

    每个邀请码对应一个 JSON 文件，保存在 ~/.aicq-sdk/ephemeral/ 目录下。
    文件名由邀请码小写化生成，避免大小写不一致导致重复创建。
    """
    return os.path.join(EPHEMERAL_DIR, f"{invite_code.strip().lower()}.json")


def _load_ephemeral_key(invite_code: str) -> Optional[str]:
    """从本地文件加载临时房间的 private_key。

    如果文件存在且有效，返回保存的 private_key；否则返回 None。
    """
    path = _ephemeral_key_path(invite_code)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        key = data.get("private_key", "")
        if key:
            logger.info("从文件加载临时房间密钥: invite_code=%s", invite_code)
            return key
    except Exception as e:
        logger.warning("加载临时房间密钥文件失败: %s", e)
    return None


def _save_ephemeral_key(invite_code: str, private_key: str, room_id: str = "",
                       display_name: str = "") -> None:
    """将临时房间的 private_key 保存到本地文件。

    保存的内容包括 private_key、room_id、display_name 等，
    方便下次自动复用身份。文件权限限制为仅所有者可读写。
    """
    os.makedirs(EPHEMERAL_DIR, exist_ok=True)
    path = _ephemeral_key_path(invite_code)
    try:
        data = {
            "invite_code": invite_code.strip().upper(),
            "private_key": private_key,
            "room_id": room_id,
            "display_name": display_name,
            "saved_at": time.time(),
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        try:
            os.chmod(path, 0o600)
        except Exception:
            pass
        logger.info("临时房间密钥已保存到: %s", path)
    except Exception as e:
        logger.warning("保存临时房间密钥文件失败: %s", e)


class AICQError(Exception):
    """AICQ SDK 基础异常。"""
    pass


class AuthError(AICQError):
    """认证相关异常。"""
    pass


class AICQConnectionError(AICQError):
    """连接相关异常。"""
    pass


class AICQCore:
    """AICQ SDK 核心。

    管理身份、认证、WebSocket 连接和消息处理。
    """

    def __init__(
        self,
        db_path: str = "~/.aicq-sdk/data.db",
        server: str = "https://aicq.online",
    ):
        self.db = Database(db_path)
        self.server = server.rstrip("/")
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._running: bool = False
        self._ws_task: Optional[asyncio.Task] = None
        self._callbacks: Dict[str, Callable] = {}
        self._pending_requests: Dict[str, asyncio.Future] = {}  # request_id → Future
        self._agent: Optional[Dict[str, Any]] = None
        # 临时房间状态
        self._ephemeral: Optional[Dict[str, Any]] = None
        # 流式输出取消标记: friend_id → bool
        self._stream_cancelled: Dict[str, bool] = {}

    # ─── HTTP 辅助 ──────────────────────────────────────────────

    async def _get_session(self) -> aiohttp.ClientSession:
        """获取或创建 aiohttp 会话。"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _http_get(self, path: str) -> Dict[str, Any]:
        """发送 GET 请求。

        Args:
            path: API 路径（如 /api/v1/friends）

        Returns:
            响应 JSON
        """
        session = await self._get_session()
        url = f"{self.server}{path}"
        headers = {}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"

        async with session.get(url, headers=headers) as resp:
            data = await self._safe_json(resp)
            if resp.status >= 400:
                raise AICQError(f"HTTP {resp.status}: {data}")
            return data

    async def _http_post(self, path: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """发送 POST 请求。

        Args:
            path: API 路径
            data: 请求体 JSON

        Returns:
            响应 JSON
        """
        session = await self._get_session()
        url = f"{self.server}{path}"
        headers = {"Content-Type": "application/json"}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"

        async with session.post(url, json=data, headers=headers) as resp:
            result = await self._safe_json(resp)
            if resp.status >= 400:
                raise AICQError(f"HTTP {resp.status}: {result}")
            return result

    async def _safe_json(self, resp: aiohttp.ClientResponse) -> Any:
        """安全地解析响应 JSON，处理非 JSON 响应。

        当服务器返回非 JSON 响应（如 404 HTML 页面）时，
        提供有意义的错误信息而不是抛出 JSONDecodeError。
        """
        try:
            return await resp.json()
        except (json.JSONDecodeError, aiohttp.ContentTypeError, ValueError):
            text = await resp.text()
            snippet = text[:200] if text else "(empty)"
            logger.warning("非 JSON 响应 (status=%d): %s", resp.status, snippet)
            return {"error": f"服务器返回非JSON响应 (HTTP {resp.status})", "raw": snippet}

    # ─── 身份管理 ───────────────────────────────────────────────

    async def create_my_agent(self, name: str) -> Dict[str, Any]:
        """创建「我的智能体」— 拥有完整密钥对，可注册到服务器。

        Args:
            name: 智能体名称

        Returns:
            智能体信息字典，包含 id, name, public_key 等
        """
        # 1. 生成密钥对
        signing_pub, signing_sec = crypto.generate_signing_keypair()
        exchange_pub, exchange_sec = crypto.generate_exchange_keypair()

        # 2. 注册到服务器
        try:
            result = await self._http_post("/api/v1/auth/register/ai", {
                "public_key": signing_pub,
                "agent_name": name,
            })
            account_id = result.get("account_id") or result.get("accountId") or result.get("id", "")
        except AICQError as e:
            logger.warning("注册失败，可能已存在: %s", e)
            # 如果注册失败，尝试通过 lookup 获取 account_id
            lookup = await self._http_get(f"/api/v1/accounts/lookup?public_key={signing_pub}")
            account_id = lookup.get("account_id") or lookup.get("accountId", "")

        # 3. 保存到本地数据库
        agent_id = self.db.save_agent(
            account_id=account_id,
            name=name,
            agent_type="my",
            signing_pub=signing_pub,
            signing_sec=signing_sec,
            exchange_pub=exchange_pub,
            exchange_sec=exchange_sec,
        )

        # 4. 自动登录
        self._agent = self.db.get_agent(agent_id)
        try:
            await self.login()
        except AuthError as e:
            logger.warning("自动登录失败: %s", e)

        return self._agent

    async def create_friend_agent(
        self, public_key: str, name: str = ""
    ) -> Dict[str, Any]:
        """创建「好友智能体」— 仅持有公钥，通过服务器查找。

        Args:
            public_key: 好友的签名公钥（十六进制）
            name: 好友名称（可选）

        Returns:
            智能体信息字典
        """
        # 1. 在服务器上查找公钥
        try:
            result = await self._http_get(
                f"/api/v1/accounts/lookup?public_key={public_key}"
            )
            account_id = result.get("account_id") or result.get("accountId", "")
            if not name:
                name = result.get("name") or result.get("agent_name", "")
        except AICQError:
            # 查找失败时使用默认值
            account_id = public_key[:16]
            if not name:
                name = f"好友-{account_id[:8]}"

        # 2. 保存到本地数据库（无私钥）
        agent_id = self.db.save_agent(
            account_id=account_id,
            name=name,
            agent_type="friend",
            signing_pub=public_key,
            signing_sec=None,
            exchange_pub=None,
            exchange_sec=None,
        )

        self._agent = self.db.get_agent(agent_id)
        return self._agent

    # ─── 认证 ───────────────────────────────────────────────────

    async def login(self) -> str:
        """通过挑战-应答登录（仅「我的智能体」可用）。

        流程:
            1. POST /api/v1/auth/challenge {public_key}
            2. 使用私钥签名挑战
            3. POST /api/v1/auth/login/agent {public_key, signature, challenge}

        Returns:
            access_token
        """
        agent = self._agent or self.db.get_agent()
        if agent is None:
            raise AuthError("没有可用的智能体，请先创建")

        if agent["type"] != "my":
            raise AuthError("好友智能体无法登录，仅我的智能体支持挑战-应答认证")

        if not agent.get("signing_sec"):
            raise AuthError("缺少签名私钥，无法完成认证")

        public_key = agent["signing_pub"]
        secret_key = agent["signing_sec"]

        # 1. 获取挑战
        try:
            challenge_resp = await self._http_post("/api/v1/auth/challenge", {
                "public_key": public_key,
            })
        except AICQError as e:
            raise AuthError(f"获取挑战失败: {e}")

        challenge = challenge_resp.get("challenge", "")
        if not challenge:
            raise AuthError("服务器返回空挑战")

        # 2. 签名挑战
        # 直接签名原始挑战字符串，不做额外编码
        # 服务器验证时使用相同的原始 challenge
        signature = crypto.sign(challenge, secret_key)

        # 3. 提交签名
        try:
            login_resp = await self._http_post("/api/v1/auth/login/agent", {
                "public_key": public_key,
                "signature": signature,
                "challenge": challenge,
            })
        except AICQError as e:
            raise AuthError(f"登录失败: {e}")

        self.access_token = login_resp.get("access_token") or login_resp.get("accessToken")
        self.refresh_token = login_resp.get("refresh_token") or login_resp.get("refreshToken")

        if not self.access_token:
            raise AuthError("登录响应中未包含 access_token")

        logger.info("登录成功，agent=%s", agent.get("account_id"))
        return self.access_token

    async def refresh_auth(self):
        """刷新 access_token。"""
        if not self.refresh_token:
            raise AuthError("没有 refresh_token，请重新登录")

        session = await self._get_session()
        url = f"{self.server}/api/v1/auth/refresh"
        async with session.post(url, json={"refresh_token": self.refresh_token}) as resp:
            data = await resp.json()
            if resp.status >= 400:
                raise AuthError(f"刷新令牌失败: {data}")

        self.access_token = data.get("access_token") or data.get("accessToken")
        self.refresh_token = data.get("refresh_token") or data.get("refreshToken", self.refresh_token)
        logger.info("令牌刷新成功")

    # ─── WebSocket 连接 ─────────────────────────────────────────

    async def connect(self):
        """连接 WebSocket 并认证。

        连接成功后启动消息接收循环。
        """
        if not self.access_token:
            raise AICQConnectionError("未登录，请先调用 login()")

        agent = self._agent or self.db.get_agent()
        if agent is None:
            raise AICQConnectionError("没有可用的智能体")

        session = await self._get_session()
        ws_url = self.server.replace("https://", "wss://").replace("http://", "ws://")
        ws_url = f"{ws_url}/ws"

        try:
            self.ws = await session.ws_connect(ws_url)
        except Exception as e:
            raise AICQConnectionError(f"WebSocket 连接失败: {e}")

        # 发送上线消息
        online_msg = {
            "type": "online",
            "nodeId": agent["account_id"],
            "token": self.access_token,
        }
        await self.ws.send_json(online_msg)

        self._running = True
        self._ws_task = asyncio.create_task(self._ws_loop())
        logger.info("WebSocket 已连接并认证，agent=%s", agent["account_id"])

    async def connect_ephemeral(self, ephemeral_id: str, room_id: str, token: str):
        """以临时身份连接 WebSocket。

        Args:
            ephemeral_id: 临时成员 ID（如 eph_xxxx）
            room_id: 临时房间 ID
            token: 临时访问 JWT 令牌
        """
        session = await self._get_session()
        ws_url = self.server.replace("https://", "wss://").replace("http://", "ws://")
        ws_url = f"{ws_url}/ws"

        try:
            self.ws = await session.ws_connect(ws_url)
        except Exception as e:
            raise AICQConnectionError(f"WebSocket 连接失败: {e}")

        # 发送临时上线消息（服务器要求 ephemeralId + roomId + token）
        ephemeral_msg = {
            "type": "ephemeral_online",
            "ephemeralId": ephemeral_id,
            "roomId": room_id,
            "token": token,
        }
        await self.ws.send_json(ephemeral_msg)

        self._running = True
        self._ws_task = asyncio.create_task(self._ws_loop())
        logger.info("临时房间 WebSocket 已连接，ephemeral=%s room=%s", ephemeral_id, room_id)

    async def _ws_loop(self):
        """WebSocket 消息接收循环。"""
        if self.ws is None:
            return

        try:
            async for msg in self.ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                        await self._handle_ws_message(data)
                    except json.JSONDecodeError:
                        logger.warning("收到非 JSON 消息: %s", msg.data[:100])
                    except Exception as e:
                        logger.error("处理 WS 消息出错: %s", e, exc_info=True)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error("WebSocket 错误: %s", self.ws.exception())
                    break
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
                    break
        except asyncio.CancelledError:
            pass
        finally:
            self._running = False
            logger.info("WebSocket 循环已退出")

    def _dispatch_callback(self, name: str, data: Dict[str, Any]):
        """分发回调为独立任务，避免阻塞 WS 接收循环。

        使用 asyncio.create_task() 启动异步回调，使 WS 循环可以立即
        继续接收下一条消息，不被慢回调（如 LLM 工具调用）阻塞。
        """
        cb = self._callbacks.get(name)
        if not cb:
            return
        if asyncio.iscoroutinefunction(cb):
            async def _run():
                try:
                    await cb(data)
                except Exception as e:
                    logger.error("回调 %s 出错: %s", name, e)
            asyncio.create_task(_run())
        else:
            try:
                cb(data)
            except Exception as e:
                logger.error("回调 %s 出错: %s", name, e)

    async def _handle_ws_message(self, data: Dict[str, Any]):
        """处理收到的 WebSocket 消息并分发回调。"""
        msg_type = data.get("type", "")

        if msg_type == "message" or msg_type == "private_message":
            # 私聊消息
            # 服务器中转格式: {type: 'message', from: senderId, data: msgObj, delivered: true}
            # msgObj = {id, from_id, to_id, type, content, created_at, status}
            agent = self._agent or self.db.get_agent()
            agent_id = agent["account_id"] if agent else ""
            from_id = data.get("from") or data.get("fromId") or data.get("senderId", "")
            # 从 data 字段中提取实际消息内容（服务器中转时内容在 data.data.content）
            msg_payload = data.get("data", {})
            if isinstance(msg_payload, dict):
                content = msg_payload.get("content", "")
                sub_type = msg_payload.get("type", "text")
            else:
                content = str(msg_payload) if msg_payload else ""
                sub_type = "text"
            # 兼容：如果 data 中没有 content，尝试从顶层获取
            if not content:
                content = data.get("content") or data.get("message", "")
                sub_type = data.get("msg_type", "text")

            if agent_id and from_id:
                self.db.save_message(
                    agent_id=agent_id,
                    chat_id=from_id,
                    is_group=False,
                    from_id=from_id,
                    content=content,
                    msg_type=sub_type,
                )

            # 将提取出的 content 写回 data 字典，方便回调使用
            data["content"] = content
            data["from"] = from_id
            self._dispatch_callback("on_message", data)

        elif msg_type == "group_message":
            # 群组消息
            # 服务器中转格式可能直接有 content 或在 data 中
            agent = self._agent or self.db.get_agent()
            agent_id = agent["account_id"] if agent else ""
            group_id = data.get("groupId") or data.get("group_id") or data.get("room", "")
            from_id = data.get("from") or data.get("fromId") or data.get("senderId", "")
            # 尝试从 data 字段提取内容
            msg_payload = data.get("data", {})
            if isinstance(msg_payload, dict):
                content = msg_payload.get("content", "")
                sub_type = msg_payload.get("type", "text")
            else:
                content = ""
                sub_type = "text"
            # 群组消息服务器可能直接放 content 在顶层
            if not content:
                content = data.get("content") or data.get("message", "")
                sub_type = data.get("msg_type") or data.get("msgType", "text")

            if agent_id and group_id:
                self.db.save_message(
                    agent_id=agent_id,
                    chat_id=group_id,
                    is_group=True,
                    from_id=from_id,
                    content=content,
                    msg_type=sub_type,
                )

            # 将提取出的 content 写回 data 字典
            data["content"] = content
            data["from"] = from_id
            self._dispatch_callback("on_group_message", data)

        elif msg_type == "stream_chunk":
            # 流式消息片段
            self._dispatch_callback("on_stream_chunk", data)

        elif msg_type == "stream_cancel":
            # 用户点击"停止生成"按钮 — 自动设置取消标记
            from_id = data.get("from", "")
            if from_id:
                self._stream_cancelled[from_id] = True
            self._dispatch_callback("on_stream_cancel", data)

        elif msg_type == "stream_cancel_ack":
            # 服务器确认已转发取消请求
            logger.info("流式输出取消已确认，from=%s", data.get("from"))

        elif msg_type == "group_messages":
            # 群组消息历史响应（来自 get_group_messages 请求）
            request_id = data.get("_requestId")
            future = self._pending_requests.pop(request_id, None)
            if future and not future.done():
                future.set_result(data.get("messages", []))

        elif msg_type == "friend_request":
            self._dispatch_callback("on_friend_request", data)

        else:
            logger.debug("收到未处理的消息类型: %s", msg_type)
            self._dispatch_callback("on_raw", data)

    async def listen(self):
        """保持运行，持续接收消息直到连接断开。

        典型用法::

            core = AICQCore()
            await core.connect_ephemeral(...)

            async def on_msg(data):
                print(data["content"])
                await core.send_group_message(room_id, "收到！")

            core.on_group_message(on_msg)
            await core.listen()  # 阻塞直到断开

        也可以使用 ``asyncio.create_task(core.listen())`` 在后台运行，
        然后主协程做其他事情。
        """
        if self._ws_task and not self._ws_task.done():
            await self._ws_task
        else:
            # 没有 WS 任务在运行 — 等待手动关闭
            while self._running:
                await asyncio.sleep(1)

    async def disconnect(self):
        """断开 WebSocket 连接。"""
        self._running = False
        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass

        if self.ws and not self.ws.closed:
            await self.ws.close()
            self.ws = None

        logger.info("已断开连接")

    # ─── 好友管理 ───────────────────────────────────────────────

    async def add_friend(self, account_id: str, message: str = "") -> Dict[str, Any]:
        """发送好友请求。

        Args:
            account_id: 对方账户 ID
            message: 验证消息（可选）

        Returns:
            请求结果
        """
        payload: Dict[str, Any] = {"to_id": account_id}
        if message:
            payload["message"] = message
        return await self._http_post("/api/v1/friends/request", payload)

    async def list_friends(self) -> List[Dict[str, Any]]:
        """获取好友列表。"""
        try:
            result = await self._http_get("/api/v1/friends")
            # 服务器返回 {"friends": [...]}
            friends = result.get("friends") or result.get("data", [])

            # 同步到本地数据库
            agent = self._agent or self.db.get_agent()
            if agent:
                self.db.sync_friends(agent["account_id"], friends)

            return friends
        except AICQError:
            # 离线时从本地返回
            agent = self._agent or self.db.get_agent()
            if agent:
                return self.db.get_friends(agent["account_id"])
            return []

    async def list_friend_requests(self) -> Dict[str, Any]:
        """获取好友请求列表（已发送和已收到）。

        Returns:
            包含 sent 和 received 列表的字典
        """
        return await self._http_get("/api/v1/friends/requests")

    async def reject_friend_request(self, request_id: str) -> Dict[str, Any]:
        """拒绝好友请求。

        Args:
            request_id: 好友请求 ID

        Returns:
            操作结果
        """
        return await self._http_post(
            f"/api/v1/friends/requests/{request_id}/reject", {}
        )

    async def accept_friend_request(self, request_id: str) -> Dict[str, Any]:
        """接受好友请求。

        Args:
            request_id: 好友请求 ID

        Returns:
            操作结果
        """
        return await self._http_post(
            f"/api/v1/friends/requests/{request_id}/accept", {}
        )

    async def delete_friend(self, friend_id: str) -> Dict[str, Any]:
        """删除好友。

        Args:
            friend_id: 好友账户 ID

        Returns:
            操作结果
        """
        session = await self._get_session()
        url = f"{self.server}/api/v1/friends/{friend_id}"
        headers = {"Content-Type": "application/json"}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"

        async with session.delete(url, headers=headers) as resp:
            try:
                data = await resp.json()
            except Exception:
                text = await resp.text()
                if resp.status >= 400:
                    raise AICQError(f"HTTP {resp.status}: {text[:200]}")
                return {"raw": text}
            if resp.status >= 400:
                raise AICQError(f"HTTP {resp.status}: {data}")
            return data

    # ─── 消息收发 ───────────────────────────────────────────────

    async def send_message(self, friend_id: str, content: str):
        """发送私聊消息。

        通过 WebSocket 发送消息。未来可扩展端到端加密。

        Args:
            friend_id: 好友账户 ID
            content: 消息内容
        """
        if self.ws is None or self.ws.closed:
            raise AICQConnectionError("WebSocket 未连接")

        agent = self._agent or self.db.get_agent()
        if agent is None:
            raise AICQError("没有可用的智能体")

        # 构造消息数据对象（与 chat.html 客户端格式一致）
        # 服务器期望: {type: 'message', to: targetId, data: msgObj}
        import uuid as _uuid
        msg_obj = {
            "id": f"msg_{int(time.time() * 1000)}_{_uuid.uuid4().hex[:8]}",
            "from_id": agent["account_id"],
            "to_id": friend_id,
            "type": "text",
            "content": content,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
            "status": "sent",
        }
        msg = {
            "type": "message",
            "to": friend_id,
            "data": msg_obj,
        }
        await self.ws.send_json(msg)

        # 保存到本地记录
        self.db.save_message(
            agent_id=agent["account_id"],
            chat_id=friend_id,
            is_group=False,
            from_id=agent["account_id"],
            content=content,
            msg_type="text",
        )

    async def send_group_message(self, group_id: str, content: str):
        """发送群组消息。

        支持普通智能体和临时房间成员发送。

        Args:
            group_id: 群组 ID（临时房间为 room_id）
            content: 消息内容
        """
        if self.ws is None or self.ws.closed:
            raise AICQConnectionError("WebSocket 未连接")

        # 优先使用临时房间身份，其次使用普通智能体身份
        sender_id = ""
        if self._ephemeral:
            sender_id = self._ephemeral.get("ephemeral_id", "")
        if not sender_id:
            agent = self._agent or self.db.get_agent()
            if agent is None:
                raise AICQError("没有可用的智能体或临时身份")
            sender_id = agent["account_id"]

        msg = {
            "type": "group_message",
            "groupId": group_id,
            "from": sender_id,
            "content": content,
            "msg_type": "text",
            "timestamp": int(time.time() * 1000),
        }
        await self.ws.send_json(msg)

        # 保存到本地记录
        try:
            self.db.save_message(
                agent_id=sender_id,
                chat_id=group_id,
                is_group=True,
                from_id=sender_id,
                content=content,
                msg_type="text",
            )
        except Exception:
            # 临时房间可能无法保存到数据库，忽略
            pass

    # ─── 流式输出 ───────────────────────────────────────────────

    async def send_stream_chunk(
        self,
        friend_id: str,
        chunk_type: str = "text",
        data: Any = "",
    ):
        """发送流式消息片段给好友。

        用于智能体与好友私聊时实时流式输出内容。客户端
        (chat.html) 会根据 chunkType 渲染对应 UI：
        ``text`` / ``reasoning`` / ``thinking`` / ``tool_call`` /
        ``tool_result`` / ``clear_text`` / ``reasoning_end``。

        典型用法::

            # 开始流式输出
            await core.send_stream_chunk(friend_id, "text", "你好")
            await core.send_stream_chunk(friend_id, "text", "，我是AI助手")

            # 工具调用
            await core.send_stream_chunk(friend_id, "tool_call", {
                "name": "web_search",
                "input": {"query": "天气预报"}
            })

            # 工具结果
            await core.send_stream_chunk(friend_id, "tool_result", {
                "output": "今天晴天，25°C"
            })

            # 清除文本缓冲（开始新一轮输出）
            await core.send_stream_chunk(friend_id, "clear_text", "")

            # 结束流式输出
            await core.send_stream_end(friend_id)

        Args:
            friend_id: 好友账户 ID
            chunk_type: 片段类型，支持:
                - ``text``: 文本内容片段
                - ``reasoning``: 推理/思考过程片段
                - ``thinking``: 思考状态标记
                - ``reasoning_end``: 推理结束标记
                - ``clear_text``: 清除当前文本缓冲区（多轮工具调用之间）
                - ``tool_call``: 工具调用，data 为 ``{"name": ..., "input": ...}``
                - ``tool_result``: 工具结果，data 为 ``{"output": ..., "success": ...}``
            data: 片段数据，类型取决于 chunk_type
        """
        if self.ws is None or self.ws.closed:
            raise AICQConnectionError("WebSocket 未连接")

        msg = {
            "type": "stream_chunk",
            "to": friend_id,
            "chunkType": chunk_type,
            "data": data,
        }
        await self.ws.send_json(msg)

    async def send_stream_end(self, friend_id: str, message_id: str = ""):
        """发送流式输出结束信号。

        必须在 ``send_stream_chunk`` 序列的末尾调用，客户端
        据此将流式消息转为最终消息并持久化。

        Args:
            friend_id: 好友账户 ID
            message_id: 可选的消息 ID，用于关联流与最终消息
        """
        if self.ws is None or self.ws.closed:
            raise AICQConnectionError("WebSocket 未连接")

        msg: Dict[str, Any] = {
            "type": "stream_end",
            "to": friend_id,
        }
        if message_id:
            msg["messageId"] = message_id
        await self.ws.send_json(msg)

    def on_stream_cancel(self, callback: Callable):
        """注册流式输出取消回调。

        当用户在前端点击"停止生成"按钮时，服务器会转发
        ``stream_cancel`` 消息给智能体。SDK 会自动设置取消标记
        （可通过 ``is_stream_cancelled()`` 轮询），同时触发此回调。

        智能体通常有两种方式处理取消：

        1. **回调方式**：在回调中设置自定义标记或取消 asyncio Task
        2. **轮询方式**：在 LLM 工具循环中调用 ``is_stream_cancelled()``

        推荐使用轮询方式，因为它不依赖回调的时序。

        Args:
            callback: 回调函数，接收 ``{"from": user_id}`` 字典
        """
        self._callbacks["on_stream_cancel"] = callback

    def is_stream_cancelled(self, friend_id: str) -> bool:
        """检查某个好友是否请求了取消流式输出。

        在 LLM 工具调用循环中轮询此方法，以便及时中止执行。

        典型用法::

            while round < MAX_ROUNDS:
                if core.is_stream_cancelled(friend_id):
                    await core.send_stream_end(friend_id)
                    core.clear_stream_cancel(friend_id)
                    break
                # ... LLM 调用 + 工具执行 ...

        Args:
            friend_id: 好友账户 ID

        Returns:
            True 表示用户已请求取消
        """
        return self._stream_cancelled.get(friend_id, False)

    def clear_stream_cancel(self, friend_id: str) -> None:
        """清除取消标记。

        通常在处理完取消逻辑后调用（发送完 stream_end 之后）。

        Args:
            friend_id: 好友账户 ID
        """
        self._stream_cancelled.pop(friend_id, None)

    async def get_group_messages(
        self, group_id: str, limit: int = 50, before: str = ""
    ) -> List[Dict[str, Any]]:
        """获取群组消息历史。

        通过 WebSocket 发送 get_group_messages 请求，等待服务器返回。
        适用于 Agent 需要查看历史消息或补偿遗漏消息的场景。

        Args:
            group_id: 群组/房间 ID
            limit: 获取数量上限（最大 200）
            before: 游标，获取此时间戳之前的消息

        Returns:
            消息列表
        """
        if not self.ws or self.ws.closed:
            raise AICQError("WebSocket 未连接")

        request_id = f"gm_{int(time.time()*1000)}"
        future = asyncio.get_event_loop().create_future()
        self._pending_requests[request_id] = future

        msg = {
            "type": "get_group_messages",
            "groupId": group_id,
            "limit": limit,
            "_requestId": request_id,
        }
        if before:
            msg["before"] = before

        await self.ws.send_json(msg)

        # 等待服务器响应（最多 10 秒）
        try:
            result = await asyncio.wait_for(future, timeout=10.0)
            return result
        except asyncio.TimeoutError:
            self._pending_requests.pop(request_id, None)
            logger.warning("get_group_messages 超时: %s", group_id)
            return []

    # ─── 群组管理 ───────────────────────────────────────────────

    async def list_groups(self) -> List[Dict[str, Any]]:
        """获取群组列表。"""
        try:
            result = await self._http_get("/api/v1/groups")
            groups = result.get("groups") or result.get("data", [])

            # 同步到本地
            agent = self._agent or self.db.get_agent()
            if agent:
                self.db.sync_groups(agent["account_id"], groups)

            return groups
        except AICQError:
            agent = self._agent or self.db.get_agent()
            if agent:
                return self.db.get_groups(agent["account_id"])
            return []

    async def create_group(
        self, name: str, description: str = ""
    ) -> Dict[str, Any]:
        """创建群组。

        Args:
            name: 群组名称
            description: 群组描述

        Returns:
            群组信息
        """
        return await self._http_post("/api/v1/groups/create", {
            "name": name,
            "description": description,
        })

    # ─── 临时房间 ───────────────────────────────────────────────

    async def join_ephemeral_room(
        self, invite_code: str, display_name: str, private_key: str = ""
    ) -> Dict[str, Any]:
        """加入临时房间（无需注册）。

        如果提供 ``private_key``，将尝试复用已有身份，
        避免创建新的临时成员。服务端会在 /api/v1/ephemeral/join
        中验证 private_key 并返回已有身份信息。

        密钥自动持久化策略（内存 → 文件 → 新建）：

        1. 优先使用传入的 ``private_key`` 参数
        2. 未传入时，尝试从内存缓存 ``_ephemeral`` 中读取
        3. 内存未命中，从本地文件 ``~/.aicq-sdk/ephemeral/{code}.json`` 读取
        4. 都没有时，不传 private_key 让服务器创建新身份
        5. 加入成功后，自动将 private_key 保存到本地文件

        Args:
            invite_code: 邀请码
            display_name: 在房间中的显示名称
            private_key: 之前加入时获得的私钥（可选，用于身份复用）

        Returns:
            包含 ephemeral_id, token, room_id 等信息的字典
        """
        # ─── 自动复用密钥（内存 → 文件） ───
        resolved_key = private_key
        if not resolved_key and self._ephemeral:
            # 内存缓存中有，且邀请码匹配
            if self._ephemeral.get("invite_code", "").upper() == invite_code.strip().upper():
                resolved_key = self._ephemeral.get("raw_token", "")
        if not resolved_key:
            # 从本地文件加载
            file_key = _load_ephemeral_key(invite_code)
            if file_key:
                resolved_key = file_key
                logger.info("自动复用文件中的临时房间密钥: invite_code=%s", invite_code)

        payload = {
            "invite_code": invite_code,
            "display_name": display_name,
        }
        if resolved_key:
            payload["private_key"] = resolved_key

        result = await self._http_post("/api/v1/ephemeral/join", payload)

        # 服务器返回: ephemeral_id, token, room_id, room_name, expires_at, members
        ephemeral_id = result.get("ephemeral_id", "")
        token = result.get("token", "")
        room_id = result.get("room_id", "")
        room_name = result.get("room_name", "")
        expires_at = result.get("expires_at", "")
        members = result.get("members", [])

        # 保存临时状态
        raw_token = result.get("raw_token", token)
        self._ephemeral = {
            "ephemeral_id": ephemeral_id,
            "token": token,
            "raw_token": raw_token,
            "room_id": room_id,
            "room_name": room_name,
            "display_name": display_name,
            "invite_code": invite_code,
            "expires_at": expires_at,
            "members": members,
        }

        # ─── 自动保存密钥到本地文件 ───
        if raw_token:
            _save_ephemeral_key(invite_code, raw_token, room_id, display_name)

        # 连接 WebSocket
        await self.connect_ephemeral(ephemeral_id, room_id, token)

        return {
            "ephemeral_id": ephemeral_id,
            "token": token,
            "raw_token": raw_token,
            "room_id": room_id,
            "room_name": room_name,
            "expires_at": expires_at,
            "members": members,
            "is_rejoin": result.get("is_rejoin", False),
        }

    # ─── 回调注册 ───────────────────────────────────────────────

    def on_message(self, callback: Callable):
        """注册私聊消息回调。

        Args:
            callback: 回调函数，接收消息字典
        """
        self._callbacks["on_message"] = callback

    def on_group_message(self, callback: Callable):
        """注册群组消息回调。

        Args:
            callback: 回调函数，接收消息字典
        """
        self._callbacks["on_group_message"] = callback

    def on_stream_chunk(self, callback: Callable):
        """注册流式消息片段回调。

        Args:
            callback: 回调函数，接收片段字典
        """
        self._callbacks["on_stream_chunk"] = callback

    def on_friend_request(self, callback: Callable):
        """注册好友请求回调。"""
        self._callbacks["on_friend_request"] = callback

    def on_raw(self, callback: Callable):
        """注册原始消息回调（所有未匹配类型的消息）。"""
        self._callbacks["on_raw"] = callback

    # ─── 状态查询 ───────────────────────────────────────────────

    @property
    def is_connected(self) -> bool:
        """WebSocket 是否已连接。"""
        return self._running and self.ws is not None and not self.ws.closed

    @property
    def current_agent(self) -> Optional[Dict[str, Any]]:
        """当前智能体信息。"""
        return self._agent or self.db.get_agent()

    def get_status(self) -> Dict[str, Any]:
        """获取 SDK 状态。"""
        agent = self.current_agent
        return {
            "connected": self.is_connected,
            "agent": agent,
            "ephemeral": self._ephemeral,
            "server": self.server,
        }

    # ─── 临时码 ─────────────────────────────────────────────────

    async def generate_temp_number(self) -> Dict[str, Any]:
        """生成6位临时码。

        生成一个6位数字临时码，其他用户可通过此码添加你为好友。
        临时码有效期为24小时，每次生成会覆盖之前的码。

        Returns:
            包含临时码信息的字典，例如:
            ``{"number": "123456", "expires_at": "2024-01-01T00:00:00Z"}``
        """
        return await self._http_post("/api/v1/temp-number", {})

    async def resolve_temp_number(self, number: str) -> Dict[str, Any]:
        """解析6位临时码，获取对应用户信息。

        Args:
            number: 6位临时码，如 "123456"

        Returns:
            包含用户信息的字典，例如:
            ``{"id": "4622ca50...", "display_name": "小助手", "type": "agent"}``
        """
        return await self._http_get(f"/api/v1/temp-number/{number}")

    async def revoke_temp_number(self, number: str) -> bool:
        """撤销6位临时码。

        Args:
            number: 要撤销的6位临时码

        Returns:
            是否撤销成功
        """
        try:
            session = await self._get_session()
            url = f"{self.server}/api/v1/temp-number/{number}"
            headers = {}
            if self.access_token:
                headers["Authorization"] = f"Bearer {self.access_token}"
            async with session.delete(url, headers=headers) as resp:
                return resp.status < 400
        except Exception as e:
            logger.warning("撤销临时码失败: %s", e)
            return False

    # ─── 文件发送 ──────────────────────────────────────────────

    async def upload_file(self, file_path: str, mime_type: str = "", p2p: bool = False, friend_id: str = "") -> Dict[str, Any]:
        """上传文件到服务器，或以 P2P 模式准备文件数据。

        当 ``p2p=True`` 且文件较小（<= 2MB）时，文件将以 base64 方式通过 WS 直传，
        不占用服务器存储和带宽。此时返回的字典包含 ``media_data`` 字段
        （base64 data URI），而不是服务器 URL。

        上传文件后，需要通过 :meth:`send_file_message` 将文件信息发送给好友。

        Args:
            file_path: 本地文件路径
            mime_type: MIME 类型（可选，自动检测）
            p2p: 是否优先使用 P2P 传输模式（默认 False）
            friend_id: 好友账户 ID（P2P 模式时需要，用于判断是否在线）

        Returns:
            包含文件信息的字典，例如:
            - 服务器模式: ``{"id": "xxx", "url": "/api/v1/chat/files/xxx", "size": 1234, "mimeType": "image/png"}``
            - P2P 模式: ``{"filename": "xxx", "size": 1234, "mimeType": "image/png", "media_data": "data:...;base64,...", "p2p": True}``
        """
        import os
        import mimetypes
        import base64

        if not os.path.isfile(file_path):
            raise AICQError(f"文件不存在: {file_path}")

        filename = os.path.basename(file_path)
        if not mime_type:
            mime_type, _ = mimetypes.guess_type(file_path)
            if not mime_type:
                mime_type = "application/octet-stream"

        # ── P2P mode: read file as base64, skip server upload ──
        P2P_SIZE_THRESHOLD = 2 * 1024 * 1024  # 2MB
        file_size = os.path.getsize(file_path)

        if p2p and file_size <= P2P_SIZE_THRESHOLD:
            with open(file_path, "rb") as f:
                file_bytes = f.read()
            b64_data = base64.b64encode(file_bytes).decode("ascii")
            data_uri = f"data:{mime_type};base64,{b64_data}"
            logger.info("P2P 模式: 文件 %s (%d bytes) 已读取为 base64", filename, file_size)
            return {
                "filename": filename,
                "size": file_size,
                "mimeType": mime_type,
                "media_data": data_uri,
                "p2p": True,
                "url": "",
                "id": "",
                "thumbUrl": "",
            }

        # ── Server upload mode (fallback for offline or large files) ──
        session = await self._get_session()
        url = f"{self.server}/api/v1/chat/upload"
        headers = {}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"

        data = aiohttp.FormData()
        data.add_field(
            "file",
            open(file_path, "rb"),
            filename=filename,
            content_type=mime_type,
        )

        async with session.post(url, data=data, headers=headers) as resp:
            result = await self._safe_json(resp)
            if resp.status >= 400:
                raise AICQError(f"文件上传失败 HTTP {resp.status}: {result}")
            return result

    async def send_file_message(self, friend_id: str, file_info: Dict[str, Any], p2p: bool = False):
        """发送文件消息给好友。

        需要先调用 :meth:`upload_file` 获取 file_info，再调用此方法发送。

        当 ``p2p=True`` 或 file_info 中包含 ``"p2p": True`` 时，
        使用 P2P 模式发送文件（media_data 直传），不占用服务器存储。

        Args:
            friend_id: 好友账户 ID
            file_info: :meth:`upload_file` 返回的文件信息字典
            p2p: 是否使用 P2P 传输模式（默认 False，也可由 file_info["p2p"] 决定）
        """
        if self.ws is None or self.ws.closed:
            raise AICQConnectionError("WebSocket 未连接")

        agent = self._agent or self.db.get_agent()
        if agent is None:
            raise AICQError("没有可用的智能体")

        import uuid as _uuid
        mime_type = file_info.get("mimeType", "application/octet-stream")
        is_image = mime_type.startswith("image/")
        msg_type = "image" if is_image else "file"

        # Determine if P2P mode should be used
        use_p2p = p2p or file_info.get("p2p", False)
        media_data = file_info.get("media_data", "")

        file_content = json.dumps({
            "file_id": file_info.get("id", ""),
            "url": file_info.get("url", ""),
            "filename": file_info.get("filename", ""),
            "size": file_info.get("size", 0),
            "mime_type": mime_type,
            "thumb_url": file_info.get("thumbUrl", ""),
        })

        media_url = file_info.get("url", "")
        if use_p2p and media_data:
            media_url = "p2p:local"

        msg_obj = {
            "id": f"msg_{int(time.time() * 1000)}_{_uuid.uuid4().hex[:8]}",
            "from_id": agent["account_id"],
            "to_id": friend_id,
            "type": msg_type,
            "content": file_content,
            "media_url": media_url,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
            "status": "sent",
        }

        # In P2P mode, attach media_data to the message so the receiver
        # gets the file data directly (no server storage needed)
        if use_p2p and media_data:
            msg_obj["media_data"] = media_data
            msg_obj["transfer_mode"] = "p2p"

        msg = {
            "type": "message",
            "to": friend_id,
            "data": msg_obj,
        }
        # Also set transfer_mode at the top level so the server handler
        # can recognize P2P mode even if msg_data is serialized
        if use_p2p:
            msg["transfer_mode"] = "p2p"

        await self.ws.send_json(msg)

        # 保存到本地记录
        self.db.save_message(
            agent_id=agent["account_id"],
            chat_id=friend_id,
            is_group=False,
            from_id=agent["account_id"],
            content=file_content,
            msg_type=msg_type,
        )

    async def send_file(self, friend_id: str, file_path: str, mime_type: str = "", p2p: bool = False):
        """上传文件并发送给好友（一步完成）。

        这是 :meth:`upload_file` + :meth:`send_file_message` 的快捷方法。

        当 ``p2p=True`` 时，优先使用 P2P 直传模式：小文件（< 2MB）
        通过 base64 直传，零服务器存储；大文件回退到服务器上传。

        Args:
            friend_id: 好友账户 ID
            file_path: 本地文件路径
            mime_type: MIME 类型（可选，自动检测）
            p2p: 是否优先使用 P2P 传输模式（默认 False）
        """
        file_info = await self.upload_file(file_path, mime_type, p2p=p2p, friend_id=friend_id)
        await self.send_file_message(friend_id, file_info, p2p=p2p)

    # ─── 清理 ───────────────────────────────────────────────────

    async def close(self):
        """关闭所有连接和资源。"""
        await self.disconnect()
        if self._session and not self._session.closed:
            await self._session.close()
        self.db.close()
        logger.info("AICQCore 已关闭")


# ═══════════════════════════════════════════════════════════════════════════
#  AICQAgentClient — 纯 HTTP Agent 工具调用客户端
# ═══════════════════════════════════════════════════════════════════════════


class AICQAgentClient:
    """HTTP client for AICQ ephemeral room agent API.

    专为 LLM tool-call 链设计，纯 HTTP 轮询式交互，无需 WebSocket。

    工作流程::

        client = AICQAgentClient()

        # 第一次：进群，获取私钥 + 历史消息 + 成员列表 + 用法说明
        # 私钥会自动保存到本地文件，下次 join 同一邀请码时自动复用
        result = await client.join("RKT22Y", "AI助手")

        # 后续：发言 + 等待回复 + 获取新消息（循环调用）
        result = await client.chat(
            speak=True,
            content="你好！",
            wait_seconds=120,
            since=client.latest_timestamp,
        )

    也可用于同步代码（使用 ``requests``）::

        client = AICQAgentClient()
        result = client.join_sync("RKT22Y", "AI助手")
        result = client.chat_sync(speak=True, content="你好！", wait_seconds=60)

    密钥自动持久化:
        调用 ``join()`` 后，private_key 会自动保存到
        ``~/.aicq-sdk/ephemeral/{邀请码}.json``。
        下次使用相同邀请码调用 ``join()`` 时，SDK 会自动读取
        已保存的密钥，无需手动传入 ``private_key`` 参数，
        从而自动复用已有身份，避免创建新的临时成员。

    属性:
        private_key:  服务器分配的私钥，用于后续 chat 调用身份验证
        ephemeral_id: 临时成员 ID（如 eph_xxxx）
        room_id:      房间 ID
        room_name:    房间名称
        members:      成员列表（用于 @mention）
        latest_timestamp: 最新消息的时间戳（用作下次 chat 的 since 参数）
        expires_at:   房间过期时间
        usage:        后续调用的用法说明
        invite_code:  最近一次加入的邀请码
    """

    def __init__(self, server: str = "https://aicq.online"):
        self.base_url = server.rstrip("/")
        self.private_key: Optional[str] = None
        self.ephemeral_id: Optional[str] = None
        self.room_id: Optional[str] = None
        self.room_name: Optional[str] = None
        self.members: List[Dict[str, Any]] = []
        self.latest_timestamp: Optional[str] = None
        self.expires_at: Optional[str] = None
        self.usage: Optional[Dict[str, Any]] = None
        self.invite_code: Optional[str] = None

    # ─── 异步方法 (aiohttp) ───────────────────────────────────────

    async def join(self, invite_code: str, display_name: str, private_key: str = "") -> Dict[str, Any]:
        """加入临时房间（第一次调用）。

        通过 HTTP POST /api/v1/ephemeral/agent/join 加入房间，
        返回私钥、完整历史消息、成员列表和后续用法说明。

        密钥自动持久化策略（显式参数 → 内存 → 文件 → 新建）：

        1. 优先使用传入的 ``private_key`` 参数
        2. 未传入时，尝试从内存缓存（上次 join 的结果）中读取
        3. 内存未命中，从本地文件 ``~/.aicq-sdk/ephemeral/{code}.json`` 读取
        4. 都没有时，不传 private_key 让服务器创建新身份
        5. 加入成功后，自动将 private_key 保存到本地文件

        Args:
            invite_code: 房间邀请码（如 RKT22Y）
            display_name: 在房间中的显示昵称
            private_key: 之前加入时获得的私钥（可选，用于身份复用）

        Returns:
            包含 private_key, ephemeral_id, room_id, room_name,
            members, history, usage, is_rejoin 的字典

        Raises:
            AICQError: 加入失败
        """
        code_upper = invite_code.strip().upper()

        # ─── 自动复用密钥（内存 → 文件） ───
        resolved_key = private_key.strip() if private_key else ""
        if not resolved_key and self.invite_code and self.invite_code.upper() == code_upper:
            # 内存缓存中有，且邀请码匹配
            resolved_key = self.private_key or ""
        if not resolved_key:
            # 从本地文件加载
            file_key = _load_ephemeral_key(invite_code)
            if file_key:
                resolved_key = file_key
                logger.info("自动复用文件中的临时房间密钥: invite_code=%s", code_upper)

        async with aiohttp.ClientSession() as session:
            url = f"{self.base_url}/api/v1/ephemeral/agent/join"
            payload = {
                "invite_code": code_upper,
                "display_name": display_name.strip(),
            }
            if resolved_key:
                payload["private_key"] = resolved_key
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                data = await resp.json()
                if resp.status != 200:
                    raise AICQError(f"加入失败: {data.get('error', 'Unknown error')}")

        # 保存状态
        self.private_key = data["private_key"]
        self.ephemeral_id = data["ephemeral_id"]
        self.room_id = data["room_id"]
        self.room_name = data.get("room_name", "")
        self.members = data.get("members", [])
        self.expires_at = data.get("expires_at", "")
        self.usage = data.get("usage")
        self.invite_code = code_upper

        # 设置 latest_timestamp 为最后一条消息的时间戳
        history = data.get("history", [])
        if history:
            self.latest_timestamp = history[-1].get("timestamp", "")

        # ─── 自动保存密钥到本地文件 ───
        if self.private_key:
            _save_ephemeral_key(code_upper, self.private_key, self.room_id, display_name.strip())

        logger.info(
            "Agent joined room: %s (%s) in %s, history=%d msgs, is_rejoin=%s",
            self.ephemeral_id, display_name, self.room_id, len(history),
            data.get("is_rejoin", False),
        )
        return data

    async def chat(
        self,
        speak: bool = False,
        content: str = "",
        wait_seconds: int = 0,
        since: str = "",
    ) -> Dict[str, Any]:
        """发送消息和/或获取聊天记录（后续调用）。

        通过 HTTP POST /api/v1/ephemeral/agent/chat 与房间交互。
        可以选择发言、等待回复、获取新消息。

        Args:
            speak: 是否发送消息
            content: 消息内容（speak=False 时可为空）
            wait_seconds: 发送后等待回复的秒数（0-300），等待期间收到的回复会包含在返回结果中
            since: 获取聊天记录的起始时间点（ISO 格式），通常使用上次返回的最后一条消息的 timestamp

        Returns:
            包含 messages, members, expires_at, your_message, latest_timestamp 的字典

        Raises:
            AICQError: 调用失败（如私钥无效、房间过期）
        """
        if not self.private_key:
            raise AICQError("尚未加入房间，请先调用 join()")

        timeout_val = max(30, wait_seconds + 30)
        async with aiohttp.ClientSession() as session:
            url = f"{self.base_url}/api/v1/ephemeral/agent/chat"
            payload = {
                "private_key": self.private_key,
                "speak": speak,
                "content": content,
                "wait_seconds": wait_seconds,
                "since": since or self.latest_timestamp or "",
            }
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=timeout_val)) as resp:
                data = await resp.json()
                if resp.status != 200:
                    raise AICQError(f"Chat 失败: {data.get('error', 'Unknown error')}")

        # 更新状态
        self.members = data.get("members", self.members)
        self.latest_timestamp = data.get("latest_timestamp", self.latest_timestamp)

        return data

    # ─── 同步方法 (requests) ──────────────────────────────────────

    def join_sync(self, invite_code: str, display_name: str, private_key: str = "") -> Dict[str, Any]:
        """加入临时房间（同步版本，使用 requests 库）。

        参数和返回值与 ``join()`` 相同，适用于非 asyncio 环境。
        同样支持密钥自动持久化（内存 → 文件 → 新建）。
        """
        import requests

        code_upper = invite_code.strip().upper()

        # ─── 自动复用密钥（内存 → 文件） ───
        resolved_key = private_key.strip() if private_key else ""
        if not resolved_key and self.invite_code and self.invite_code.upper() == code_upper:
            resolved_key = self.private_key or ""
        if not resolved_key:
            file_key = _load_ephemeral_key(invite_code)
            if file_key:
                resolved_key = file_key
                logger.info("自动复用文件中的临时房间密钥: invite_code=%s", code_upper)

        url = f"{self.base_url}/api/v1/ephemeral/agent/join"
        payload = {
            "invite_code": code_upper,
            "display_name": display_name.strip(),
        }
        if resolved_key:
            payload["private_key"] = resolved_key
        resp = requests.post(url, json=payload, timeout=30)
        data = resp.json()
        if resp.status_code != 200:
            raise AICQError(f"加入失败: {data.get('error', 'Unknown error')}")

        self.private_key = data["private_key"]
        self.ephemeral_id = data["ephemeral_id"]
        self.room_id = data["room_id"]
        self.room_name = data.get("room_name", "")
        self.members = data.get("members", [])
        self.expires_at = data.get("expires_at", "")
        self.usage = data.get("usage")
        self.invite_code = code_upper

        history = data.get("history", [])
        if history:
            self.latest_timestamp = history[-1].get("timestamp", "")

        # ─── 自动保存密钥到本地文件 ───
        if self.private_key:
            _save_ephemeral_key(code_upper, self.private_key, self.room_id, display_name.strip())

        return data

    def chat_sync(
        self,
        speak: bool = False,
        content: str = "",
        wait_seconds: int = 0,
        since: str = "",
    ) -> Dict[str, Any]:
        """发送消息和/或获取聊天记录（同步版本，使用 requests 库）。

        参数和返回值与 ``chat()`` 相同，适用于非 asyncio 环境。
        """
        import requests

        if not self.private_key:
            raise AICQError("尚未加入房间，请先调用 join_sync()")

        url = f"{self.base_url}/api/v1/ephemeral/agent/chat"
        payload = {
            "private_key": self.private_key,
            "speak": speak,
            "content": content,
            "wait_seconds": wait_seconds,
            "since": since or self.latest_timestamp or "",
        }

        timeout_val = max(30, wait_seconds + 30)
        resp = requests.post(url, json=payload, timeout=timeout_val)
        data = resp.json()
        if resp.status_code != 200:
            raise AICQError(f"Chat 失败: {data.get('error', 'Unknown error')}")

        self.members = data.get("members", self.members)
        self.latest_timestamp = data.get("latest_timestamp", self.latest_timestamp)

        return data
