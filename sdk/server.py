"""
aicq.server — HTTP API 服务器

基于 aiohttp 的轻量 REST API 服务，监听端口 16109，
为外部工具提供与 SDK 交互的 HTTP 接口。
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from aiohttp import web

from .core import AICQCore

logger = logging.getLogger("aicq.server")

DEFAULT_PORT = 16109


class APIServer:
    """AICQ SDK HTTP API 服务器。"""

    def __init__(self, core: AICQCore, port: int = DEFAULT_PORT):
        self.core = core
        self.port = port
        self.app = web.Application()
        self._setup_routes()

    def _setup_routes(self):
        """注册所有路由。"""
        r = self.app.router

        # 状态
        r.add_get("/api/status", self._handle_status)

        # 智能体
        r.add_get("/api/agents", self._handle_list_agents)
        r.add_post("/api/agents", self._handle_create_agent)
        r.add_post("/api/agents/switch", self._handle_switch_agent)

        # 好友
        r.add_get("/api/friends", self._handle_list_friends)
        r.add_post("/api/friends/request", self._handle_add_friend)
        r.add_get("/api/friends/requests", self._handle_list_friend_requests)
        r.add_post("/api/friends/requests/{request_id}/accept", self._handle_accept_friend_request)
        r.add_post("/api/friends/requests/{request_id}/reject", self._handle_reject_friend_request)

        # 消息
        r.add_post("/api/chat/send", self._handle_send_message)
        r.add_post("/api/groups/message", self._handle_send_group_message)

        # 群组
        r.add_get("/api/groups", self._handle_list_groups)

        # 临时房间
        r.add_post("/api/ephemeral/join", self._handle_ephemeral_join)

    # ─── 路由处理 ───────────────────────────────────────────────

    async def _handle_status(self, request: web.Request) -> web.Response:
        """GET /api/status — 获取连接状态和当前智能体。"""
        status = self.core.get_status()
        return web.json_response(status)

    async def _handle_list_agents(self, request: web.Request) -> web.Response:
        """GET /api/agents — 列出所有智能体。"""
        agents = self.core.db.list_agents()
        # 隐藏私钥信息
        safe_agents = []
        for a in agents:
            safe = {
                "id": a["id"],
                "account_id": a["account_id"],
                "name": a["name"],
                "type": a["type"],
                "signing_pub": a["signing_pub"],
                "is_current": bool(a["is_current"]),
                "created_at": a["created_at"],
            }
            safe_agents.append(safe)
        return web.json_response({"agents": safe_agents})

    async def _handle_create_agent(self, request: web.Request) -> web.Response:
        """POST /api/agents — 创建智能体。

        Body: {"name": "xxx", "type": "my"|"friend", "public_key": "xxx"}
        """
        try:
            data = await request.json()
        except json.JSONDecodeError:
            return web.json_response({"error": "无效的 JSON"}, status=400)

        name = data.get("name", "")
        agent_type = data.get("type", "my")
        public_key = data.get("public_key", "")

        if not name:
            return web.json_response({"error": "缺少 name 参数"}, status=400)

        try:
            if agent_type == "friend" and public_key:
                agent = await self.core.create_friend_agent(public_key, name)
            else:
                agent = await self.core.create_my_agent(name)

            return web.json_response({
                "ok": True,
                "agent": {
                    "id": agent["id"],
                    "account_id": agent["account_id"],
                    "name": agent["name"],
                    "type": agent["type"],
                    "signing_pub": agent["signing_pub"],
                },
            })
        except Exception as e:
            logger.error("创建智能体失败: %s", e)
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_switch_agent(self, request: web.Request) -> web.Response:
        """POST /api/agents/switch — 切换当前智能体。

        Body: {"agent_id": "xxx"}
        """
        try:
            data = await request.json()
        except json.JSONDecodeError:
            return web.json_response({"error": "无效的 JSON"}, status=400)

        agent_id = data.get("agent_id", "")
        if not agent_id:
            return web.json_response({"error": "缺少 agent_id"}, status=400)

        success = self.core.db.set_current(agent_id)
        if success:
            self.core._agent = self.core.db.get_agent(agent_id)
            return web.json_response({"ok": True, "agent_id": agent_id})
        else:
            return web.json_response({"error": "智能体不存在"}, status=404)

    async def _handle_list_friends(self, request: web.Request) -> web.Response:
        """GET /api/friends — 列出好友。"""
        try:
            friends = await self.core.list_friends()
            return web.json_response({"friends": friends})
        except Exception as e:
            logger.error("获取好友列表失败: %s", e)
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_add_friend(self, request: web.Request) -> web.Response:
        """POST /api/friends/request — 发送好友请求。

        Body: {"to_id": "xxx", "message": "xxx"}
        """
        try:
            data = await request.json()
        except json.JSONDecodeError:
            return web.json_response({"error": "无效的 JSON"}, status=400)

        account_id = data.get("to_id") or data.get("account_id", "")
        message = data.get("message", "")
        if not account_id:
            return web.json_response({"error": "缺少 to_id"}, status=400)

        try:
            result = await self.core.add_friend(account_id, message=message)
            return web.json_response({"ok": True, "result": result})
        except Exception as e:
            logger.error("添加好友失败: %s", e)
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_list_friend_requests(self, request: web.Request) -> web.Response:
        """GET /api/friends/requests — 列出好友请求。"""
        try:
            result = await self.core.list_friend_requests()
            return web.json_response(result)
        except Exception as e:
            logger.error("获取好友请求失败: %s", e)
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_accept_friend_request(self, request: web.Request) -> web.Response:
        """POST /api/friends/requests/{request_id}/accept — 接受好友请求。"""
        request_id = request.match_info.get("request_id", "")
        if not request_id:
            return web.json_response({"error": "缺少 request_id"}, status=400)
        try:
            result = await self.core.accept_friend_request(request_id)
            return web.json_response({"ok": True, "result": result})
        except Exception as e:
            logger.error("接受好友请求失败: %s", e)
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_reject_friend_request(self, request: web.Request) -> web.Response:
        """POST /api/friends/requests/{request_id}/reject — 拒绝好友请求。"""
        request_id = request.match_info.get("request_id", "")
        if not request_id:
            return web.json_response({"error": "缺少 request_id"}, status=400)
        try:
            result = await self.core.reject_friend_request(request_id)
            return web.json_response({"ok": True, "result": result})
        except Exception as e:
            logger.error("拒绝好友请求失败: %s", e)
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_send_message(self, request: web.Request) -> web.Response:
        """POST /api/chat/send — 发送私聊消息。

        Body: {"to": "friend_id", "content": "xxx"}
        """
        try:
            data = await request.json()
        except json.JSONDecodeError:
            return web.json_response({"error": "无效的 JSON"}, status=400)

        to_id = data.get("to", "")
        content = data.get("content", "")
        if not to_id or not content:
            return web.json_response({"error": "缺少 to 或 content"}, status=400)

        try:
            await self.core.send_message(to_id, content)
            return web.json_response({"ok": True})
        except Exception as e:
            logger.error("发送消息失败: %s", e)
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_send_group_message(self, request: web.Request) -> web.Response:
        """POST /api/groups/message — 发送群组消息。

        Body: {"group_id": "xxx", "content": "xxx"}
        """
        try:
            data = await request.json()
        except json.JSONDecodeError:
            return web.json_response({"error": "无效的 JSON"}, status=400)

        group_id = data.get("group_id", "")
        content = data.get("content", "")
        if not group_id or not content:
            return web.json_response({"error": "缺少 group_id 或 content"}, status=400)

        try:
            await self.core.send_group_message(group_id, content)
            return web.json_response({"ok": True})
        except Exception as e:
            logger.error("发送群组消息失败: %s", e)
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_list_groups(self, request: web.Request) -> web.Response:
        """GET /api/groups — 列出群组。"""
        try:
            groups = await self.core.list_groups()
            return web.json_response({"groups": groups})
        except Exception as e:
            logger.error("获取群组列表失败: %s", e)
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_ephemeral_join(self, request: web.Request) -> web.Response:
        """POST /api/ephemeral/join — 加入临时房间。

        Body: {"invite_code": "xxx", "display_name": "xxx"}
        """
        try:
            data = await request.json()
        except json.JSONDecodeError:
            return web.json_response({"error": "无效的 JSON"}, status=400)

        invite_code = data.get("invite_code", "")
        display_name = data.get("display_name", "匿名用户")
        if not invite_code:
            return web.json_response({"error": "缺少 invite_code"}, status=400)

        try:
            result = await self.core.join_ephemeral_room(invite_code, display_name)
            return web.json_response({"ok": True, **result})
        except Exception as e:
            logger.error("加入临时房间失败: %s", e)
            return web.json_response({"error": str(e)}, status=500)

    # ─── 启动 / 停止 ────────────────────────────────────────────

    async def start(self):
        """启动 API 服务器。"""
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", self.port)
        await site.start()
        logger.info("API 服务器已启动，监听端口 %d", self.port)
        return runner

    async def stop(self, runner: web.AppRunner):
        """停止 API 服务器。"""
        await runner.cleanup()
        logger.info("API 服务器已停止")
