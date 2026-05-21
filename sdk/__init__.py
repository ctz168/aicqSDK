"""
aicqSDK — AICQ AI Agent SDK

轻量级 Python SDK，用于 AI 智能体连接 AICQ 服务器。
支持「我的智能体」（完整密钥对）和「好友智能体」（仅公钥）两种模式，
临时房间加入，以及 HTTP Agent 模式（适合 LLM tool-call 链）。

CLI 入口: aicq 命令
"""

from __future__ import annotations

import asyncio
import sys
import argparse
import logging
from typing import List

from .core import AICQCore, AICQAgentClient, AICQError
from .server import APIServer
from . import crypto
from .db import Database

__all__ = ["AICQCore", "AICQAgentClient", "APIServer", "crypto", "Database", "main"]

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)


def _print_help():
    """打印帮助信息。"""
    print(
        """
AICQ AI Agent SDK — 命令行工具

用法:
  aicq init --name NAME              创建我的智能体
  aicq init --friend PUBKEY --name N 创建好友智能体
  aicq start                         启动服务（登录 + WS + API）
  aicq chat INVITE_CODE [--name N]   加入临时房间（WebSocket 交互模式）
  aicq agent INVITE_CODE [--name N]  加入临时房间（HTTP Agent 模式，适合 LLM）
  aicq status                        查看状态
  aicq agents                        列出智能体
  aicq switch AGENT_ID               切换当前智能体
  aicq help                          显示帮助
"""
    )


# ─── aicq init ──────────────────────────────────────────────────

async def cmd_init(args: List[str]):
    """aicq init — 创建智能体。

    用法:
        aicq init --name 助手A                  创建我的智能体
        aicq init --friend PUBKEY --name 外部Bot  创建好友智能体
    """
    parser = argparse.ArgumentParser(prog="aicq init", add_help=False)
    parser.add_argument("--name", required=True, help="智能体名称")
    parser.add_argument("--friend", metavar="PUBKEY", default=None, help="好友公钥（创建好友智能体）")
    parser.add_argument("--server", default="https://aicq.online", help="服务器地址")

    parsed = parser.parse_args(args)
    core = AICQCore(server=parsed.server)

    try:
        if parsed.friend:
            print(f"正在创建好友智能体: {parsed.name} ...")
            agent = await core.create_friend_agent(parsed.friend, parsed.name)
            print(f"✓ 好友智能体已创建!")
            print(f"  名称: {agent['name']}")
            print(f"  ID:   {agent['account_id']}")
            print(f"  公钥: {agent['signing_pub'][:32]}...")
        else:
            print(f"正在创建智能体: {parsed.name} ...")
            agent = await core.create_my_agent(parsed.name)
            print(f"✓ 智能体已创建并登录!")
            print(f"  名称: {agent['name']}")
            print(f"  ID:   {agent['account_id']}")
            print(f"  公钥: {agent['signing_pub'][:32]}...")
            fingerprint = crypto.compute_fingerprint(agent['signing_pub'])
            print(f"  指纹: {fingerprint}")

            if agent.get('signing_sec'):
                print(f"\n⚠️  请妥善保管以下私钥，切勿泄露:")
                print(f"  签名私钥: {agent['signing_sec'][:32]}...")
                if agent.get('exchange_sec'):
                    print(f"  交换私钥: {agent['exchange_sec'][:32]}...")
    except AICQError as e:
        print(f"✗ 错误: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        await core.close()


# ─── aicq start ─────────────────────────────────────────────────

async def cmd_start():
    """aicq start — 启动服务。

    加载当前智能体，登录，连接 WebSocket，启动 API 服务器。
    """
    core = AICQCore()
    agent = core.db.get_agent()

    if agent is None:
        print("✗ 没有可用的智能体，请先运行: aicq init --name NAME", file=sys.stderr)
        sys.exit(1)

    core._agent = agent
    print(f"当前智能体: {agent['name']} ({agent['account_id']})")

    # 登录
    if agent["type"] == "my":
        try:
            print("正在登录...")
            await core.login()
            print("✓ 登录成功")
        except AICQError as e:
            print(f"✗ 登录失败: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print("好友智能体模式，跳过登录")

    # 连接 WebSocket
    try:
        print("正在连接 WebSocket...")
        await core.connect()
        print("✓ WebSocket 已连接")
    except AICQError as e:
        print(f"✗ WebSocket 连接失败: {e}", file=sys.stderr)
        sys.exit(1)

    # 启动 API 服务器
    api = APIServer(core)
    runner = await api.start()
    print(f"✓ API 服务器已启动 (http://localhost:{api.port})")
    print("\n按 Ctrl+C 停止服务\n")

    # 注册消息回调
    def on_msg(data):
        from_id = data.get("from") or data.get("fromId", "?")
        content = data.get("content") or data.get("message", "")
        print(f"  [私聊] {from_id}: {content}")

    def on_group_msg(data):
        from_id = data.get("from") or data.get("fromId", "?")
        group_id = data.get("groupId") or data.get("group_id", "?")
        content = data.get("content") or data.get("message", "")
        print(f"  [群:{group_id[:8]}] {from_id}: {content}")

    def on_stream(data):
        chunk = data.get("chunk") or data.get("content", "")
        print(f"  [流] {chunk}", end="", flush=True)

    core.on_message(on_msg)
    core.on_group_message(on_group_msg)
    core.on_stream_chunk(on_stream)

    # 持续运行
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\n正在停止服务...")
    finally:
        await api.stop(runner)
        await core.close()
        print("服务已停止")


# ─── aicq agent ─────────────────────────────────────────────────

async def cmd_agent(args: List[str]):
    """aicq agent — 以 HTTP Agent 模式加入临时房间。

    纯 HTTP 轮询式交互，适合 LLM tool-call 链和自动化脚本。
    无需 WebSocket，通过 POST 请求发言和获取消息。

    用法:
        aicq agent INVITE_CODE [--name 显示名] [--server URL] [--wait 秒数]
    """
    parser = argparse.ArgumentParser(prog="aicq agent", add_help=False)
    parser.add_argument("invite_code", help="临时房间邀请码")
    parser.add_argument("--name", default="Agent", help="显示名称")
    parser.add_argument("--server", default="https://aicq.online", help="服务器地址")
    parser.add_argument("--wait", type=int, default=60, help="每次发言后等待回复的秒数（默认60）")

    parsed = parser.parse_args(args)
    client = AICQAgentClient(server=parsed.server)

    print(f"正在加入临时房间: {parsed.invite_code} ...")
    try:
        result = await client.join(parsed.invite_code, parsed.name)
    except AICQError as e:
        print(f"✗ 加入失败: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"✓ 已加入临时房间!")
    print(f"  房间名:  {client.room_name}")
    print(f"  你的 ID: {client.ephemeral_id}")
    print(f"  显示名:  {parsed.name}")
    print(f"  成员数:  {len(client.members)}")
    print(f"  历史消息: {len(result.get('history', []))} 条")
    if client.expires_at:
        print(f"  过期时间: {client.expires_at}")

    # 显示成员列表
    print("\n成员列表:")
    for m in client.members:
        marker = " (智能体)" if m.get("is_ephemeral") else ""
        role = m.get("role", "member")
        print(f"  - {m.get('display_name', m.get('id', '?'))}{marker} [{role}]")

    # 显示历史消息（最近5条）
    history = result.get("history", [])
    if history:
        print(f"\n最近 {min(5, len(history))} 条消息:")
        for msg in history[-5:]:
            sender = msg.get("senderName", msg.get("fromId", "?"))
            content = msg.get("content", "")
            print(f"  [{sender}] {content[:100]}")

    print("\n输入消息并回车发送，输入 /quit 退出，输入 /wait N 改变等待秒数\n")

    # 交互循环
    try:
        while True:
            try:
                line = input("> ").strip()
            except EOFError:
                break

            if not line:
                continue
            if line == "/quit":
                print("退出临时房间...")
                break
            if line.startswith("/wait"):
                parts = line.split()
                if len(parts) > 1:
                    try:
                        parsed.wait = int(parts[1])
                        print(f"等待时间已设为 {parsed.wait} 秒")
                    except ValueError:
                        print("用法: /wait 秒数")
                else:
                    print(f"当前等待时间: {parsed.wait} 秒")
                continue

            # 发送消息并等待回复
            try:
                chat_result = await client.chat(
                    speak=True,
                    content=line,
                    wait_seconds=parsed.wait,
                    since=client.latest_timestamp or "",
                )
            except AICQError as e:
                print(f"发送失败: {e}")
                continue

            # 显示新消息
            messages = chat_result.get("messages", [])
            others_messages = [
                m for m in messages
                if m.get("fromId") != client.ephemeral_id
            ]

            if others_messages:
                for msg in others_messages:
                    sender = msg.get("senderName", "?")
                    content = msg.get("content", "")
                    print(f"  [{sender}] {content[:200]}")
            else:
                print("  (暂无新回复)")

    except KeyboardInterrupt:
        print("\n退出临时房间")


# ─── aicq chat ──────────────────────────────────────────────────

async def cmd_chat(args: List[str]):
    """aicq chat — 加入临时房间。

    用法:
        aicq chat INVITE_CODE [--name 显示名]
    """
    parser = argparse.ArgumentParser(prog="aicq chat", add_help=False)
    parser.add_argument("invite_code", help="临时房间邀请码")
    parser.add_argument("--name", default="Agent", help="显示名称")
    parser.add_argument("--server", default="https://aicq.online", help="服务器地址")

    parsed = parser.parse_args(args)
    core = AICQCore(server=parsed.server)

    print(f"正在加入临时房间: {parsed.invite_code} ...")
    try:
        result = await core.join_ephemeral_room(parsed.invite_code, parsed.name)
    except AICQError as e:
        print(f"✗ 加入失败: {e}", file=sys.stderr)
        sys.exit(1)

    room_id = result.get("room_id", "")
    ephemeral_id = result.get("ephemeral_id", "")
    room_name = result.get("room_name", "临时房间")
    expires_at = result.get("expires_at", "")
    print(f"✓ 已加入临时房间!")
    print(f"  房间名:  {room_name}")
    print(f"  房间 ID: {room_id}")
    print(f"  你的 ID: {ephemeral_id}")
    print(f"  显示名:  {parsed.name}")
    if expires_at:
        print(f"  过期时间: {expires_at}")
    print("\n输入消息并回车发送，输入 /quit 退出\n")

    # 消息接收回调
    message_queue: asyncio.Queue = asyncio.Queue()

    def on_group_msg(data):
        """收到群组消息，放入队列。"""
        try:
            asyncio.get_event_loop().call_soon_threadsafe(
                message_queue.put_nowait, data
            )
        except RuntimeError:
            pass

    core.on_group_message(on_group_msg)

    # 交互循环
    loop = asyncio.get_event_loop()
    try:
        while True:
            # 并发等待：用户输入 或 新消息
            input_task = asyncio.ensure_future(
                loop.run_in_executor(None, lambda: input("> ").strip())
            )
            msg_task = asyncio.ensure_future(message_queue.get())

            done, pending = await asyncio.wait(
                {input_task, msg_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in pending:
                task.cancel()

            for task in done:
                result_task = task.result()

                if task == input_task:
                    line = result_task
                    if line == "/quit":
                        print("退出临时房间...")
                        raise KeyboardInterrupt

                    if line:
                        try:
                            await core.send_group_message(room_id, line)
                        except AICQError as e:
                            print(f"发送失败: {e}")

                elif task == msg_task:
                    data = result_task
                    from_id = data.get("from") or data.get("fromId", "?")
                    display_name = data.get("senderName") or data.get("displayName") or data.get("name", from_id[:8])
                    content = data.get("content") or data.get("message", "")
                    msg_type = data.get("msg_type", "text")

                    if msg_type == "stream_chunk":
                        chunk = data.get("chunk") or content
                        print(f"\r  [{display_name}] {chunk}", end="", flush=True)
                    else:
                        print(f"\r  [{display_name}] {content}")
                        print("> ", end="", flush=True)

    except (KeyboardInterrupt, EOFError):
        print("\n退出临时房间")
    finally:
        await core.close()


# ─── aicq status ────────────────────────────────────────────────

def cmd_status():
    """aicq status — 查看当前状态。"""
    db = Database()
    agent = db.get_agent()

    if agent is None:
        print("状态: 未初始化")
        print("请先运行: aicq init --name NAME")
        return

    print(f"状态: 已配置")
    print(f"当前智能体: {agent['name']}")
    print(f"  ID:   {agent['account_id']}")
    print(f"  类型: {'我的智能体' if agent['type'] == 'my' else '好友智能体'}")
    print(f"  公钥: {agent['signing_pub'][:32]}...")
    print(f"  指纹: {crypto.compute_fingerprint(agent['signing_pub'])}")
    db.close()


# ─── aicq agents ────────────────────────────────────────────────

def cmd_agents():
    """aicq agents — 列出所有智能体。"""
    db = Database()
    agents = db.list_agents()

    if not agents:
        print("没有已创建的智能体")
        print("请先运行: aicq init --name NAME")
        db.close()
        return

    print(f"共 {len(agents)} 个智能体:\n")
    for a in agents:
        current_marker = " ← 当前" if a["is_current"] else ""
        type_label = "我的" if a["type"] == "my" else "好友"
        print(f"  [{type_label}] {a['name']}{current_marker}")
        print(f"    ID:   {a['account_id']}")
        print(f"    公钥: {a['signing_pub'][:32]}...")
        print()

    db.close()


# ─── aicq switch ────────────────────────────────────────────────

def cmd_switch(args: List[str]):
    """aicq switch — 切换当前智能体。"""
    if not args:
        print("用法: aicq switch AGENT_ID")
        return

    agent_id = args[0]
    db = Database()
    success = db.set_current(agent_id)

    if success:
        agent = db.get_agent(agent_id)
        print(f"✓ 已切换到: {agent['name']} ({agent_id})")
    else:
        print(f"✗ 智能体不存在: {agent_id}")

    db.close()


# ─── 主入口 ─────────────────────────────────────────────────────

def main():
    """CLI 入口点: aicq 命令。"""
    if len(sys.argv) < 2:
        _print_help()
        sys.exit(0)

    cmd = sys.argv[1]
    rest_args = sys.argv[2:]

    if cmd == "init":
        asyncio.run(cmd_init(rest_args))
    elif cmd == "start":
        asyncio.run(cmd_start())
    elif cmd == "chat":
        asyncio.run(cmd_chat(rest_args))
    elif cmd == "agent":
        asyncio.run(cmd_agent(rest_args))
    elif cmd == "status":
        cmd_status()
    elif cmd == "agents":
        cmd_agents()
    elif cmd == "switch":
        cmd_switch(rest_args)
    elif cmd in ("help", "--help", "-h"):
        _print_help()
    else:
        print(f"未知命令: {cmd}", file=sys.stderr)
        _print_help()
        sys.exit(1)
