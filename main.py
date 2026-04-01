import re
import json
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register, StarTools
from astrbot.api import logger


@register(
    "astrbot_plugin_group_sender",
    "何意味小祥",
    "私聊机器人后，使用 /发送 和 /定时发送 管理群发/定时发送",
    "1.1.1",
)
class GroupSenderPlugin(Star):
    def __init__(self, context: Context):
        super().__init__(context)
        self.allowed_user_ids = {"3827675264"}
        self.data_dir = StarTools.get_data_dir("astrbot_plugin_group_sender")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.schedule_file = self.data_dir / "schedules.json"
        self._schedule_lock = asyncio.Lock()
        self.schedules: List[Dict] = self._load_schedules()
        self._scheduler_task = None
        self._scheduler_started = False

    async def initialize(self):
        if not self._scheduler_started:
            self._scheduler_started = True
            self._scheduler_task = asyncio.create_task(self._scheduler_loop())
            logger.info("[GroupSender] 定时任务调度器已启动")

    async def terminate(self):
        if self._scheduler_task and not self._scheduler_task.done():
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"[GroupSender] 终止调度器时发生异常: {e}")

    def _load_schedules(self) -> List[Dict]:
        if not self.schedule_file.exists():
            return []
        try:
            return json.loads(self.schedule_file.read_text(encoding="utf-8"))
        except Exception as e:
            logger.error(f"[GroupSender] 读取定时任务失败: {e}")
            return []

    def _save_schedules_unlocked(self):
        try:
            self.schedule_file.write_text(
                json.dumps(self.schedules, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
        except Exception as e:
            logger.error(f"[GroupSender] 保存定时任务失败: {e}")

    def _next_id_unlocked(self) -> int:
        return max([int(x.get("id", 0)) for x in self.schedules] + [0]) + 1

    def _get_sender_id(self, event: AstrMessageEvent) -> str:
        try:
            return str(event.get_sender_id())
        except Exception:
            try:
                return str(event.message_obj.sender.user_id)
            except Exception:
                return ""

    def _is_private_chat(self, event: AstrMessageEvent) -> bool:
        """默认拒绝，判断失败时返回 False。"""
        try:
            origin = str(getattr(event, 'unified_msg_origin', '') or '')
            if 'FriendMessage' in origin:
                return True
            if 'GroupMessage' in origin:
                return False
        except Exception:
            pass

        try:
            mt = str(event.get_message_type())
            if 'friend' in mt.lower() or 'private' in mt.lower():
                return True
            if 'group' in mt.lower():
                return False
        except Exception:
            pass

        try:
            msg_type = str(getattr(event.message_obj, 'type', ''))
            if 'friend' in msg_type.lower() or 'private' in msg_type.lower():
                return True
            if 'group' in msg_type.lower():
                return False
        except Exception:
            pass

        return False

    async def _get_all_groups(self, event: AstrMessageEvent) -> List[Dict]:
        try:
            ret = await event.bot.api.call_action('get_group_list')
            return ret if isinstance(ret, list) else []
        except Exception as e:
            logger.error(f'[GroupSender] 获取群列表失败: {e}')
            return []

    async def _get_all_group_ids(self, event: AstrMessageEvent) -> List[str]:
        groups = await self._get_all_groups(event)
        result = []
        for g in groups:
            gid = g.get('group_id')
            if gid is not None:
                result.append(str(gid))
        return result

    async def _send_group_msg(self, event: AstrMessageEvent, group_id: str, message: str) -> bool:
        try:
            await event.bot.api.call_action('send_group_msg', group_id=int(group_id), message=message)
            return True
        except Exception as e:
            logger.error(f'[GroupSender] 发送到群 {group_id} 失败: {e}')
            return False

    def _parse_send_args(self, raw_text: str):
        raw_text = re.sub(r'^/?发送\s*', '', raw_text).strip()
        if not raw_text:
            return None, None
        if raw_text == '列表':
            return '列表', ''
        parts = raw_text.split(maxsplit=1)
        if len(parts) < 2:
            return None, None
        return parts[0].strip(), parts[1].strip()

    def _parse_schedule_text(self, raw_text: str):
        text = re.sub(r'^/?定时发送\s*', '', raw_text).strip()
        if not text:
            return None

        target = '群发'
        if text.startswith('群发 '):
            text = text[3:].strip()
            target = '群发'
        else:
            m = re.match(r'^(\d+)\s+(.+)$', text)
            if m:
                target = m.group(1)
                text = m.group(2).strip()

        m_daily = re.match(r'^每天(\d{1,2}):(\d{2})\s+([\s\S]+)$', text)
        if m_daily:
            hh = int(m_daily.group(1))
            mm = int(m_daily.group(2))
            content = m_daily.group(3).strip()
            if 0 <= hh <= 23 and 0 <= mm <= 59 and content:
                return {
                    'target': target,
                    'mode': 'daily',
                    'time_text': f'每天{hh:02d}:{mm:02d}',
                    'hour': hh,
                    'minute': mm,
                    'content': content,
                }

        m_once = re.match(r'^(\d{4})年(\d{1,2})月(\d{1,2})日(\d{1,2}):(\d{2})\s+([\s\S]+)$', text)
        if m_once:
            y, mo, d, hh, mm = map(int, m_once.groups()[:5])
            content = m_once.group(6).strip()
            try:
                dt = datetime(y, mo, d, hh, mm, 0)
                if dt <= datetime.now():
                    return {'error': '过去时间'}
                if content:
                    return {
                        'target': target,
                        'mode': 'once',
                        'time_text': f'{y:04d}年{mo:02d}月{d:02d}日{hh:02d}:{mm:02d}',
                        'run_at': dt.strftime('%Y-%m-%d %H:%M:%S'),
                        'content': content,
                    }
            except Exception:
                return None
        return None

    def _calc_next_run(self, item: Dict) -> Optional[datetime]:
        now = datetime.now()
        mode = item.get('mode')
        if mode == 'daily':
            hh = int(item['hour'])
            mm = int(item['minute'])
            dt = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
            if dt <= now:
                dt += timedelta(days=1)
            return dt
        if mode == 'once':
            try:
                return datetime.strptime(item['run_at'], '%Y-%m-%d %H:%M:%S')
            except Exception:
                return None
        return None

    async def _scheduler_loop(self):
        await asyncio.sleep(3)
        while True:
            try:
                async with self._schedule_lock:
                    now = datetime.now()
                    changed = False
                    snapshot = list(self.schedules)
                    for item in snapshot:
                        if not item.get('enabled', True):
                            continue
                        next_run = self._calc_next_run(item)
                        if not next_run:
                            continue
                        if next_run <= now:
                            ok = await self._execute_schedule(item)
                            item['last_run'] = now.strftime('%Y-%m-%d %H:%M:%S')
                            item['last_status'] = 'success' if ok else 'failed'
                            changed = True
                            if item.get('mode') == 'once' and ok:
                                item['enabled'] = False
                    if changed:
                        self._save_schedules_unlocked()
            except asyncio.CancelledError:
                logger.info('[GroupSender] 调度循环已取消')
                raise
            except Exception as e:
                logger.error(f'[GroupSender] 调度循环异常: {e}')
            await asyncio.sleep(20)

    async def _execute_schedule(self, item: Dict) -> bool:
        try:
            bot = None
            try:
                bot = self.context.get_platform_manager().get_default_platform().get_client()
            except Exception:
                pass
            if bot is None:
                try:
                    bot = self.context.platform_manager.get_default_platform().get_client()
                except Exception:
                    pass
            if bot is None:
                logger.error('[GroupSender] 无法获取 bot 实例，定时发送失败')
                return False

            target = str(item.get('target', '群发'))
            content = item.get('content', '')
            if target == '群发':
                ret = await bot.api.call_action('get_group_list')
                groups = ret if isinstance(ret, list) else []
                ok_any = False
                for g in groups:
                    gid = g.get('group_id')
                    if gid is None:
                        continue
                    try:
                        await bot.api.call_action('send_group_msg', group_id=int(gid), message=content)
                        ok_any = True
                    except Exception as e:
                        logger.error(f'[GroupSender] 定时群发到 {gid} 失败: {e}')
                return ok_any
            else:
                await bot.api.call_action('send_group_msg', group_id=int(target), message=content)
                return True
        except Exception as e:
            logger.error(f'[GroupSender] 执行定时任务失败: {e}')
            return False

    def _check_permission(self, event: AstrMessageEvent):
        if not self._is_private_chat(event):
            return '请私聊机器人使用该命令。'
        sender_id = self._get_sender_id(event)
        if sender_id not in self.allowed_user_ids:
            return f'你没有权限使用这个命令。你的ID是：{sender_id}'
        return None

    @filter.command("发送")
    async def send_message_cmd(self, event: AstrMessageEvent):
        deny = self._check_permission(event)
        if deny:
            yield event.plain_result(deny)
            return

        raw_text = (getattr(event, 'message_str', '') or '').strip()
        target, content = self._parse_send_args(raw_text)

        if target == '列表':
            groups = await self._get_all_groups(event)
            if not groups:
                yield event.plain_result('没有获取到任何群聊。')
                return
            lines = ['当前群聊列表：']
            for i, g in enumerate(groups, 1):
                name = g.get('group_name', '未知群名')
                gid = g.get('group_id', '')
                lines.append(f'{i}. {name} - {gid}')
            yield event.plain_result('\n'.join(lines))
            return

        if not target or content is None:
            yield event.plain_result('格式错误。\n用法：\n/发送 列表\n/发送 群发 内容\n/发送 123456789 内容')
            return

        if not content:
            yield event.plain_result('发送内容不能为空。')
            return

        if target == '群发':
            group_ids = await self._get_all_group_ids(event)
            if not group_ids:
                yield event.plain_result('没有获取到任何群聊，群发失败。')
                return
            success = 0
            failed = []
            for gid in group_ids:
                ok = await self._send_group_msg(event, gid, content)
                if ok:
                    success += 1
                else:
                    failed.append(gid)
            msg = f'群发完成。\n成功：{success}\n失败：{len(failed)}'
            if failed:
                msg += f'\n失败群号：{", ".join(failed)}'
            yield event.plain_result(msg)
            return

        if not str(target).isdigit():
            yield event.plain_result('目标必须是“列表”“群发”或纯数字群号。')
            return

        ok = await self._send_group_msg(event, str(target), content)
        if ok:
            yield event.plain_result(f'已发送到群 {target}')
        else:
            yield event.plain_result(f'发送到群 {target} 失败，请检查 bot 是否在该群内、是否有发言权限。')

    @filter.command("定时发送")
    async def schedule_send_cmd(self, event: AstrMessageEvent):
        deny = self._check_permission(event)
        if deny:
            yield event.plain_result(deny)
            return

        raw_text = (getattr(event, 'message_str', '') or '').strip()
        parsed = self._parse_schedule_text(raw_text)
        if parsed and parsed.get('error') == '过去时间':
            yield event.plain_result('不能创建过去时间的一次性定时任务，请重新输入未来时间。')
            return
        if not parsed:
            yield event.plain_result(
                '格式错误。\n'
                '支持：\n'
                '/定时发送 每天08:30 内容（默认群发）\n'
                '/定时发送 群发 每天08:30 内容\n'
                '/定时发送 123456789 每天08:30 内容\n'
                '/定时发送 2026年04月02日20:30 内容（默认群发）\n'
                '/定时发送 群发 2026年04月02日20:30 内容\n'
                '/定时发送 123456789 2026年04月02日20:30 内容'
            )
            return

        async with self._schedule_lock:
            item = {
                'id': self._next_id_unlocked(),
                'target': parsed['target'],
                'mode': parsed['mode'],
                'time_text': parsed['time_text'],
                'content': parsed['content'],
                'enabled': True,
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'last_run': '',
                'last_status': '',
            }
            if parsed['mode'] == 'daily':
                item['hour'] = parsed['hour']
                item['minute'] = parsed['minute']
            if parsed['mode'] == 'once':
                item['run_at'] = parsed['run_at']

            self.schedules.append(item)
            self._save_schedules_unlocked()

        yield event.plain_result(f"定时任务已创建。\n编号：{item['id']}\n目标：{item['target']}\n时间：{item['time_text']}\n内容：{item['content']}")

    @filter.command("定时列表")
    async def schedule_list_cmd(self, event: AstrMessageEvent):
        deny = self._check_permission(event)
        if deny:
            yield event.plain_result(deny)
            return

        async with self._schedule_lock:
            if not self.schedules:
                yield event.plain_result('当前没有定时任务。')
                return

            lines = ['当前定时任务：']
            for item in self.schedules:
                status = '启用' if item.get('enabled', True) else '已停用'
                lines.append(
                    f"[{item.get('id')}] {status} | 目标:{item.get('target')} | 时间:{item.get('time_text')} | 内容:{item.get('content')} | 上次结果:{item.get('last_status', '')}"
                )
        yield event.plain_result('\n'.join(lines))

    @filter.command("取消定时")
    async def schedule_delete_cmd(self, event: AstrMessageEvent):
        deny = self._check_permission(event)
        if deny:
            yield event.plain_result(deny)
            return

        raw_text = (getattr(event, 'message_str', '') or '').strip()
        text = re.sub(r'^/?取消定时\s*', '', raw_text).strip()
        if not text.isdigit():
            yield event.plain_result('格式错误。用法：/取消定时 编号')
            return

        sid = int(text)
        async with self._schedule_lock:
            before = len(self.schedules)
            self.schedules = [x for x in self.schedules if int(x.get('id', 0)) != sid]
            after = len(self.schedules)
            self._save_schedules_unlocked()

        if after < before:
            yield event.plain_result(f'已删除定时任务 {sid}')
        else:
            yield event.plain_result(f'未找到定时任务 {sid}')
