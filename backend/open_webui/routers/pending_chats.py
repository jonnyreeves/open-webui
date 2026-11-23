"""Opt-in pending-chat policy lives in WebUI, not the Terminals template schema."""
import asyncio
import logging
import time
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import delete, select, update, text

from open_webui.internal.db import get_async_db
from open_webui.models.config import Config
from open_webui.models.pending_chats import PendingChat, ensure_pending
from open_webui.utils.auth import get_verified_user
from open_webui.utils.machines import template_connection, split_template_id, server_request

router = APIRouter()
log = logging.getLogger(__name__)


def policy_key(machine_id):
    return 'machines.pending_chat.' + machine_id


class Policy(BaseModel):
    enabled: bool = False
    expiry_minutes: int = Field(default=15, ge=1, le=1440)


@router.get('/settings')
async def settings(machine_id: str, user=Depends(get_verified_user)):
    await template_connection(user, machine_id)
    return Policy(**(await Config.get(policy_key(machine_id), {}) or {}))


@router.put('/settings')
async def save_settings(machine_id: str, body: Policy, user=Depends(get_verified_user)):
    _, template = await template_connection(user, machine_id, 'write')
    if body.enabled and template.get('data', {}).get('scope') != 'chat':
        raise HTTPException(422, 'Preparing new conversations requires Chat scope')
    await Config.upsert({policy_key(machine_id): body.model_dump()})
    return body


class EnsureForm(BaseModel):
    machine_id: str
    folder_id: str | None = None
    params: dict = Field(default_factory=dict)


@router.post('/ensure')
async def ensure(body: EnsureForm, user=Depends(get_verified_user)):
    from open_webui.routers.chats import has_folder_write_access
    from open_webui.models.folders import Folders
    _, template = await template_connection(user, body.machine_id)
    policy = Policy(**(await Config.get(policy_key(body.machine_id), {}) or {}))
    if not policy.enabled or template.get('data', {}).get('scope') != 'chat':
        raise HTTPException(409, 'Preparing new conversations is disabled')
    directory = (template.get('data', {}).get('env') or {}).get('OPEN_TERMINAL_START_DIR', '/home/user')
    if body.folder_id:
        if not await has_folder_write_access(user.id, body.folder_id):
            raise HTTPException(404, 'Folder not found')
        folder = await Folders.get_folder_by_id(body.folder_id)
        if folder is None:
            raise HTTPException(404, 'Folder not found')
        data = folder.data or {}
        if (data.get('machine_id') or data.get('terminal_id')) != body.machine_id:
            raise HTTPException(409, 'Project environment changed; refresh the project')
        directory = data.get('working_directory') or directory
    return await ensure_pending(user, body.machine_id, body.folder_id, directory,
                                policy.expiry_minutes, body.params)


async def expire_once():
    """Claim before external cleanup; retries survive failures and restarts."""
    from open_webui.models.chats import Chat, Chats, chat_message_list
    async with get_async_db() as session:
        candidates = list((await session.scalars(select(PendingChat).where(
            PendingChat.state == 'expiring',
        ))).all()) + list((await session.scalars(select(PendingChat).where(
            PendingChat.state == 'pending', PendingChat.expires_at <= int(time.time())
        ))).all())
    for item in candidates:
        try:
            async with get_async_db() as session:
                if session.bind.dialect.name == "sqlite":
                    await session.execute(text("BEGIN IMMEDIATE"))
                row = await session.get(PendingChat, item.slot, with_for_update=True)
                if row is None or row.chat_id != item.chat_id or row.state == 'used':
                    continue
                chat = await session.get(Chat, row.chat_id)
                # Defensive protection for messages written by older callers.
                if chat and chat_message_list(chat.chat):
                    row.state = 'used'
                    await session.commit()
                    continue
                result = await session.execute(update(PendingChat).where(
                    PendingChat.slot == row.slot, PendingChat.chat_id == row.chat_id,
                    PendingChat.state == row.state).values(state='expiring'))
                if result.rowcount != 1:
                    continue
                await session.commit()
            connection_id, template_id = split_template_id(item.machine_id)
            connections = await Config.get('machines.connections', []) or []
            connection = next((c for c in connections if c.get('id') == connection_id), None)
            if connection is None:
                raise RuntimeError('Pending cleanup connection is unavailable')
            await server_request(connection,
                f'api/v1/terminals/templates/{quote(template_id, safe="")}/instance',
                method='DELETE', body={'user_id': item.user_id, 'chat_id': item.chat_id})
            await Chats.delete_chat_by_id_and_user_id(item.chat_id, item.user_id)
            async with get_async_db() as session:
                await session.execute(delete(PendingChat).where(
                    PendingChat.slot == item.slot, PendingChat.chat_id == item.chat_id,
                    PendingChat.state == 'expiring'))
                await session.commit()
        except Exception:
            log.exception('Pending conversation cleanup failed for %s', item.chat_id)


async def cleanup_loop():
    while True:
        try:
            await expire_once()
        except Exception:
            log.exception('Pending conversation sweep failed')
        await asyncio.sleep(60)
