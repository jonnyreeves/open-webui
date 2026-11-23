"""Database-backed, single-use slots for eagerly provisioned conversations."""
import hashlib
import json
import time
from uuid import uuid4

from fastapi import HTTPException
from sqlalchemy import BigInteger, Column, String, select, update, text
from sqlalchemy.exc import IntegrityError

from open_webui.internal.db import Base, get_async_db


class PendingChat(Base):
    __tablename__ = 'pending_chat'
    slot = Column(String, primary_key=True)
    chat_id = Column(String, unique=True, nullable=False)
    user_id = Column(String, nullable=False)
    machine_id = Column(String, nullable=False)
    state = Column(String, nullable=False)
    expires_at = Column(BigInteger, nullable=False, index=True)


async def mark_used(session, chat_id):
    """Called inside the transaction accepting a message or before a mutation.

    Row locking/conditional writes arbitrate against expiry across workers.
    Once cleanup owns the slot, new mutations must fail rather than lose work.
    """
    row = await session.scalar(select(PendingChat).where(
        PendingChat.chat_id == chat_id).with_for_update())
    if row is not None:
        if row.state == 'expiring':
            raise HTTPException(409, 'This unused conversation expired. Start a new chat; your draft is preserved.')
        await session.execute(update(PendingChat).where(
            PendingChat.chat_id == chat_id, PendingChat.state == 'pending').values(state='used'))


async def use_chat(chat_id):
    from open_webui.utils.chat_id import is_saved_chat_id
    from open_webui.models.chats import Chat
    if is_saved_chat_id(chat_id):
        async with get_async_db() as session:
            if session.bind.dialect.name == "sqlite":
                await session.execute(text("BEGIN IMMEDIATE"))
            if await session.get(Chat, chat_id, with_for_update=True) is None:
                raise HTTPException(409, 'Conversation no longer exists. Start a new chat.')
            await mark_used(session, chat_id)
            await session.commit()


async def ensure_pending(user, machine_id, folder_id, directory, minutes, params):
    from open_webui.models.chats import Chat, ChatModel, Chats, chat_message_list
    await Chats.require_chat_creation_permission(user.id)
    slot = hashlib.sha256(json.dumps([user.id, folder_id, machine_id]).encode()).hexdigest()
    # The slot and empty chat are committed together. A losing concurrent insert
    # rolls back its chat as well, then reads the winning row on a fresh session.
    for _ in range(8):
        try:
            async with get_async_db() as session:
                if session.bind.dialect.name == "sqlite":
                    await session.execute(text("BEGIN IMMEDIATE"))
                row = await session.get(PendingChat, slot, with_for_update=True)
                if row is not None and row.state == 'pending':
                    chat = await session.get(Chat, row.chat_id)
                    if chat is None:
                        # Keep the old environment addressable until cleanup, rather
                        # than overwrite the slot and orphan its persistent HOME.
                        row.state = 'expiring'
                        await session.commit()
                        raise HTTPException(409, 'Deleted pending conversation is being cleaned up. Retry shortly.')
                    if not chat_message_list(chat.chat):
                        if row.expires_at <= int(time.time()):
                            raise HTTPException(409, 'Unused conversation is expiring. Retry shortly.')
                        return ChatModel.model_validate(chat)
                if row is not None and row.state == 'expiring':
                    raise HTTPException(409, 'Unused conversation is being removed. Retry shortly.')
                now = int(time.time())
                chat = ChatModel(id=str(uuid4()), user_id=user.id, title='New Chat',
                    chat={'title': 'New Chat', 'machine_id': machine_id,
                          'working_directory': directory, 'models': [], 'params': params,
                          'history': {'messages': {}, 'currentId': None}, 'messages': []},
                    folder_id=folder_id, created_at=now, updated_at=now, last_read_at=now)
                session.add(Chat(**chat.model_dump()))
                values = dict(chat_id=chat.id, user_id=user.id, machine_id=machine_id,
                              state='pending', expires_at=now + minutes * 60)
                if row is None:
                    session.add(PendingChat(slot=slot, **values))
                else:
                    result = await session.execute(update(PendingChat).where(
                        PendingChat.slot == slot, PendingChat.chat_id == row.chat_id,
                        PendingChat.state == row.state).values(**values))
                    if result.rowcount != 1:
                        await session.rollback()
                        continue
                await session.commit()
                return chat
        except IntegrityError:
            continue
    raise HTTPException(409, 'Conversation changed concurrently. Retry.')
