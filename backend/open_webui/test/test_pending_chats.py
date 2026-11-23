"""Run against an isolated SQLite database; never touch application state."""
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from open_webui.models.chats import Chat, Chats
from open_webui.models.pending_chats import PendingChat, ensure_pending, use_chat
from open_webui.routers.pending_chats import expire_once


class PendingChatTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.engine = create_async_engine('sqlite+aiosqlite:///' + str(Path(self.directory.name) / 'test.db'))
        self.factory = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as connection:
            await connection.run_sync(Chat.__table__.create)
            await connection.run_sync(PendingChat.__table__.create)

        @asynccontextmanager
        async def session():
            async with self.factory() as db:
                yield db
        self.session = session

        async def delete_chat(chat_id, user_id):
            async with self.session() as db:
                await db.execute(delete(Chat).where(Chat.id == chat_id, Chat.user_id == user_id))
                await db.commit()

        self.remote = AsyncMock(return_value={'ok': True})
        self.patches = [
            patch('open_webui.models.pending_chats.get_async_db', session),
            patch('open_webui.models.chats.get_async_db', session),
            patch.object(Chats, 'has_chat_access', AsyncMock(return_value=True)),
            patch('open_webui.models.chats.ChatMessages.upsert_message', AsyncMock()),
            patch('open_webui.routers.pending_chats.get_async_db', session),
            patch.object(Chats, 'require_chat_creation_permission', AsyncMock()),
            patch.object(Chats, 'delete_chat_by_id_and_user_id', delete_chat),
            patch('open_webui.routers.pending_chats.Config.get', AsyncMock(return_value=[{'id': 'repro'}])),
            patch('open_webui.routers.pending_chats.server_request', self.remote),
        ]
        for item in self.patches:
            item.start()
        self.user = SimpleNamespace(id='owner', name='Owner')

    async def asyncTearDown(self):
        for item in reversed(self.patches):
            item.stop()
        await self.engine.dispose()
        self.directory.cleanup()

    async def ensure(self, folder='project', user=None):
        return await ensure_pending(user or self.user, 'template:repro:fixture', folder,
                                    '/home/user/workspace', 15, {})

    async def expire(self, chat_id):
        async with self.session() as db:
            await db.execute(update(PendingChat).where(PendingChat.chat_id == chat_id).values(expires_at=0))
            await db.commit()

    async def test_concurrent_requests_create_exactly_one_chat(self):
        chats = await asyncio.gather(*(self.ensure() for _ in range(16)))
        self.assertEqual(len({c.id for c in chats}), 1)
        async with self.session() as db:
            self.assertEqual(len(list(await db.scalars(select(Chat)))), 1)
        self.assertEqual((await self.ensure()).id, chats[0].id)

    async def test_user_and_project_slots_are_independent(self):
        a = await self.ensure()
        b = await self.ensure('other-project')
        c = await self.ensure(user=SimpleNamespace(id='other-user'))
        self.assertEqual(len({a.id, b.id, c.id}), 3)

    async def test_meaningful_use_prevents_cleanup_and_allocates_new_slot(self):
        first = await self.ensure()
        await self.expire(first.id)
        await use_chat(first.id)
        await expire_once()
        self.remote.assert_not_awaited()
        second = await self.ensure()
        self.assertNotEqual(first.id, second.id)
        async with self.session() as db:
            self.assertIsNotNone(await db.get(Chat, first.id))

    async def test_first_accepted_turn_marks_pending_used_atomically(self):
        chat = await self.ensure()
        await Chats.insert_chat_turn(chat.id, self.user,
            {'id': 'message-1', 'content': 'Run pwd', 'parentId': None}, [])
        async with self.session() as db:
            row = await db.scalar(select(PendingChat))
            self.assertEqual(row.state, 'used')
        self.assertNotEqual(chat.id, (await self.ensure()).id)

    async def test_legacy_saved_message_is_never_reused(self):
        chat = await self.ensure()
        async with self.session() as db:
            row = await db.get(Chat, chat.id)
            row.chat = {'messages': [{'role': 'user', 'content': 'existing work'}]}
            await db.commit()
        self.assertNotEqual(chat.id, (await self.ensure()).id)

    async def test_expiry_claim_rejects_racing_use(self):
        chat = await self.ensure()
        await self.expire(chat.id)
        claimed, release = asyncio.Event(), asyncio.Event()

        async def remote(*args, **kwargs):
            claimed.set()
            await release.wait()
            return {'ok': True}
        self.remote.side_effect = remote
        cleanup = asyncio.create_task(expire_once())
        await asyncio.wait_for(claimed.wait(), 5)
        try:
            with self.assertRaises(HTTPException) as error:
                await use_chat(chat.id)
            self.assertEqual(error.exception.status_code, 409)
        finally:
            release.set()
            await cleanup
        async with self.session() as db:
            self.assertIsNone(await db.get(Chat, chat.id))
            self.assertIsNone(await db.scalar(select(PendingChat)))

    async def test_deleted_pending_chat_keeps_its_home_addressable_for_cleanup(self):
        chat = await self.ensure()
        async with self.session() as db:
            await db.execute(delete(Chat).where(Chat.id == chat.id))
            await db.commit()
        with self.assertRaises(HTTPException):
            await self.ensure()
        async with self.session() as db:
            row = await db.scalar(select(PendingChat))
            self.assertEqual(row.chat_id, chat.id)
            self.assertEqual(row.state, 'expiring')
        await expire_once()
        self.remote.assert_awaited_once()
        self.assertNotEqual(chat.id, (await self.ensure()).id)

    async def test_mutation_after_completed_cleanup_is_rejected(self):
        chat = await self.ensure()
        await self.expire(chat.id)
        await expire_once()
        with self.assertRaises(HTTPException) as error:
            await use_chat(chat.id)
        self.assertEqual(error.exception.status_code, 409)

    async def test_failed_cleanup_is_retried_without_reuse(self):
        chat = await self.ensure()
        await self.expire(chat.id)
        self.remote.side_effect = RuntimeError('Disposable backend unavailable')
        with self.assertLogs('open_webui.routers.pending_chats', level='ERROR'):
            await expire_once()
        with self.assertRaises(HTTPException):
            await self.ensure()
        self.remote.side_effect = None
        await expire_once()
        self.assertNotEqual(chat.id, (await self.ensure()).id)


if __name__ == '__main__':
    unittest.main()
