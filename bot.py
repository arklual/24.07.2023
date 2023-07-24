from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.raw import functions

import pygsheets
from datetime import datetime, date
import zoneinfo
import asyncio
import aiofiles

from aiogram import Bot, types
from aiogram.dispatcher import Dispatcher
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher import FSMContext

from settings import *

to_send_message = None
to_send_photo_path = None

class MessageForm(StatesGroup):
    message = State()

async def start(message: types.Message):
    await message.answer('Чтобы заработал бот, нужно установить текст рассылки. Для этого используйте команду /set_text')

async def get_text(message: types.Message, state: FSMContext):
    await message.answer('Отправьте мне сообщение для рассылки')
    await state.set_state(MessageForm.message)


async def set_text(message: types.Message, state: FSMContext):
    await state.finish()
    global to_send_message, to_send_photo_path
    to_send_message = message.text if message.text else message.caption
    if message.photo and len(message.photo) != 0:
        to_send_photo_path = message.photo[-1].download()
    else:
        to_send_photo_path = None


async def setup():
    bot = Bot(TOKEN)
    app = Client("bot", api_id=api_id, api_hash=api_hash)
    await app.start()
    storage = MemoryStorage()
    asyncio.create_task(scheduler(app))
    asyncio.create_task(is_messages_seen(app))
    dp = Dispatcher(bot, storage)
    dp.register_message_handler(start, commands=['start'])
    dp.register_message_handler(get_text, commands=['set_text'])
    dp.register_message_handler(set_text, state=MessageForm.message)
    dp.start_polling()

async def is_messages_seen(app: Client):
    zone = zoneinfo.ZoneInfo("Europe/Moscow")
    gc = pygsheets.authorize(service_account_file='sheets_key.json')
    ws = gc.open('База по крипте рассылка').worksheet()
    while True:
        ids = ws.get_col(1)
        for n,i in enumerate(ids):
            if i == '':
                ids = ids[:n]
                break
        for i, id in enumerate(ids):
            result = await app.invoke(
                functions.messages.GetPeerDialogs(
                    peers=[await app.resolve_peer(id)]
                )
            )   
            async for message in app.get_chat_history(id, limit=10):
                if message.outgoing:
                    if result.dialogs[0].read_outbox_max_id >= message.id:
                        ws.update_value(f'G{i+1}', f'Прочитано {datetime.now(zone).date()}')

        await asyncio.sleep(86400)
        

async def scheduler(app: Client):
    global to_send_photo_path, to_send_message
    zone = zoneinfo.ZoneInfo("Europe/Moscow")
    gc = pygsheets.authorize(service_account_file='sheets_key.json')
    ws = gc.open('База по крипте').worksheet()
    ws1 = gc.open('База по крипте рассылка').worksheet()
    ids_already = ws1.get_col(1)
    for n,i in enumerate(ids_already):
        if i == '':
            ids_already = ids_already[:n]
            break
    while True:
        if to_send_message:
            ids = ws.get_col(1)
            fnames = ws.get_col(2)
            lnames = ws.get_col(3)
            usernames = ws.get_col(4)
            phones = ws.get_col(5)
            for n,i in enumerate(ids):
                if i == '':
                    ids = ids[:n]
                    fnames = fnames[:n]
                    lnames = lnames[:n]
                    usernames = usernames[:n]
                    phones = phones[:n]
                    break
            for i, id in enumerate(ids):
                await asyncio.sleep(55)
                if id in ids_already:
                    continue
                if to_send_photo_path:
                    async with aiofiles.open(to_send_photo_path, 'wb') as fp:
                        await app.send_photo(id, await fp.read(), to_send_message)
                else:
                    await app.send_message(id, to_send_message)
                ids_already.append(id)
                ws.update_row(len(ids_already), [id, fnames[i], lnames[i], usernames[i], phones[i], datetime.now(zone).date(), 'Не прочитано'])


if __name__ == "__main__":
    asyncio.run(setup())