__version__ = (1, 0, 0)
# meta developer: I_execute.t.me

import asyncio
from telethon import functions, types

from .. import loader, utils


@loader.tds
class DeleteAvka(loader.Module):
    """Delete all avatars from your profile"""

    strings = {
        "name": "DeleteAvka",
        "processing": "Processing...",
        "no_avatars": "Error: no avatars in your profile",
        "deleted": "Deleted: {} avatars",
        "error": "Error: {}",
    }

    strings_ru = {
        "processing": "Обработка...",
        "no_avatars": "Ошибка: в вашем профиле нет аватарок",
        "deleted": "Удалено: {} аватарок",
        "error": "Ошибка: {}",
    }

    @loader.command(
        ru_doc="Удалить все аватарки из вашего профиля",
        en_doc="Delete all avatars from your profile",
    )
    async def avkarm(self, message):
        """Delete all avatars from your profile"""
        await utils.answer(message, self.strings["processing"])
        try:
            c = 0
            while True:
                photos = await self.client(functions.photos.GetUserPhotosRequest(
                    user_id=types.InputPeerSelf(),
                    offset=0,
                    max_id=0,
                    limit=100
                ))
                
                if not photos.photos:
                    if c == 0:
                        return await utils.answer(message, self.strings["no_avatars"])
                    break
                
                ids = [
                    types.InputPhoto(
                        id=p.id,
                        access_hash=p.access_hash,
                        file_reference=p.file_reference
                    )
                    for p in photos.photos
                ]
                
                await self.client(functions.photos.DeletePhotosRequest(id=ids))
                c += len(ids)
                await asyncio.sleep(0.1)
            
            await utils.answer(message, self.strings["deleted"].format(c))
        except Exception as e:
            await utils.answer(message, self.strings["error"].format(e))