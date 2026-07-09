<p align="center">
  <a href="https://t.me/I_execute"><img src="https://img.shields.io/badge/Telegram-@I__execute-26A5E4?style=flat&logo=telegram&logoColor=white" alt="Telegram" /></a>
</p>

### Heroku modules

A collection of modules for [Heroku](https://github.com/coddrago/Heroku), a Telethon-based Telegram userbot framework. Covers VPN management, media/session utilities, Minecraft world conversion, downloaders (video/music), telegram tools and more other. Some modules are described in detail below, the rest are listed with a short summary.

### Installation

Every module can be installed the same way, using Heroku's `.dlm` command with a raw link to the module file:

```
.dlm https://github.com/i-execute/Modules/raw/main/Robber.py
```

Replace the filename with the module you want. Blob links work too!

Also you can add my repository to "ADDITIONAL_REPOS" via command:

```
.addrepo https://raw.githubusercontent.com/i-execute/Modules/main
```

It's let you installing modules directly by name, for example just:

```
.dlm Robber 
```

Or install all models from repository via one command and tap on i-execute/Modules button - just send:

```
.dlmall 
```

After adding my repository to "ADDITIONAL_REPOS"

### My best modules:

**XRay**
Multi-user VPN manager built around Xray-core, using VLESS with Reality (XHTTP or TCP+Vision transport). Runs and manages a separate Xray process per user rather than one shared instance, and reattaches to already-running processes after a bot restart instead of relaunching them. Handles per-user config generation (UUID, short IDs, Reality keypairs), connection link generation, and process lifecycle (start/stop/autostart) through the module's own commands.

**YNDXMusic**
Yandex Music module for searching, downloading, and browsing tracks, playlists, and audiobooks directly from Telegram. Also shows the currently playing track and can send it as an audio file. Requests are routed through curl_cffi with browser TLS impersonation instead of plain aiohttp, since Yandex's backend filters on TLS fingerprint and blocks default Python HTTP clients.

**Grabber**
Universal media downloader module. Runs as a Telegram bot (linked via a bot token) that accepts video/audio links and downloads them through yt-dlp, with ffmpeg used for merging and format conversion. Supports both video and MP3 audio modes, with an inline editor for setting a custom title, artist, and cover art on audio files before upload. Handles large files by falling back from the Bot API to userbot upload when a file exceeds the bot API size limit, and requires Telegram Premium on the userbot account for the largest files. Has a queue system with configurable worker count, live progress messages (download, merge, convert, upload stages with speed/percentage), cookie import for age-restricted or login-gated sources, and per-group/per-topic activation toggles.

### Other interesting modules

- **Chunker** — Minecraft Bedrock ←→ Java world conversion via a Chunker CLI instance on yor VPS
- **InlineDL** — inline downloader for Instagram, TikTok, and Pinterest content
- **ShorBot** — demonstration module for Shor's quantum factorization algorithm using Telegram Bot API rich messages
- **NFTChecker** — Utility module for checking telegram NFT gifts on blockchain

### Contributing

Pull requests are welcome. For major changes, open an issue first or contact me in telegram via comments in channel [@Hotaru_modules](https://t.me/Hotaru_modules) You also can suggest me idea for module, but firstly tap on star 