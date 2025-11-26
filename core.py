import os
import time
import datetime
import aiohttp
import aiofiles
import asyncio
import logging
import subprocess
import concurrent.futures

from pyrogram import Client
from pyrogram.types import Message

# ================= GLOBAL ==================
failed_counter = 0

# ================= UTILITIES ==================

def get_duration(filename):
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries",
             "format=duration", "-of",
             "default=noprint_wrappers=1:nokey=1", filename],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        return float(result.stdout.strip())
    except Exception:
        return 0


def run_cmd(cmd):
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = process.stdout.decode(errors="ignore")
    error = process.stderr.decode(errors="ignore")

    if output:
        print(output)
    if error:
        print(error)

    return output if output else error


def run_parallel(workers, cmds):
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        list(executor.map(run_cmd, cmds))


def human_readable_size(size):
    for unit in ['B','KB','MB','GB','TB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"


def time_name():
    now = datetime.datetime.now()
    return now.strftime("%Y-%m-%d_%H-%M-%S.mp4")


# ================= ASYNC DOWNLOAD ==================

async def download_file(url, name):
    filename = f"{name}.pdf"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                async with aiofiles.open(filename, "wb") as f:
                    await f.write(await resp.read())
    return filename


# ================= INFO PARSERS ==================

def parse_vid_info(info: str):
    result = []
    temp = set()

    for line in info.split("\n"):
        if "[" not in line and "---" not in line:
            while "  " in line:
                line = line.replace("  ", " ")
            parts = line.strip().split("|")[0].split(" ", 2)

            if len(parts) >= 3:
                res = parts[2]
                if "RESOLUTION" not in res and "audio" not in res and res not in temp:
                    temp.add(res)
                    result.append((parts[0], res))

    return result


def vid_info(info: str):
    data = {}
    temp = set()

    for line in info.split("\n"):
        if "[" not in line and "---" not in line:
            while "  " in line:
                line = line.replace("  ", " ")
            parts = line.strip().split("|")[0].split(" ", 3)

            if len(parts) >= 3:
                res = parts[2]
                if res not in temp and "audio" not in res and "RESOLUTION" not in res:
                    temp.add(res)
                    data[res] = parts[0]

    return data


# ================= SHELL RUNNER ==================

async def run_async(cmd):
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await proc.communicate()

    if proc.returncode != 0:
        return False

    return stdout.decode() if stdout else stderr.decode()


# ================= VIDEO DOWNLOAD ==================

async def download_video(url, cmd, name):
    global failed_counter

    full_cmd = (
        f'{cmd} -R 25 --external-downloader aria2c '
        '--downloader-args "aria2c: -x 32 -j 32 -s 16 -k 1M"'
    )

    print(full_cmd)
    result = subprocess.run(full_cmd, shell=True)

    if "visionias" in cmd and result.returncode != 0 and failed_counter < 10:
        failed_counter += 1
        await asyncio.sleep(5)
        return await download_video(url, cmd, name)

    failed_counter = 0

    name_no_ext = os.path.splitext(name)[0]

    for ext in ["", ".mp4", ".mkv", ".webm", ".mp4.webm"]:
        file = f"{name_no_ext}{ext}"
        if os.path.isfile(file):
            return file

    return None


# ================= TELEGRAM UPLOAD ==================

async def send_doc(bot: Client, msg: Message, file_path, caption, name):
    status = await msg.reply_text(f"Uploading: `{name}`")
    await bot.send_document(msg.chat.id, file_path, caption=caption)
    await status.delete()

    if os.path.exists(file_path):
        os.remove(file_path)


async def send_video(bot: Client, msg: Message, caption, filename, thumb, name):
    thumbnail = None

    try:
        if thumb == "no":
            thumb_file = f"{filename}.jpg"
            subprocess.run(
                f'ffmpeg -i "{filename}" -ss 00:00:05 -vframes 1 "{thumb_file}"',
                shell=True
            )
            thumbnail = thumb_file
        else:
            thumbnail = thumb
    except:
        thumbnail = None

    duration = get_duration(filename)
    status = await msg.reply_text(f"Uploading video: `{name}`")

    try:
        await bot.send_video(
            msg.chat.id,
            filename,
            caption=caption,
            duration=int(duration),
            supports_streaming=True,
            thumb=thumbnail
        )
    except:
        await bot.send_document(msg.chat.id, filename, caption=caption)

    await status.delete()

    if os.path.exists(filename):
        os.remove(filename)
    if thumbnail and os.path.exists(thumbnail):
        os.remove(thumbnail)


# ================= LOGGING ==================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
