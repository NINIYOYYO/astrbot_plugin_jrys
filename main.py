from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
from astrbot.api import AstrBotConfig
import random
import json
import os
import errno
import shutil
import tempfile
from pathlib import Path
from hashlib import sha256
from urllib.parse import urlparse
from uuid import uuid4
from typing import Optional, List, Tuple
from PIL import Image, ImageDraw, ImageFont
import aiohttp
from datetime import datetime
import asyncio
import aiofiles
import aiofiles.os


ONE_DAY_IN_SECONDS = 86400
IMAGE_HEIGHT = 1920
IMAGE_WIDTH = 1080
AVATAR_SIZE = (150, 150)
AVATAR_POSITION = (60, 1350)
FONT_NAME = "千图马克手写体.ttf"

TEXT_BOX_Y = 1270
TEXT_BOX_HEIGHT = 700
TEXT_BOX_RADIUS = 50

DATE_Y = 1300
SUMMARY_Y = 1400
LUCKY_STAR_Y = 1500
SIGN_TEXT_Y = 1600
UNSIGN_TEXT_Y = 1700
WARNING_TEXT_Y = 1850

WARNING_TEXT_Y_OFFSET = 10
UNSIGN_TEXT_Y_OFFSET = 15
TEXT_WRAP_WIDTH = 1000

LEFT_PADDING = 20


@register("今日运势", "ominus", "一个今日运势海报生成图", "1.0.3")
class JrysPlugin(Star):
    """今日运势插件,可生成今日运势海报"""

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)

        self.config = config
        self.avatar_cache_expiration = self.config.get(
            "avatar_cache_expiration", ONE_DAY_IN_SECONDS
        )  # 默认一天过期
        self.font_name = self.config.get("font_name", FONT_NAME)  # 默认字体名称

        self.image_width = self.config.get("img_width", IMAGE_WIDTH)
        self.image_height = self.config.get("img_height", IMAGE_HEIGHT)  # 默认图片高度

        avatar_position_list = self.config.get("avatar_position", list(AVATAR_POSITION))
        self.avatar_position = tuple(avatar_position_list)  # 默认头像位置

        avatar_size_list = self.config.get("avatar_size", list(AVATAR_SIZE))
        self.avatar_size = tuple(avatar_size_list)

        self.date_y = self.config.get("date_y_position", DATE_Y)
        self.summary_y = self.config.get("summary_y_position", SUMMARY_Y)
        self.lucky_star_y = self.config.get("lucky_star_y_position", LUCKY_STAR_Y)
        self.sign_text_y = self.config.get("sign_text_y_position", SIGN_TEXT_Y)
        self.unsign_text_y = self.config.get("unsign_text_y_position", UNSIGN_TEXT_Y)
        self.warning_text_y = self.config.get("warning_text_y_position", WARNING_TEXT_Y)

        self.data_dir = os.path.dirname(os.path.abspath(__file__))
        self.avatar_dir = os.path.join(self.data_dir, "avatars")
        self.background_dir = os.path.join(self.data_dir, "backgroundFolder")
        self.font_dir = os.path.join(self.data_dir, "font")
        self.font_path = os.path.join(self.data_dir, "font", self.font_name)

        # 是否启用关键词触发功能
        self.jrys_keyword_enabled = self.config.get("jrys_keyword_enabled", True)

        # 网络请求部分
        self._http_timeout = aiohttp.ClientTimeout(total=5)  # 设置请求超时时间为5秒
        self._connection_limit = aiohttp.TCPConnector(limit=10)  # 限制并发连接数为10
        self._session = aiohttp.ClientSession(
            timeout=self._http_timeout, connector=self._connection_limit
        )
        self._http_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }

        self.fonts = {}
        FONT_SIZES = [50, 60, 36, 30]  # 字体大小列表
        try:
            for size in FONT_SIZES:
                self.fonts[size] = ImageFont.truetype(self.font_path, size)

        except Exception:
            logger.error(f"无法加载字体文件 {self.font_path},使用默认字体回退")
            self.default_font = ImageFont.load_default()
            for size in FONT_SIZES:
                self.fonts[size] = self.default_font

        # 初始化jrys数据
        self.jrys_data = {}
        self.is_data_loaded = False

        # 确保目录存在
        os.makedirs(self.avatar_dir, exist_ok=True)
        os.makedirs(self.background_dir, exist_ok=True)
        os.makedirs(self.font_dir, exist_ok=True)

        # 大文件缓存目录（在 initialize() 中初始化为 data/plugin_data/{plugin_name}/...）
        self._storage_initialized = False
        self._plugin_data_dir: Optional[Path] = None
        self._background_cache_dir: Optional[Path] = None
        self._background_tmp_dir: Optional[Path] = None
        self._precache_task: Optional[asyncio.Task] = None

    async def initialize(self):
        """插件加载/重载后执行（适合做缓存预热等异步任务）。"""
        self._ensure_storage_dirs()

        if self.config.get("pre_cache_background_images", False):
            self._start_background_precache()

    def _migrate_legacy_cache_dir(self, legacy_dir: Path, target_dir: Path, label: str) -> None:
        """将旧版本缓存目录迁移到标准插件数据目录。"""
        try:
            if not legacy_dir.exists() or not legacy_dir.is_dir():
                return

            legacy_resolved = legacy_dir.resolve()
            target_resolved = target_dir.resolve()
            if legacy_resolved == target_resolved:
                return

            target_dir.mkdir(parents=True, exist_ok=True)

            moved = 0
            skipped = 0
            replaced = 0
            failed = 0

            for item in legacy_dir.iterdir():
                if not item.is_file():
                    continue

                dest = target_dir / item.name
                try:
                    if dest.exists():
                        try:
                            src_stat = item.stat()
                            dest_stat = dest.stat()
                            if src_stat.st_mtime <= dest_stat.st_mtime:
                                item.unlink(missing_ok=True)
                                skipped += 1
                                continue
                        except Exception:
                            item.unlink(missing_ok=True)
                            skipped += 1
                            continue

                        replaced += 1

                    try:
                        os.replace(item, dest)
                    except OSError as e:
                        if e.errno == errno.EXDEV:
                            shutil.copy2(item, dest)
                            item.unlink(missing_ok=True)
                        else:
                            raise

                    moved += 1
                except Exception as e:
                    failed += 1
                    logger.warning(f"迁移{label}缓存失败: {item} -> {dest} | {e}")

            try:
                if not any(legacy_dir.iterdir()):
                    legacy_dir.rmdir()
            except Exception:
                pass

            if moved or replaced or skipped or failed:
                logger.info(
                    f"{label}缓存迁移完成: "
                    f"from={legacy_dir} to={target_dir} "
                    f"moved={moved} replaced={replaced} skipped={skipped} failed={failed}"
                )
        except Exception as e:
            logger.warning(f"{label}缓存迁移异常: {e}")

    def _ensure_storage_dirs(self) -> None:
        """初始化插件大文件缓存目录（优先 data/plugin_data/{plugin_name}）。"""
        if self._storage_initialized:
            return

        try:
            from astrbot.core.utils.astrbot_path import get_astrbot_data_path

            plugin_name = getattr(self, "name", None) or "unknown"
            data_root = get_astrbot_data_path()
            data_root_path = data_root if isinstance(data_root, Path) else Path(str(data_root))
            plugin_data_dir = data_root_path / "plugin_data" / plugin_name
            plugin_data_dir.mkdir(parents=True, exist_ok=True)

            self._plugin_data_dir = plugin_data_dir

            cache_dir = plugin_data_dir / "cache"
            cache_dir.mkdir(parents=True, exist_ok=True)

            self._background_cache_dir = cache_dir / "background_images"
            self._background_cache_dir.mkdir(parents=True, exist_ok=True)
            self._background_tmp_dir = cache_dir / "background_images_tmp"
            self._background_tmp_dir.mkdir(parents=True, exist_ok=True)

            # 缓存目录分类：avatars / background_images / background_images_tmp
            target_avatar_dir = cache_dir / "avatars"
            self.avatar_dir = str(target_avatar_dir)
            os.makedirs(self.avatar_dir, exist_ok=True)

            # 迁移旧版本缓存目录（插件目录 / 旧 plugin_data 结构 / 旧 fallback 结构）
            legacy_avatar_dirs = [
                Path(self.data_dir) / "avatars",
                plugin_data_dir / "avatars",
            ]
            for legacy_dir in legacy_avatar_dirs:
                self._migrate_legacy_cache_dir(legacy_dir, target_avatar_dir, label="头像")

            legacy_background_dirs = [
                Path(self.background_dir) / "images",  # 旧 fallback 结构
                Path(self.data_dir) / "background_images",
                plugin_data_dir / "background_images",
            ]
            for legacy_dir in legacy_background_dirs:
                self._migrate_legacy_cache_dir(
                    legacy_dir, self._background_cache_dir, label="背景图"
                )

            legacy_background_tmp_dirs = [
                Path(self.background_dir) / "images_tmp",  # 旧 fallback 结构
                Path(self.data_dir) / "background_images_tmp",
                plugin_data_dir / "background_images_tmp",
            ]
            for legacy_dir in legacy_background_tmp_dirs:
                self._migrate_legacy_cache_dir(
                    legacy_dir, self._background_tmp_dir, label="背景图临时"
                )

            self._storage_initialized = True
            logger.info(f"插件数据目录初始化完成: {plugin_data_dir}")
        except Exception as e:
            # 兼容：若无法获取 AstrBot 数据目录，则回退到插件目录
            logger.warning(f"初始化插件数据目录失败，将回退到插件目录缓存: {e}")
            self._plugin_data_dir = Path(self.data_dir)

            cache_dir = self._plugin_data_dir / "cache"
            cache_dir.mkdir(parents=True, exist_ok=True)

            self._background_cache_dir = cache_dir / "background_images"
            self._background_cache_dir.mkdir(parents=True, exist_ok=True)
            self._background_tmp_dir = cache_dir / "background_images_tmp"
            self._background_tmp_dir.mkdir(parents=True, exist_ok=True)

            target_avatar_dir = cache_dir / "avatars"
            self.avatar_dir = str(target_avatar_dir)
            os.makedirs(self.avatar_dir, exist_ok=True)

            legacy_avatar_dirs = [
                Path(self.data_dir) / "avatars",
            ]
            for legacy_dir in legacy_avatar_dirs:
                self._migrate_legacy_cache_dir(legacy_dir, target_avatar_dir, label="头像")

            legacy_background_dirs = [
                Path(self.background_dir) / "images",
                Path(self.data_dir) / "background_images",
            ]
            for legacy_dir in legacy_background_dirs:
                self._migrate_legacy_cache_dir(
                    legacy_dir, self._background_cache_dir, label="背景图"
                )

            legacy_background_tmp_dirs = [
                Path(self.background_dir) / "images_tmp",
                Path(self.data_dir) / "background_images_tmp",
            ]
            for legacy_dir in legacy_background_tmp_dirs:
                self._migrate_legacy_cache_dir(
                    legacy_dir, self._background_tmp_dir, label="背景图临时"
                )

            self._storage_initialized = True

    def _start_background_precache(self) -> None:
        """启动后台预缓存任务（不会阻塞插件加载/重载）。"""
        if self._precache_task and not self._precache_task.done():
            return
        self._precache_task = asyncio.create_task(self._pre_cache_background_images())

    def _background_cache_path_for_url(self, url: str) -> Path:
        self._ensure_storage_dirs()
        assert self._background_cache_dir is not None

        parsed = urlparse(url)
        ext = os.path.splitext(parsed.path)[1].lower()
        if not ext or len(ext) > 10:
            ext = ".img"
        digest = sha256(url.encode("utf-8")).hexdigest()
        return self._background_cache_dir / f"{digest}{ext}"

    def _background_tmp_path_for_url(self, url: str) -> Path:
        self._ensure_storage_dirs()
        assert self._background_tmp_dir is not None

        parsed = urlparse(url)
        ext = os.path.splitext(parsed.path)[1].lower()
        if not ext or len(ext) > 10:
            ext = ".img"
        return self._background_tmp_dir / f"{uuid4().hex}{ext}"

    async def _download_to_path(
        self, url: str, dest: Path, label: str = "图片", retries: int = 1
    ) -> bool:
        dest.parent.mkdir(parents=True, exist_ok=True)
        retries = max(0, int(retries))

        for attempt in range(retries + 1):
            status: Optional[int] = None
            reason = ""
            tmp_path = dest.parent / f"{dest.name}.{uuid4().hex}.tmp"

            try:
                async with self._session.get(url, headers=self._http_headers) as response:
                    status = response.status
                    reason = (response.reason or "").strip()

                    if status < 200 or status >= 300:
                        # 5xx 可能是临时问题，允许重试；其它状态码直接失败
                        if 500 <= status <= 599 and attempt < retries:
                            logger.warning(
                                f"{label}下载失败({attempt + 1}/{retries + 1}): HTTP {status} {reason} | {url}"
                            )
                            continue

                        logger.error(f"{label}下载失败: HTTP {status} {reason} | {url}")
                        return False

                    # 流式写入，避免一次性读入内存
                    async with aiofiles.open(tmp_path, "wb") as f:
                        async for chunk in response.content.iter_chunked(64 * 1024):
                            await f.write(chunk)

                await asyncio.to_thread(os.replace, tmp_path, dest)
                return True
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                http_info = f"HTTP {status} {reason} | " if status is not None else ""
                if attempt < retries:
                    logger.warning(
                        f"{label}下载失败({attempt + 1}/{retries + 1}): {http_info}Timeout | {url}"
                    )
                    await asyncio.sleep(0.2 * (attempt + 1))
                    continue
                logger.error(f"{label}下载失败: {http_info}Timeout | {url}")
            except aiohttp.ClientPayloadError as e:
                msg = str(e).strip()
                # 该类错误通常带有较长的内部异常信息，保持简短即可
                if ":" in msg:
                    msg = msg.split(":", 1)[0].strip()
                if len(msg) > 200:
                    msg = msg[:200] + "..."
                http_info = f"HTTP {status} {reason} | " if status is not None else ""
                if attempt < retries:
                    logger.warning(
                        f"{label}下载失败({attempt + 1}/{retries + 1}): {http_info}{type(e).__name__}: {msg} | {url}"
                    )
                    await asyncio.sleep(0.2 * (attempt + 1))
                    continue
                logger.error(
                    f"{label}下载失败: {http_info}{type(e).__name__}: {msg} | {url}"
                )
            except aiohttp.ClientError as e:
                msg = str(e).strip()
                if len(msg) > 200:
                    msg = msg[:200] + "..."
                http_info = f"HTTP {status} {reason} | " if status is not None else ""
                if attempt < retries:
                    logger.warning(
                        f"{label}下载失败({attempt + 1}/{retries + 1}): {http_info}{type(e).__name__}: {msg} | {url}"
                    )
                    await asyncio.sleep(0.2 * (attempt + 1))
                    continue
                logger.error(
                    f"{label}下载失败: {http_info}{type(e).__name__}: {msg} | {url}"
                )
            except Exception as e:
                msg = str(e).strip()
                if len(msg) > 200:
                    msg = msg[:200] + "..."
                http_info = f"HTTP {status} {reason} | " if status is not None else ""
                if attempt < retries:
                    logger.warning(
                        f"{label}下载失败({attempt + 1}/{retries + 1}): {http_info}{type(e).__name__}: {msg} | {url}"
                    )
                    await asyncio.sleep(0.2 * (attempt + 1))
                    continue
                logger.error(
                    f"{label}下载失败: {http_info}{type(e).__name__}: {msg} | {url}"
                )
            finally:
                try:
                    if tmp_path.exists():
                        tmp_path.unlink()
                except Exception:
                    pass

        return False

    async def _collect_all_background_urls(self) -> List[str]:
        background_files = await asyncio.to_thread(
            lambda: [f for f in os.listdir(self.background_dir) if f.endswith(".txt")]
        )

        urls: set[str] = set()
        for background_file in background_files:
            background_file_path = os.path.join(self.background_dir, background_file)
            try:
                async with aiofiles.open(background_file_path, "r", encoding="utf-8") as f:
                    async for line in f:
                        url = line.strip()
                        if not url:
                            continue
                        if url.startswith("http://") or url.startswith("https://"):
                            urls.add(url)
            except Exception as e:
                logger.warning(f"读取背景图列表失败: {background_file_path} | {e}")

        return sorted(urls)

    async def _pre_cache_background_images(self) -> None:
        self._ensure_storage_dirs()

        urls = await self._collect_all_background_urls()
        total = len(urls)
        if total == 0:
            logger.warning("预缓存背景图：未找到任何图片 URL")
            return

        try:
            concurrency = int(self.config.get("pre_cache_concurrency", 3))
        except Exception:
            concurrency = 3
        concurrency = max(1, min(concurrency, 10))

        already_cached = 0
        to_download: List[Tuple[str, Path]] = []
        for url in urls:
            dest = self._background_cache_path_for_url(url)
            if dest.exists():
                already_cached += 1
            else:
                to_download.append((url, dest))

        logger.info(
            f"预缓存背景图开始: total={total}, cached={already_cached}, download={len(to_download)}, concurrency={concurrency}"
        )

        if hasattr(self, "put_kv_data"):
            try:
                await self.put_kv_data(
                    "bg_cache_status",
                    {
                        "status": "running",
                        "total": total,
                        "cached": already_cached,
                        "download": len(to_download),
                        "started_at": datetime.now().isoformat(),
                    },
                )
            except Exception as e:
                logger.warning(f"写入 KV 缓存状态失败: {e}")

        sem = asyncio.Semaphore(concurrency)

        async def _dl(url: str, dest: Path) -> bool:
            if dest.exists():
                return True
            async with sem:
                if dest.exists():
                    return True
                return await self._download_to_path(url, dest, label="背景图")

        downloaded = 0
        failed = 0
        cancelled = False
        try:
            results = await asyncio.gather(
                *(_dl(url, dest) for url, dest in to_download),
                return_exceptions=True,
            )
            for r in results:
                if r is True:
                    downloaded += 1
                else:
                    # False 或 Exception 都算失败（个别 URL 可能已失效）
                    failed += 1
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            if hasattr(self, "put_kv_data"):
                try:
                    await self.put_kv_data(
                        "bg_cache_status",
                        {
                            "status": "cancelled" if cancelled else "done",
                            "total": total,
                            "cached": already_cached,
                            "download": len(to_download),
                            "downloaded": downloaded,
                            "failed": failed,
                            "ended_at": datetime.now().isoformat(),
                        },
                    )
                except Exception as e:
                    logger.warning(f"写入 KV 缓存状态失败: {e}")

        logger.info(
            f"预缓存背景图完成: total={total}, cached={already_cached}, downloaded={downloaded}, failed={failed}"
        )

    # 处理器1：指令处理器
    @filter.command("jrys", alias=["今日运势", "运势"])
    async def jrys_command_handler(self, event: AstrMessageEvent):
        """处理 /jrys, /今日运势, /运势 等指令"""
        logger.info("指令处理器被触发")

        # 关键步骤1: 给事件打上“已处理”标记
        # 利用 event 对象是可变的特性，给它动态添加一个属性
        setattr(event, "_jrys_processed", True)

        # 调用核心业务逻辑
        async for result in self.jrys(event):
            yield result

    @filter.command("jrys_last")
    async def jrys_last_command_handler(self, event: AstrMessageEvent):
        """处理 /jrys_last 指令，发送上一次生成的原图"""
        user_id = event.get_sender_id()
        jrys_data = await self._load_jrys_data()
        
        user_last_images = jrys_data.get("_user_last_images", {})
        if user_id not in user_last_images:
            yield event.plain_result("你还没有生成过今日运势哦，先发送 jrys 生成一张吧！")
            return

        last_info = user_last_images[user_id]
        path = last_info.get("path")

        if not path or not os.path.exists(path):
            yield event.plain_result("找不到上一次生成的原图了，可能已被清理，请重新生成～")
            return

        yield event.image_result(path)

    # 处理器2：关键词处理器
    @filter.event_message_type(filter.EventMessageType.ALL)
    async def jrys_keyword_handler(self, event: AstrMessageEvent, *args, **kwargs):
        """处理 jrys, 今日运势, 运势 等关键词"""

        # 关键步骤2: 检查事件是否已被指令处理器处理过
        if getattr(event, "_jrys_processed", False):
            return  # 如果已被处理，立即退出

        # 如果没被处理过，再进行后续的关键词匹配逻辑
        message_str = event.message_str.strip()
        keywords = {"jrys", "今日运势", "运势"}

        if self.jrys_keyword_enabled and message_str in keywords:
            logger.info("关键词处理器被触发")
            # 调用核心业务逻辑
            async for result in self.jrys(event):
                yield result

    async def jrys(self, event: AstrMessageEvent):
        """
        输入/jrys,"/今日运势", "/运势"指令后，生成今日运势海报
        """

        user_id = event.get_sender_id()
        user_name = event.get_sender_name()

        self.jrys_data = await self._load_jrys_data()  # 确保数据已加载
        if not self.jrys_data:
            logger.error("运势数据未加载或为空")
            yield event.plain_result("运势数据加载失败，请稍后再试～")
            return

        logger.info(f"正在为用户 {user_name}({user_id}) 生成今日运势")

        background_path = None
        background_should_cleanup = False

        try:

            results = await asyncio.gather(
                self.get_avatar_img(user_id),
                self.get_background_image(),
                return_exceptions=True,  # 捕获异常
            )

            avatar_path, background_result = results

            if isinstance(background_result, Exception):
                logger.error(f"获取背景图片时出错: {background_result}")
                yield event.plain_result("获取背景图片失败，请稍后再试～")
                return

            if background_result is None:
                logger.error("获取背景图片失败: 返回为空")
                yield event.plain_result("获取背景图片失败，请稍后再试～")
                return

            background_path, background_should_cleanup = background_result

            if isinstance(avatar_path, Exception):
                logger.error(f"获取头像时出错: {avatar_path}")
                yield event.plain_result("获取头像失败，请稍后再试～")
                if background_should_cleanup and background_path and os.path.exists(background_path):
                    try:
                        await aiofiles.os.remove(background_path)
                    except Exception:
                        pass
                return

        except Exception as e:
            logger.error(f"获取头像或背景图片时出错: {e}")
            yield event.plain_result("获取头像或背景图片失败，请稍后再试～")
            return

        temp_file_path = None  # 用于存储临时文件路径

        try:

            logger.info(f"正在为用户 {user_name}({user_id}) 生成今日运势图片")
            temp_file_path = await asyncio.to_thread(
                self._generate_image_sync, user_id, avatar_path, background_path
            )

            if temp_file_path is None:
                logger.error("生成今日运势图片失败")
                yield event.plain_result("生成图片失败，请稍后再试～")
                return

            yield event.image_result(temp_file_path)
            logger.info(f"成功为用户 {user_name}({user_id}) 生成今日运势图片")

            # 保存最后一次使用的背景图信息到 jrys_data
            if "_user_last_images" not in self.jrys_data:
                self.jrys_data["_user_last_images"] = {}
            
            user_last_images = self.jrys_data["_user_last_images"]
            if user_id in user_last_images:
                old_info = user_last_images[user_id]
                old_path = old_info.get("path")
                # 如果旧图是临时图且与新图不同，则删除旧图
                if old_info.get("should_cleanup") and old_path and old_path != background_path and os.path.exists(old_path):
                    try:
                        await aiofiles.os.remove(old_path)
                    except:
                        pass
            
            user_last_images[user_id] = {
                "path": background_path,
                "should_cleanup": background_should_cleanup
            }
            await self._save_jrys_data() # 保存更新后的 jrys_data
            
            # 标记当前背景图已由 jrys_data 管理，不要在 finally 中清理
            background_should_cleanup = False 

        except Exception as e:
            logger.error(f"生成运势图片过程中出错: {e}")
            yield event.plain_result("生成图片失败，请稍后再试～")

        finally:
            # 用完后删除临时文件

            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    await aiofiles.os.remove(temp_file_path)
                    logger.info(f"成功删除临时文件")

                except OSError as e:
                    logger.warning(f"删除临时文件 {temp_file_path} 失败: {e}")

                except FileNotFoundError:
                    logger.warning(f"临时文件 {temp_file_path} 已经被删除或不存在")
                    pass

                except Exception as e:
                    logger.warning(f"删除临时文件 {temp_file_path} 失败: {e}")

            if (
                background_should_cleanup
                and background_path
                and os.path.exists(background_path)
            ):
                try:
                    await aiofiles.os.remove(background_path)
                except Exception:
                    pass

    def _generate_image_sync(
        self, user_id: str, avatar_path: str, background_path: str
    ) -> Optional[str]:
        """
            同步函数：执行所有CPU密集的图像处理任务。
            这个函数将在一个单独的线程中运行，以避免阻塞asyncio事件循环。
        Args:
            avatar_path (str): 用户头像的路径
            background_path (str): 背景图片的路径
        Returns:
            Optional[str]: 返回生成的运势海报图片路径，如果失败则返回None
        """
        if not self.jrys_data:
            logger.error("运势数据为空")
            return None

        date_y = self.date_y
        summary_y = self.summary_y
        lucky_star_y = self.lucky_star_y
        sign_text_y = self.sign_text_y
        unsign_text_y = self.unsign_text_y
        warning_text_y = self.warning_text_y

        try:
            # 获取当前日期字符串
            today_str = datetime.now().strftime("%Y-%m-%d")

            # 结合用户ID和日期生成一个确定性的种子
            seed = f"{user_id}-{today_str}"

            # 设置随机种子，以确保该用户今日运势固定
            random.seed(seed)

            available_keys_list = list(self.jrys_data.keys())

            key_1 = random.choice(available_keys_list)

            if key_1 not in self.jrys_data:
                logger.error(f"运势数据中没有找到 {key_1} 的数据")
                return None

            key_2 = random.choice(list(range(len(self.jrys_data[key_1]))))
            fortune_data = self.jrys_data[key_1][key_2]

            # 获取当前日期
            now = datetime.now()
            date = f"{now.strftime('%Y/%m/%d')}"

            # 1. 获取运势数据
            fortune_summary = fortune_data.get("fortuneSummary", "运势数据未知")
            lucky_star = fortune_data.get("luckyStar", "幸运星未知")
            sign_text = fortune_data.get("signText", "星座运势未知")
            unsign_text = fortune_data.get("unsignText", "非星座运势未知")
            warning_text = "仅供娱乐 | 相信科学 | 请勿迷信"

            # 如果unsign_lines>3行，怕这个warning_text和unsign_text贴在一起，加个自动换行的
            unsign_lines = self.wrap_text(
                unsign_text, font=self.fonts[36], max_width=TEXT_WRAP_WIDTH
            )

            # 如果unsign_lines>3行，warning_text_y向下移动 unsign_text_y向上移动
            if len(unsign_lines) > 3:
                warning_text_y += (
                    len(unsign_lines) - 3
                ) * WARNING_TEXT_Y_OFFSET  # 每行10像素的间距
                unsign_text_y -= (
                    len(unsign_lines) - 3
                ) * UNSIGN_TEXT_Y_OFFSET  # 每行15像素的间距

            # 2. 核心图像处理流程

            # 裁切图片
            image = self.crop_center(background_path)
            if image is None:
                logger.error("裁剪背景图片失败")
                return None

            # 添加半透明图层
            image = self.add_transparent_layer(
                image, position=(0, 1270), box_width=1080, box_height=700
            )

            # 在图片上绘制文字

            # 绘制日期
            image = self.draw_text(
                image,
                text=date,
                position="center",
                y=date_y,
                color=(255, 255, 255),
                font=self.fonts[50],  # 使用50号字体
                gradients=True,
            )

            # 绘制幸运总结
            image = self.draw_text(
                image,
                text=fortune_summary,
                position="center",
                y=summary_y,
                color=(255, 255, 255),
                font=self.fonts[60],  # 使用60号字体
            )

            # 绘制幸运星
            image = self.draw_text(
                image,
                text=lucky_star,
                position="center",
                y=lucky_star_y,
                color=(255, 255, 255),
                font=self.fonts[60],  # 使用60号字体
                gradients=True,
            )
            # 绘制运势文本
            image = self.draw_text(
                image,
                text=sign_text,
                position="left",
                y=sign_text_y,
                color=(255, 255, 255),
                font=self.fonts[30],  # 使用30号字体
            )
            image = self.draw_text(
                image,
                text=unsign_text,
                position="left",
                y=unsign_text_y,
                color=(255, 255, 255),
                font=self.fonts[30],  # 使用30号字体
            )
            # 绘制警告文本
            image = self.draw_text(
                image,
                text=warning_text,
                position="center",
                y=warning_text_y,
                color=(255, 255, 255),
                font=self.fonts[30],  # 使用30号字体
            )

            # 在图片上绘制用户头像
            image = self.draw_avatar_img(avatar_path, image)

            # 3 . 保存图片到临时文件并且返回路径
            with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as temp_file:
                image = image.convert("RGB")  # 确保图片是RGB模式
                image.save(temp_file, format="JPEG", quality=85, optimize=True)
                return temp_file.name

        except Exception as e:
            logger.error(f"获取运势数据失败: {e}")
            return None

        finally:
            # 无论成功或失败，最后都重置随机种子，以免影响程序其他部分的随机性
            random.seed(None)

    async def _load_jrys_data(self) -> dict:
        """
        初始化 jrys.json 文件
        1. 检查当前目录下是否存在 jrys.json 文件
        2. 如果不存在，则创建一个空的 jrys.json 文件
        3. 如果存在，则读取文件内容
        4. 如果文件内容不是有效的 JSON 格式，则打印错误信息
        """

        if self.is_data_loaded:
            return self.jrys_data

        jrys_path = os.path.join(self.data_dir, "jrys.json")

        # 检查 jrys.json 文件是否存在,如果不存在，则创建一个空的 jrys.json 文件
        if not os.path.exists(jrys_path):
            async with aiofiles.open(jrys_path, "w", encoding="utf-8") as f:
                await f.write(json.dumps({}))
                logger.info(f"创建空的运势数据文件: {jrys_path}")

        # 读取 JSON 文件
        try:
            async with aiofiles.open(jrys_path, "r", encoding="utf-8") as f:
                content = await f.read()
                # json.loads是CPU密集型，用 to_thread 包装
                self.jrys_data = await asyncio.to_thread(json.loads, content)
                self.is_data_loaded = True  # 标记数据已加载
                logger.info(f"读取运势数据文件: {jrys_path}")

            return self.jrys_data

        except FileNotFoundError:
            logger.error(f"文件 {jrys_path} 没找到")
            return {}
        except json.JSONDecodeError:
            logger.error(f"文件 {jrys_path} 不是有效的 JSON 格式")
            return {}

    async def _save_jrys_data(self):
        """保存 jrys 数据到 jrys.json"""
        jrys_path = os.path.join(self.data_dir, "jrys.json")
        try:
            async with aiofiles.open(jrys_path, "w", encoding="utf-8") as f:
                content = await asyncio.to_thread(json.dumps, self.jrys_data, ensure_ascii=False, indent=4)
                await f.write(content)
        except Exception as e:
            logger.error(f"保存运势数据失败: {e}")

    async def get_background_image(self) -> Optional[Tuple[str, bool]]:
        """
        随机获取背景图片
        1. 在当前目录下的 backgroundFolder 文件夹中查找所有的 txt 文件
        2. 随机选择一个 txt 文件
        3. 从选中的 txt 文件中随机选择一行
        4. 将选中的行作为图片的 URL
        5.返回图片路径，以及是否需要清理
        """

        try:
            self._ensure_storage_dirs()

            # 查找所有的 txt 文件
            background_files = await asyncio.to_thread(
                lambda: [
                    f for f in os.listdir(self.background_dir) if f.endswith(".txt")
                ]
            )

            if not background_files:
                logger.warning("没有找到背景图片文件")
                return None
            # 随机选择一个 txt 文件
            background_file = random.choice(background_files)
            background_file_path = os.path.join(self.background_dir, background_file)

            # 从选中的 txt 文件中随机选择一行
            async with aiofiles.open(background_file_path, "r", encoding="utf-8") as f:

                # 读取文件内容
                background_urls = [line.strip() async for line in f if line.strip()]

                if not background_urls:
                    logger.warning(f"文件 {background_file} 中没有找到有效的 URL")
                    return None

                # 尝试多个 URL，避免个别链接失效导致整体失败
                random.shuffle(background_urls)
                max_attempts = min(5, len(background_urls))

                pre_cache_enabled = bool(
                    self.config.get("pre_cache_background_images", False)
                )
                cleanup_downloads = bool(
                    self.config.get("cleanup_background_downloads", True)
                )

                for image_url in background_urls[:max_attempts]:
                    if not (
                        image_url.startswith("http://")
                        or image_url.startswith("https://")
                    ):
                        continue

                    cache_path = self._background_cache_path_for_url(image_url)

                    # 已缓存则直接返回（持久化缓存不做清理）
                    if cache_path.exists():
                        return str(cache_path), False

                    # 未启用预缓存时：默认按需下载后清理；关闭开关则仍然写入持久化缓存目录
                    image_path = cache_path
                    should_cleanup = False
                    if (not pre_cache_enabled) and cleanup_downloads:
                        image_path = self._background_tmp_path_for_url(image_url)
                        should_cleanup = True

                    ok = await self._download_to_path(image_url, image_path, label="背景图")
                    if ok:
                        logger.info(f"下载图片成功: {image_url}")
                        return str(image_path), should_cleanup

                logger.warning(f"背景图下载失败: 已尝试 {max_attempts} 个 URL")
                return None

        except Exception as e:
            logger.error(f"获取背景图片时出错: {e}")
            return None

    def draw_text(
        self,
        img: Image.Image,
        text: str,
        position: str,
        font: ImageFont.ImageFont,
        y: int = None,
        color: Tuple[int, int, int] = (255, 255, 255),
        max_width: int = 800,
        gradients: bool = False,
    ) -> Image.Image:
        """
        在图片上绘制文字
        参数：
            img (Image): 要绘制的图片
            text (str): 要绘制的文字
            position (tuple or str): 文字的位置, 可为'left','center'或坐标元组
            y (int): 文字的y坐标,如果position为'topleft'或'center',则y无效
            color (tuple): 文字颜色，默认为白色
            font (ImageFont): 字体对象,如果为None则使用默认字体
            max_width (int): 文字的最大宽度,默认为800
            gradients (bool): 是否使用渐变色填充文字，默认为False
        """

        try:
            draw = ImageDraw.Draw(img)

            # 自动换行处理
            lines = self.wrap_text(
                text=text,
                font=font,
                draw=draw,
                max_width=TEXT_WRAP_WIDTH,
            )  # 将文字按最大宽度进行换行

            # 获取图片的宽高
            img_width, img_height = img.size

            if isinstance(position, str):
                if position == "center":

                    def x_func(line):
                        bbox = draw.textbbox((0, 0), line, font=font)
                        line_width = bbox[2] - bbox[0]  # 获取文字宽度
                        return (img_width - line_width) // 2  # 计算x坐标

                    def offset_x_func(line):
                        bbox = draw.textbbox((0, 0), line, font=font)
                        return -bbox[0]

                elif position == "left":

                    def x_func(line):
                        return LEFT_PADDING  # 固定左侧留白

                    def offset_x_func(line):
                        return 0

                else:
                    raise ValueError(
                        "position参数错误,只能为'topleft','center'或坐标元组"
                    )
                # 计算y坐标
                text_y = y if y is not None else 0
            elif isinstance(position, tuple):
                text_x, text_y = position

                def x_func(line):
                    return text_x

                def offset_x_func(line):
                    return 0

            else:
                raise ValueError("position参数错误,只能为'left','center'或坐标元组")

            # 绘制每一行
            line_spacing = int(font.size * 1.5)  # 行间距
            for line in lines:
                if gradients:
                    base_x = x_func(line)
                    offset_x = offset_x_func(line)
                    for char in line:
                        #
                        colors = self.get_light_color()
                        gradient_char = self.create_gradients_image(char, font, colors)
                        img.paste(
                            gradient_char, (base_x + offset_x, text_y), gradient_char
                        )

                        bbox = font.getbbox(char)
                        char_width = bbox[2] - bbox[0]  # 获取字符宽度
                        base_x += char_width  # 更新x坐标
                        offset_x += bbox[0]  # 更新偏移量

                else:
                    # 绘制普通文字
                    offset_x = offset_x_func(line)  # 获取偏移量
                    draw.text(
                        (x_func(line) + offset_x, text_y), line, font=font, fill=color
                    )

                text_y += line_spacing  # 更新y坐标

            return img

        except Exception as e:
            logger.error(f"绘制文字时出错: {e}")
            return img

    def crop_center(
        self, image_path: str, width: int = None, height: int = None
    ) -> Optional[Image.Image]:
        """
        从图片中间裁剪指定尺寸的区域，如果图片尺寸小于目标尺寸，则先放大,太大则缩小。

        参数：

            width (int): 裁剪宽度，默认为 1080 像素。
            height (int): 裁剪高度，默认为 1920 像素。

        返回：
            Image.Image: 裁剪后的图片对象，如果发生错误则返回 None。
        """
        width = width if width is not None else self.image_width
        height = height if height is not None else self.image_height
        try:
            img = Image.open(image_path).convert("RGBA")
            img_width, img_height = img.size

            # 如果图片尺寸小于目标尺寸，则先放大
            if img_width < width or img_height < height:
                scale_x = width / img_width
                scale_y = height / img_height
                scale = max(scale_x, scale_y)  # 保持比例，选择较大的缩放倍数
                new_width = int(img_width * scale)
                new_height = int(img_height * scale)
                img = img.resize((new_width, new_height), Image.LANCZOS)  #

            # 如果图片尺寸远大于目标尺寸

            else:
                max_scale = 1.8  # 防止图片太大浪费资源
                if img_width > width * max_scale or img_height > height * max_scale:
                    scale_x = (width * max_scale) / img_width
                    scale_y = (height * max_scale) / img_height
                    scale = min(scale_x, scale_y)
                    new_width = int(img_width * scale)
                    new_height = int(img_height * scale)
                    img = img.resize((new_width, new_height), Image.LANCZOS)

            # 重新获取放大后的图片尺寸
            img_width, img_height = img.size

            left = (img_width - width) / 2
            top = (img_height - height) / 2
            right = (img_width + width) / 2
            bottom = (img_height + height) / 2

            # 创建半透明图层

            cropped_img = img.crop((left, top, right, bottom))

            return cropped_img

        except FileNotFoundError:
            logger.error(f"错误：找不到图片文件：{image_path}")
        except Exception as e:
            logger.error(f"发生错误：{e}")
            return None

    def add_transparent_layer(
        self,
        base_img: Image.Image,
        box_width: int = 800,
        box_height: int = 400,
        position: Tuple[int, int] = (100, 200),
        layer_color: Tuple[int, int, int, int] = (0, 0, 0, 128),
        radius: int = 50,
    ) -> Image.Image:
        """
        在图片上添加一个半透明图层

        参数：
            base_img (Image): 背景图像（RGBA 格式）
            text (str): 要绘制的文字内容
            box_width (int): 半透明框的宽度
            box_height (int): 半透明框的高度
            position (tuple): 半透明框的位置
            layer_color (tuple): 半透明层颜色，RGBA 格式
            radius (int): 圆角半径
        返回：
            合成后的 Image 对象
        """
        try:
            x1, y1 = position
            x2 = x1 + box_width
            y2 = y1 + box_height

            # 创建半透明图层
            overlay = Image.new("RGBA", base_img.size, (0, 0, 0, 0))
            draw = ImageDraw.Draw(overlay)

            draw.rounded_rectangle((x1, y1, x2, y2), radius=radius, fill=layer_color)

            return Image.alpha_composite(base_img, overlay)

        except Exception as e:
            logger.error(f"添加半透明图层时出错: {e}")
            return base_img

    def wrap_text(
        self,
        text: str,
        font: ImageFont.ImageFont,
        draw: ImageDraw.ImageDraw = None,
        max_width: int = TEXT_WRAP_WIDTH,
    ) -> List[str]:
        """
        将文字按最大宽度进行换行
        参数：
            text (str): 原始文字
            max_width (int): 最大宽度
            draw: ImageDraw对象，用于测量文字宽度
            font: ImageFont对象
        返回：
            list[str]: 每行一段文字

        """
        try:
            if draw is None:
                img = Image.new("RGB", (self.image_width, self.image_height))
                draw = ImageDraw.Draw(img)

            lines: List[str] = []
            current_line = ""
            for char in text:
                test_line = current_line + char
                bbox = draw.textbbox((0, 0), test_line, font=font)
                width = bbox[2] - bbox[0]  # 获取文字宽度
                if width <= max_width:
                    current_line = test_line
                else:
                    lines.append(current_line)
                    current_line = char
            if current_line:
                lines.append(current_line)
            return lines
        except Exception as e:
            logger.error(f"换行时出错: {e}")
            return [text]  # 如果出错，返回原始文本

    def create_gradients_image(
        self, char: str, font, colors: List[Tuple[int, int, int]]
    ) -> Image.Image:
        """
        创建渐变色字体图像
        参数：
            char (str): 要绘制的字符
            font: ImageFont对象
            colors (list of tuple): 渐变色列表，包含起始和结束颜色

        Returns:
            Image: 渐变色字体图像

        """
        try:
            bbox = font.getbbox(char)
            width = bbox[2] - bbox[0]  # 字符宽度
            height = bbox[3] - bbox[1]  # 字符高度
            if width <= 0 or height <= 0:
                width, height = font.getsize(
                    char
                )  # 如果获取的宽度或高度为0，则使用字体大小
                offset_x, offset_y = 0, 0

            else:
                # 计算偏移量
                offset_x = -bbox[0]
                offset_y = -bbox[1]

            gradient = Image.new("RGBA", (width, height), color=0)
            draw = ImageDraw.Draw(gradient)

            # 字体蒙版
            mask = Image.new("L", (width, height), 0)
            mask_draw = ImageDraw.Draw(mask)
            mask_draw.text((offset_x, offset_y), char, font=font, fill=255)

            num_colors = len(colors)
            if num_colors < 2:
                raise ValueError("至少需要两个颜色进行渐变")

            # 绘制横向多颜色渐变色条
            segement_width = width / (num_colors - 1)  # 每个颜色段的宽度
            for i in range(num_colors - 1):
                start_color = colors[i]
                end_color = colors[i + 1]
                start_x = int(i * segement_width)
                end_x = int((i + 1) * segement_width)

                for x in range(start_x, end_x):
                    factor = (x - start_x) / segement_width
                    color = tuple(
                        [
                            int(
                                start_color[j]
                                + (end_color[j] - start_color[j]) * factor
                            )
                            for j in range(3)
                        ]
                    )
                    draw.line([(x, 0), (x, height)], fill=color)

            gradient.putalpha(mask)  # 添加蒙版

            return gradient
        except Exception as e:
            logger.error(f"创建渐变色字体图像时出错: {e}")
            # 如果出错，返回一个透明图像

            img = Image.new("RGBA", (width, height), (255, 255, 255, 0))
            draw = ImageDraw.Draw(img)
            draw.text((0, 0), char, font=font, fill=(255, 255, 255))
            return img

    def get_light_color(self) -> List[Tuple[int, int, int]]:
        """获取浅色调颜色列表用于渐变

        Returns:
            浅色调颜色列表
        """

        light_colors = [
            (255, 250, 205),  # 浅黄色
            (173, 216, 230),  # 浅蓝色
            (221, 160, 221),  # 浅紫色
            (255, 182, 193),  # 浅粉色
            (240, 230, 140),  # 浅卡其色
            (224, 255, 255),  # 浅青色
            (245, 245, 220),  # 浅米色
            (230, 230, 250),  # 浅薰衣草色
        ]
        return random.choices(light_colors, k=4)  # 随机选4个颜色进行渐变

    async def get_avatar_img(self, user_id: str) -> Optional[str]:
        """
        获取用户头像
          1. 获取用户头像2. 获取用户头像的 URL3. 下载头像4. 返回头像的路径
        Args:
            user_id (str): 用户 ID

        Returns:
            str: 头像的路径
        """
        try:
            self._ensure_storage_dirs()
            avatar_path = os.path.join(self.avatar_dir, f"{user_id}.jpg")
            # 检查头像是否存在
            if await aiofiles.os.path.exists(avatar_path):

                def _file_stat(path):
                    try:
                        st = os.stat(path)
                        return st.st_mtime
                    except FileNotFoundError:
                        return None

                file_mtime = await asyncio.to_thread(_file_stat, avatar_path)
                file_age = datetime.now().timestamp() - file_mtime
                if (
                    file_age < self.avatar_cache_expiration
                ):  # 默认如果头像文件小于一天，则不下载
                    return avatar_path

            url = f"http://q.qlogo.cn/g?b=qq&nk={user_id}&s=640"

            ok = await self._download_to_path(url, Path(avatar_path), label="头像")
            if ok:
                return avatar_path
            return None

        except Exception as e:
            logger.error(f"获取用户头像失败: {e}")
            return None

    def draw_avatar_img(self, avatar_path: str, img: Image.Image) -> Image.Image:
        """
        在图片上绘制用户头像
        1. 获取用户头像
        2. 将头像裁剪为圆形
        3. 将头像绘制到图片上
        Args:
            avatar_path (str): 头像的路径
            img (Image): 要绘制的图片
        Returns:
            Image: 绘制了头像的图片
        """
        try:
            avatar = Image.open(avatar_path).convert("RGBA")
            avatar = avatar.resize(self.avatar_size, Image.LANCZOS)

            # 创建一个与头像尺寸相同的透明蒙版
            mask = Image.new("L", avatar.size, 0)
            mask_draw = ImageDraw.Draw(mask)

            # 绘制一个白色的圆形，作为不透明区域
            mask_draw.ellipse((0, 0, avatar.size[0], avatar.size[1]), fill=255)

            # 将蒙版应用到头像上
            avatar.putalpha(mask)

            # 将头像粘贴到图片上
            img.paste(avatar, self.avatar_position, avatar)

            return img
        except Exception as e:
            logger.error(f"绘制头像时出错: {e}")
            # 如果出错，返回原始图片
            return img

    async def terminate(self):
        """插件终止时的清理工作"""
        if self._precache_task and not self._precache_task.done():
            self._precache_task.cancel()
            try:
                await self._precache_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.warning(f"预缓存任务清理失败: {e}")

        if self._session:
            await self._session.close()
            logger.info("HTTP会话已关闭")

        logger.info("今日运势插件已终止")
