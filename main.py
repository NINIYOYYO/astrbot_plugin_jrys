from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
from astrbot.api import AstrBotConfig
import random
import json
import os
import tempfile
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


@register("今日运势", "ominus", "一个今日运势海报生成图", "1.0.1")
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

    @filter.command("jrys", alias=["今日运势", "运势"])
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

        try:

            results = await asyncio.gather(
                self.get_avatar_img(user_id),
                self.get_background_image(),
                return_exceptions=True,  # 捕获异常
            )

            avatar_path, background_path = results

            if isinstance(avatar_path, Exception):
                logger.error(f"获取头像时出错: {avatar_path}")
                yield event.plain_result("获取头像失败，请稍后再试～")
                return

            if isinstance(background_path, Exception):
                logger.error(f"获取背景图片时出错: {background_path}")
                yield event.plain_result("获取背景图片失败，请稍后再试～")
                return

        except Exception as e:
            logger.error(f"获取头像或背景图片时出错: {e}")
            yield event.plain_result("获取头像或背景图片失败，请稍后再试～")
            return

        temp_file_path = None  # 用于存储临时文件路径

        try:

            logger.info(f"正在为用户 {user_name}({user_id}) 生成今日运势图片")
            temp_file_path = await asyncio.to_thread(
                self._generate_image_sync, avatar_path, background_path
            )

            if temp_file_path is None:
                logger.error("生成今日运势图片失败")
                yield event.plain_result("生成图片失败，请稍后再试～")
                return

            yield event.image_result(temp_file_path)
            logger.info(f"成功为用户 {user_name}({user_id}) 生成今日运势图片")

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


    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_all_message(self, event: AstrMessageEvent):
        """
        输入jrys 今日运势 运势 等关键词后，生成今日运势海报
        """
        message_str = event.message_str.strip()
        if self.jrys_keyword_enabled:
            if message_str == 'jrys' or message_str == '今日运势' or message_str == '运势':
                async for result in self.jrys(event):
                    yield result

        

    def _generate_image_sync(
        self, avatar_path: str, background_path: str
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
                y=self.warning_text_y,
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

    async def get_background_image(self) -> Optional[str]:
        """
        随机获取背景图片
        1. 在当前目录下的 backgroundFolder 文件夹中查找所有的 txt 文件
        2. 随机选择一个 txt 文件
        3. 从选中的 txt 文件中随机选择一行
        4. 将选中的行作为图片的 URL
        5.返回图片路径
        """

        try:
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

                # 随机选择一行URL
                image_url = random.choice(background_urls)

                # 创建图片目录
                image_dir = os.path.join(self.background_dir, "images")
                os.makedirs(image_dir, exist_ok=True)

                image_name = os.path.basename(image_url)
                image_path = os.path.join(image_dir, image_name)

                # 检查图片是否存在,如果存在则返回
                if os.path.exists(image_path):
                    return image_path
                # 下载图片

                try:
                    async with self._session.get(image_url) as response:
                        response.raise_for_status()  # 检查请求是否成功
                        content = await response.read()  # 异步读取响应内容

                        async with aiofiles.open(image_path, "wb") as f:
                            await f.write(content)

                        logger.info(f"下载图片成功: {image_url}")
                    return image_path

                except aiohttp.ClientResponseError as e:
                    logger.error(f"状态码错误: {e}")
                    return None
                except aiohttp.ClientError as e:
                    logger.error(f"请求错误: {e}")
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

            try:
                async with self._session.get(url) as response:
                    response.raise_for_status()
                    content = await response.read()  # 异步读取响应内容

                    async with aiofiles.open(os.path.join(avatar_path), "wb") as f:
                        await f.write(content)

                    return avatar_path

            except aiohttp.ClientResponseError as e:
                logger.error(f"状态码错误: {e}")
                return None
            except aiohttp.ClientError as e:
                logger.error(f"请求错误: {e}")
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
        if self._session:
            await self._session.close()
            logger.info("HTTP会话已关闭")

        logger.info("今日运势插件已终止")
