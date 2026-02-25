from PIL import Image, ImageDraw, ImageFont
from typing import Optional, List, Tuple
from astrbot.api import logger
from datetime import datetime
import tempfile
import random
import os

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


class FortunePainter:
    """
    今日运势海报生成器，负责根据用户头像和背景图生成今日运势海报图片
    """

    def __init__(
        self,
        plugin_config,
    ) -> None:

        self.plugin_config = plugin_config

        self.font_name = self.plugin_config.get("font_name", FONT_NAME)  # 默认字体名称

        self.image_width = self.plugin_config.get("img_width", IMAGE_WIDTH)
        self.image_height = self.plugin_config.get(
            "img_height", IMAGE_HEIGHT
        )  # 默认图片高度

        avatar_position_list = self.plugin_config.get(
            "avatar_position", list(AVATAR_POSITION)
        )
        self.avatar_position = tuple(avatar_position_list)  # 默认头像位置

        avatar_size_list = self.plugin_config.get("avatar_size", list(AVATAR_SIZE))
        self.avatar_size = tuple(avatar_size_list)

        self.date_y = self.plugin_config.get("date_y_position", DATE_Y)
        self.summary_y = self.plugin_config.get("summary_y_position", SUMMARY_Y)
        self.lucky_star_y = self.plugin_config.get(
            "lucky_star_y_position", LUCKY_STAR_Y
        )
        self.sign_text_y = self.plugin_config.get("sign_text_y_position", SIGN_TEXT_Y)
        self.unsign_text_y = self.plugin_config.get(
            "unsign_text_y_position", UNSIGN_TEXT_Y
        )
        self.warning_text_y = self.plugin_config.get(
            "warning_text_y_position", WARNING_TEXT_Y
        )

        self.data_dir = os.path.dirname(os.path.abspath(__file__))
        self.avatar_dir = os.path.join(self.data_dir, "avatars")
        self.background_dir = os.path.join(self.data_dir, "backgroundFolder")
        self.font_dir = os.path.join(self.data_dir, "font")
        self.font_path = os.path.join(self.data_dir, "font", self.font_name)

        os.makedirs(self.avatar_dir, exist_ok=True)
        os.makedirs(self.background_dir, exist_ok=True)
        os.makedirs(self.font_dir, exist_ok=True)

        # 是否启用关键词触发功能
        self.jrys_keyword_enabled = self.plugin_config.get("jrys_keyword_enabled", True)
        # 是否启用节假期爆率调整功能
        self.holiday_rates_enabled = self.plugin_config.get(
            "holiday_rates_enabled", True
        )
        # 是否启用固定运势功能（即每天同一用户的运势相同）
        self.fixed_daily_fortune = self.plugin_config.get("fixed_daily_fortune", True)

        self.holidays = self.plugin_config.get(
            "holidays", ["01-01", "02-14", "05-01", "10-01", "12-25"]
        )

        self.normal_rates = self.plugin_config.get(
            "normal_rates", {"good": 40, "normal": 40, "bad": 20}
        )

        self.holiday_rates = self.plugin_config.get(
            "holiday_rates", {"good": 85, "normal": 15, "bad": 0}
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

    def generate_image_sync(
        self, user_id: str, avatar_path: str, background_path: str, jrys_data: dict
    ) -> Optional[str]:
        if not jrys_data:
            logger.error("运势数据为空")
            return None

        date_y = self.date_y
        summary_y = self.summary_y
        lucky_star_y = self.lucky_star_y
        sign_text_y = self.sign_text_y
        unsign_text_y = self.unsign_text_y
        warning_text_y = self.warning_text_y

        try:
            rng = random.Random()
            # 使用局部的 random 实例，保证并发安全
            if self.fixed_daily_fortune:
                today_str = datetime.now().strftime("%Y-%m-%d")
                seed = f"{user_id}-{today_str}"
                rng.seed(seed)
            else:
                pass

            valid_keys_list = [k for k in jrys_data.keys() if not k.startswith("_")]

            # --- 读取当天的爆率权重 ---
            today_md = datetime.now().strftime("%m-%d")
            # 如果启用了节假日功能且今天在节假日列表中，用 holiday_rates
            if self.holiday_rates_enabled and today_md in self.holidays:
                current_rates = self.holiday_rates
                logger.info(f"触发节假日爆率配置! 日期: {today_md}")
            else:
                current_rates = self.normal_rates

            # --- 对运势分数进行分类 ---
            good_keys = [k for k in valid_keys_list if int(k) > 70]
            normal_keys = [k for k in valid_keys_list if 56 <= int(k) <= 70]
            bad_keys = [k for k in valid_keys_list if int(k) < 56]

            # --- 分配每一个 key 具体的权重 ---
            weights = []
            for k in valid_keys_list:
                val = int(k)
                if val > 70:
                    weights.append(
                        current_rates.get("good", 40) / max(len(good_keys), 1)
                    )
                elif val >= 56:
                    weights.append(
                        current_rates.get("normal", 40) / max(len(normal_keys), 1)
                    )
                else:
                    weights.append(current_rates.get("bad", 20) / max(len(bad_keys), 1))

            # 极端情况防抖：如果所有的滑块都被用户滑到了 0，恢复随机
            if sum(weights) <= 0:
                weights = [1] * len(valid_keys_list)

            # --- 按爆率权重抽签 ---
            key_1 = rng.choices(valid_keys_list, weights=weights, k=1)[0]
            logger.info(f"根据配置权重，成功选择运势一级键: {key_1}")

            if key_1 not in jrys_data:
                logger.error(f"运势数据中没有找到 {key_1} 的数据")
                return None

            key_2 = rng.choice(list(range(len(jrys_data[key_1]))))
            fortune_data = jrys_data[key_1][key_2]

            now = datetime.now()
            date = f"{now.strftime('%Y/%m/%d')}"

            # 1. 获取运势数据
            fortune_summary = fortune_data.get("fortuneSummary", "运势数据未知")
            lucky_star = fortune_data.get("luckyStar", "幸运星未知")
            sign_text = fortune_data.get("signText", "星座运势未知")
            unsign_text = fortune_data.get("unsignText", "非星座运势未知")
            warning_text = "仅供娱乐 | 相信科学 | 请勿迷信"

            unsign_lines = self.wrap_text(
                unsign_text, font=self.fonts[36], max_width=TEXT_WRAP_WIDTH
            )

            if len(unsign_lines) > 3:
                warning_text_y += (len(unsign_lines) - 3) * WARNING_TEXT_Y_OFFSET
                unsign_text_y -= (len(unsign_lines) - 3) * UNSIGN_TEXT_Y_OFFSET

            # 2. 核心图像处理流程
            image = self.crop_center(background_path)
            if image is None:
                logger.error("裁剪背景图片失败")
                return None

            image = self.add_transparent_layer(
                image, position=(0, 1270), box_width=1080, box_height=700
            )

            image = self.draw_text(
                image,
                text=date,
                position="center",
                y=date_y,
                color=(255, 255, 255),
                font=self.fonts[50],
                gradients=True,
            )
            image = self.draw_text(
                image,
                text=fortune_summary,
                position="center",
                y=summary_y,
                color=(255, 255, 255),
                font=self.fonts[60],
            )
            image = self.draw_text(
                image,
                text=lucky_star,
                position="center",
                y=lucky_star_y,
                color=(255, 255, 255),
                font=self.fonts[60],
                gradients=True,
            )
            image = self.draw_text(
                image,
                text=sign_text,
                position="left",
                y=sign_text_y,
                color=(255, 255, 255),
                font=self.fonts[30],
            )
            image = self.draw_text(
                image,
                text=unsign_text,
                position="left",
                y=unsign_text_y,
                color=(255, 255, 255),
                font=self.fonts[30],
            )
            image = self.draw_text(
                image,
                text=warning_text,
                position="center",
                y=warning_text_y,
                color=(255, 255, 255),
                font=self.fonts[30],
            )

            image = self.draw_avatar_img(avatar_path, image)

            with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as temp_file:
                image = image.convert("RGB")
                image.save(temp_file, format="JPEG", quality=85, optimize=True)
                return temp_file.name

        except Exception as e:
            logger.error(f"获取运势数据失败: {e}")
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
        self, char: str, font: ImageFont.ImageFont, colors: List[Tuple[int, int, int]]
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
                width, height = font.size, font.size
                # 如果获取的宽度或高度为0，则使用字体大小
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
