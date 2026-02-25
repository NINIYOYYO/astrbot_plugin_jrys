from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
from astrbot.api import AstrBotConfig
import os
import asyncio
import aiofiles
import aiofiles.os
from .resources import ResourceManager 
from .painter import FortunePainter


@register("今日运势", "ominus", "一个今日运势海报生成图", "1.0.3")
class JrysPlugin(Star):
    """今日运势插件,可生成今日运势海报"""

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)

        self.config = config    
        self.resources = ResourceManager(self.config)
        self.painter = FortunePainter(self.config)

        # 是否启用关键词触发功能
        self.jrys_keyword_enabled = self.config.get("jrys_keyword_enabled", True)


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
        self.jrys_data = await self.resources._load_jrys_data()
        user_last_images = self.jrys_data.get("_user_last_images", {})
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

        self.jrys_data = await self.resources._load_jrys_data()



        logger.info(f"正在为用户 {user_name}({user_id}) 生成今日运势")

        background_path = None
        background_should_cleanup = False

        try:

            results = await asyncio.gather(
                self.resources.get_avatar_img(user_id),
                self.resources.get_background_image(),
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
                self.painter.generate_image_sync, user_id, avatar_path, background_path, self.jrys_data
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
            await self.resources._save_jrys_data() # 保存更新后的 jrys_data
            
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

    async def terminate(self):
        """插件终止时的清理工作"""
        if self.resources._precache_task and not self.resources._precache_task.done():
            self.resources._precache_task.cancel()
            try:
                await self.resources._precache_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.warning(f"预缓存任务清理失败: {e}")

        if self.resources._session:
            await self.resources._session.close()
            logger.info("HTTP会话已关闭")

        logger.info("今日运势插件已终止")


