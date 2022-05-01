import logging
import os
import vk_api

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
)

logging.getLogger("vk_api").setLevel(logging.WARNING)

def getenv(name):
    res = os.getenv(name)
    if not res:
        logger.error("variable %s not set in environment", name)
        exit(0)
    return res

login = getenv("VK_LOGIN")
password = getenv("VK_PASSWORD")
vk_session = vk_api.VkApi(login, password)
vk_session.auth()
vk_tools = vk_api.VkTools(vk_session)
