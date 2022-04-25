import argparse
import json
import logging
import os
import vk_api
from collections import defaultdict
from tqdm.auto import tqdm

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--group_id", help="Vk group id", type=int)
    parser.add_argument("--posts_input_file", help="Instead of loading by bot")
    parser.add_argument("--allow_fields", help="Which fields from post will be saved", nargs="*")
    parser.add_argument("--max_posts", help="How many posts may be downloaded", type=int)
    parser.add_argument("--deduplicate_by_text", help="Save posts where unique text", action="store_true")
    return parser.parse_args()


def get_filtered_post_info(post, allow_fields):
    if not allow_fields:
        return post

    return {k: v for k, v in post.items() if k in allow_fields}


def download_posts(vk_tools, owner_id, allow_fields, max_posts):
    logger.info("Downloading posts from https://vk.com/club%s", owner_id)
    posts = []
    reposts = []
    wall_iter = vk_tools.get_all_iter("wall.get", 20, {"owner_id": -owner_id}, limit=max_posts)
    try:
        for post in tqdm(wall_iter):
            if post.get("copy_history") == None:
                posts.append(get_filtered_post_info(post, allow_fields))
            else:
                reposts.append(post)
    except:
        logger.exception("Got error")

    logger.info("Downloading done: %s posts and %s reposts parsed", len(posts), len(reposts))
    return posts, reposts


def deduplicate_by_text(posts):
    text2post = {}
    for post in tqdm(posts):
        text2post[post["text"]] = post

    logger.info("Removed %s duplicate posts", len(posts) - len(text2post))
    return list(text2post.values())


def save_posts(posts, filename):
    logger.info("Saving %s posts to %s", len(posts), filename)
    with open(filename, "w") as fout:
        fout.write(json.dumps(posts, ensure_ascii=False, indent=2, sort_keys=True))


def getenv(name):
    res = os.getenv(name)
    if not res:
        logger.error("variable %s not set in environment", name)
        exit(0)
    return res


def main():
    config = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
    )

    if not (bool(config.group_id) ^ bool(config.posts_input_file)):
        logger.error("one parameter is required simultaneously: --group_id or --posts_input_file")
        return

    if config.posts_input_file:
        with open(config.posts_input_file, "r") as fin:
            posts = json.loads(fin.read())
            reposts = []
    else:
        login = getenv("VK_LOGIN")
        password = getenv("VK_PASSWORD")
        vk_session = vk_api.VkApi(login, password)
        vk_session.auth()
        vk_tools = vk_api.VkTools(vk_session)
        posts, reposts = download_posts(vk_tools, config.group_id, set(config.allow_fields), config.max_posts)

    if config.deduplicate_by_text:
        posts = deduplicate_by_text(posts)

    save_posts(posts, "{}_posts.json".format(config.group_id))
    save_posts(reposts, "{}_reposts.json".format(config.group_id))


if __name__ == "__main__":
    main()
