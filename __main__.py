import argparse
import datetime
import itertools
import json
import logging
import os
import pandas
import vk_api
from collections import defaultdict
from tqdm.auto import tqdm

from vk_api_helpers import vk_tools

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--group_ids_path", help="Vk group ids path with list of group_id. Default configs/group_ids.tsv", default="configs/group_ids.tsv")
    parser.add_argument("--group_id", help="Vk group id", nargs="*")
    parser.add_argument("--posts_input_file", help="Instead of loading by bot")
    parser.add_argument("--max_days_ago", help="How much days ago data will download", type=int)
    parser.add_argument("--allow_fields", help="Which fields from post will be saved", nargs="*",
                        default=["marked_as_ads", "copy_history", "date", "from_id", "id", "owner_id", "post_source", "post_type", "text"])
    parser.add_argument("--max_posts", help="How many posts may be downloaded", type=int)
    parser.add_argument("--deduplicate_by_text", help="Save posts where unique text", action="store_true")
    parser.add_argument("--output_posts_path", help="Path for saving posts", default="posts.json")
    parser.add_argument("--output_reposts_path", help="Path for saving reposts", default="reposts.json")
    return parser.parse_args()


def get_filtered_post_info(post, allow_fields):
    if not allow_fields:
        return post

    return {k: v for k, v in post.items() if k in allow_fields}


def is_repost(post):
    return post.get("copy_history") != None


def download_posts(vk_tools, owner_id, group_domain, allow_fields, max_posts, start_ts, max_days_ago, last_download_date):
    if last_download_date == start_ts:
        return [], []

    download_from_ts = max(start_ts - pandas.Timedelta(max_days_ago, "d"), last_download_date)
    def stop_fn(items):
        return any(map(lambda x: download_from_ts > pandas.Timestamp(x["date"], unit="s"), items))

    logger.info("Downloading posts from https://vk.com/%s", group_domain or f"club{abs(owner_id)}")
    posts = []
    reposts = []
    request_params = {"domain": group_domain} if group_domain else {"owner_id": -abs(owner_id)}
    wall_iter = vk_tools.get_all_iter("wall.get", 20, request_params, limit=max_posts, stop_fn=stop_fn)
    try:
        for post in tqdm(wall_iter, total=max_posts):
            if not is_repost(post):
                posts.append(get_filtered_post_info(post, allow_fields))
            else:
                reposts.append(post)
    except KeyboardInterrupt:
        exit(1)
    except:
        logger.exception("Got error")

    logger.info("Downloading done: %s posts and %s reposts parsed", len(posts), len(reposts))
    return posts, reposts


def deduplicate_by_id(posts):
    id2post = {post["id"]: post for post in posts}
    logger.info("Removed %s duplicated by id %s", len(posts) - len(id2post), "repost" if is_repost(posts[-1]) else "post")
    return list(id2post.values())


def deduplicate_by_text(posts):
    text2post = {}
    empty_text = 0
    for post in posts:
        if len(post["text"]) < 10:
            empty_text += 1
            continue
        text2post[post["text"]] = post

    logger.info("Removed %s empty text posts", empty_text)
    logger.info("Removed %s duplicated by text posts", len(posts) - len(text2post) - empty_text)
    return list(text2post.values())


def save_posts(posts, filename):
    logger.info("Saving %s posts to %s", len(posts), filename)
    with open(filename, "w") as fout:
        json.dump(posts, fout, ensure_ascii=False, indent=2, sort_keys=True)


def try_load_saved_posts(filepath):
    try:
        logger.info("Reading recently saved posts from %s", filepath)
        with open(filepath, "r") as fin:
            return json.load(fin)
    except:
        logger.warning("Failed reading saved posts. Before saving downloaded posts, data in file %s will be reset", filepath)
        return []


def main():
    config = parse_args()
    all_posts = try_load_saved_posts(config.output_posts_path)
    all_reposts = try_load_saved_posts(config.output_reposts_path)

    try:
        group_ids = pandas.read_csv(config.group_ids_path, sep="\t", parse_dates=["last_download_date"])
    except:
        if not config.group_id:
            logger.error("--group_id required for start")
            return
        group_ids = pandas.DataFrame(columns=("group_id", "last_download_date"))

    if config.group_id:
        group_ids = pandas.concat(group_ids, pandas.DataFrame([[group_id, pandas.Timestamp("2000-05-01")] for group_id in config.group_id], group_ids.columns))

    group_ids.set_index("group_id", inplace=True)
    start_ts = pandas.Timestamp(datetime.date.today())
    for row in group_ids.itertuples():
        posts, reposts = download_posts(
            vk_tools=vk_tools,
            owner_id=row.Index,
            group_domain=None,
            allow_fields=set(config.allow_fields or []),
            max_posts=config.max_posts,
            start_ts=start_ts,
            max_days_ago=config.max_days_ago,
            last_download_date=row.last_download_date,
        )
        group_ids.loc[group_ids.index == row.Index, "last_download_date"] = max(row.last_download_date, start_ts)
        all_posts.extend(posts)
        all_reposts.extend(reposts)

    all_posts = deduplicate_by_id(all_posts)
    all_reposts = deduplicate_by_id(all_reposts)

    if config.deduplicate_by_text:
        all_posts = deduplicate_by_text(all_posts)

    save_posts(all_posts, config.output_posts_path)
    save_posts(all_reposts, config.output_reposts_path)
    group_ids.to_csv(config.group_ids_path, sep="\t")


if __name__ == "__main__":
    main()
