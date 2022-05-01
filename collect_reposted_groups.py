import argparse
import json
import pandas
from tqdm.auto import tqdm

from vk_api_helpers import vk_session


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reposts_path", help="Path to reposts file. Default reposts.json", default="reposts.json")
    parser.add_argument("--reposted_groups_path", help="Output path to reposted groups file. Default configs/reposted_groups.tsv", default="configs/reposted_groups.tsv")
    return parser.parse_args()


def generate_group_link(owner_id):
    return f"https://vk.com/club{-owner_id}"


def generate_post_link(owner_id, post_id):
    return f"https://vk.com/feed?w=wall{owner_id}_{post_id}"


def get_group_info_from_repost(repost):
    copy_history = repost.get("copy_history")
    if not copy_history:
        return

    original_post_info = copy_history[-1]
    owner_id = original_post_info["owner_id"]
    post_id = original_post_info["id"]
    if owner_id > 0:
        return
    return {
        "group_id": str(-owner_id),
        "group_domain": f"club{-owner_id}",
        "group_link": generate_group_link(owner_id),
        "post_link": generate_post_link(owner_id, post_id),
        "source_repost_link": generate_post_link(repost["owner_id"], repost["id"])
    }


def get_possible_groups_from_reposts(reposts):
    possible_group_infos = list(map(get_group_info_from_repost, reposts))
    vk = vk_session.get_api()
    possible_group_infos = pandas.json_normalize(possible_group_infos).dropna()
    stats = possible_group_infos.groupby(["group_id"]).size().reset_index(name="counts").set_index("group_id")
    possible_group_infos = possible_group_infos.drop_duplicates("group_id")
    possible_group_infos.set_index("group_id", inplace=True)

    additional_infos = []
    for i in tqdm(range(0, len(possible_group_infos), 500)):
        group_ids = ",".join(map(str, list(possible_group_infos.index)[i:i+500]))
        additional_infos.extend(vk.groups.getById(group_ids=group_ids))

    additional_infos = pandas.DataFrame(additional_infos)
    additional_infos["group_id"] = additional_infos.id
    additional_infos["group_id"] = additional_infos["group_id"].apply(str)
    additional_infos.set_index("group_id", inplace=True)
    return stats.join(additional_infos[["name"]]).join(possible_group_infos, how="right").sort_values(["counts", "name"], ascending=False)


def main():
    config = parse_args()
    with open(config.reposts_path, "r") as fin:
        reposts = json.load(fin)

    prev_possible_group = pandas.read_csv(config.reposted_groups_path, sep='\t')
    prev_possible_group.group_id = prev_possible_group.group_id.apply(str)
    prev_possible_group = prev_possible_group[["group_id", "status"]].set_index("group_id")

    possible_groups = get_possible_groups_from_reposts(reposts)
    possible_groups = prev_possible_group.join(possible_groups, how="right")
    column_prefix = ["status", "group_id"]
    possible_groups.reset_index().to_csv("new_" + config.reposted_groups_path, sep='\t', index=False,
        columns=column_prefix + list(filter(lambda x: x not in column_prefix, possible_groups.columns)))


if __name__ == '__main__':
    main()
