# vk wall parser

A command line program for downloading and preprocessing posts from the group wall https://vk.com

### Fast start
Create and activate virtual environment
```
python3 -m venv ./venv
source venv/bin/activate
```
For first run
```
pip install -r requirements.txt
export VK_LOGIN=<vk bot login>
export VK_PASSWORD=<vk bot password>
python3 __main__.py --help
python3 __main__.py --deduplicate_by_text --max_days_ago 10 --group_id <vk group id> --group_ids_path configs/personal_group_ids.tsv
```
Then `python3 __main__.py --deduplicate_by_text --max_days_ago 10 --group_ids_path configs/group_ids.tsv`, group_ids will be taken from `configs/group_ids.tsv`. It is default value for `--group_ids_path`


### Usefull tools
#### collect group from reposts
```
python3 collect_reposted_groups.py --help
python3 collect_reposted_groups.py
vim configs/reposted_groups.tsv
```
