import sys
import os
import json
import pprint
from collections import defaultdict
from extraction_utils import reduce_dup_key
from settings import N_BINS, OUT_DIR

from multiprocessing import Pool

def resolve_split_dup_keys(bin_idx, indir):
	print(bin_idx)

	# read in all split keys
	split_indir = os.path.join(indir, 'split_keys')
	split_keys = {}
	for file in os.listdir(split_indir):
		with open(os.path.join(split_indir, file)) as f:
			curr_split_keys = json.load(f)
			for dup_key, split_dict in curr_split_keys.items(): 
				split_keys[dup_key] = {k: set(v) for k, v in split_dict.items()}

	print(bin_idx, len(split_keys))
	with open(os.path.join(indir, 'content', '%02d.json' % bin_idx)) as f:
		deduped_emails = json.load(f)
	with open(os.path.join(indir, 'tree', '%02d.up.json' % bin_idx)) as f:
		child_parent_mappings = json.load(f)
	with open(os.path.join(indir, 'tree', '%02d.down.json' % bin_idx)) as f:
		parent_child_mappings = {k: set(v) for k, v in json.load(f).items()}

	n_modified = 0
	for child, parent in child_parent_mappings.items():
		if parent in split_keys:
			new_reply_key = None
			curr_source_files = set(x.split('.txt')[0] 
				for x in deduped_emails[child]['source_files'])
			for new_dup_key, source_files in split_keys[parent].items():
				if len(source_files.intersection(curr_source_files)) > 0:
					new_reply_key = new_dup_key
			if new_reply_key is None:
				new_reply_key = new_dup_key
			deduped_emails[child]['reply_key'] = new_reply_key
			child_parent_mappings[child] = new_reply_key
			if new_reply_key in parent_child_mappings:
				parent_child_mappings[new_reply_key].add(child)
			else:
				parent_child_mappings[new_reply_key] = set([child])
			n_modified += 1
	print(bin_idx, n_modified)

	with open(os.path.join(indir, 'content', '%02d.json' % bin_idx), 'w') as f:
		json.dump(deduped_emails, f)
	with open(os.path.join(indir, 'tree', '%02d.up.json' % bin_idx), 'w') as f:
		json.dump(child_parent_mappings, f)
	with open(os.path.join(indir, 'tree', '%02d.down.json' % bin_idx), 'w') as f:
		json.dump({k: list(v) for k, v in parent_child_mappings.items()}, f)

if __name__ == '__main__':

	root_indir = os.path.join(OUT_DIR, 'reduce_dedup_extracted')
	pool = Pool(2)
	pool.starmap(resolve_split_dup_keys, 
		[(bin_idx, root_indir) for bin_idx in range(N_BINS)])