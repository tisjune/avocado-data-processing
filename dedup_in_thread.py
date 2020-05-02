import sys
import os
import json
import pprint
from collections import defaultdict
from extraction_utils import dedup_thread
from settings import N_BINS, N_THREAD_BINS, OUT_DIR
from multiprocessing import Pool

def get_email_id(email):
	file = email['file'].replace('.txt','')
	in_file_idx = email['in_file_idx']
	return '%s.%02d' % (file, in_file_idx)


def dedup_in_thread(bin_idx, indir, outdir):
	print(bin_idx)
	task_info = {'thread_bin_idx': bin_idx, 'n_threads': 0, 
		'n_input_emails': 0, 'n_emails': 0}

	email_dict = {}
	# yes this is duplicating work
	thread_sets = defaultdict(set)
	bin_indir = os.path.join(indir, '%02d' % bin_idx)
	for file in os.listdir(bin_indir):
		print(bin_idx, file)
		with open(os.path.join(bin_indir, file)) as f:
			curr_email_dict = json.load(f)
			for key, email in curr_email_dict.items():
				email_dict[key] = email
				thread_sets[email['thread_root_key']].add((key, email['thread_level']))
	task_info['n_threads'] = len(thread_sets)
	task_info['n_input_emails'] = len(email_dict)

	deduped_thread_sets = {}
	deduped_emails = {}
	for thread, thread_set in thread_sets.items():
		deduped_thread_sets[thread] = dedup_thread(thread_set, email_dict)
		for key, _ in deduped_thread_sets[thread]:
			deduped_emails[key] = email_dict[key]

	parent_child_mappings = defaultdict(set)
	child_parent_mappings = {}
	reindexed_emails = {}
	# reindex and rebuild tree
	for key, email in deduped_emails.items():
		email['id'] = get_email_id(email)
		if 'reply_key' in email:
			email['parent_id'] = get_email_id(deduped_emails[email['dedup_reply_key']])
		else:
			email['parent_id'] = 'ROOT'
		email['thread_id'] = get_email_id(deduped_emails[email['thread_root_key']])
		reindexed_emails[email['id']] = email
		child_parent_mappings[email['id']] = email['parent_id']
		parent_child_mappings[email['parent_id']].add(email['id'])
	for key, email in reindexed_emails.items():
		email['children_id'] = list(parent_child_mappings[key])

	print(len(deduped_emails), len(reindexed_emails))
	task_info['n_emails'] = len(deduped_emails)
	pprint.pprint(task_info)

	with open(os.path.join(outdir, 'content', '%02d.json' % bin_idx), 'w') as f:
		json.dump(reindexed_emails, f)
	with open(os.path.join(outdir, 'tree', '%02d.up.json' % bin_idx), 'w') as f:
		json.dump(child_parent_mappings, f)
	with open(os.path.join(outdir, 'tree', '%02d.down.json' % bin_idx), 'w') as f:
		json.dump({k: list(v) for k, v in parent_child_mappings.items()}, f)
	with open(os.path.join(outdir, 'log', '%02d.json' % bin_idx), 'w') as f:
		json.dump(task_info, f)

if __name__ == '__main__':
	indir = os.path.join(OUT_DIR, 'reduce_dedup_thread/content')
	outdir = os.path.join(OUT_DIR, 'email_threads')
	pool = Pool(4)
	pool.starmap(dedup_in_thread, [(thread_bin_idx, indir, outdir)
			for thread_bin_idx in range(N_THREAD_BINS)])
