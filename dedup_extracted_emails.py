import sys
import os
import json
import pprint
from collections import defaultdict
from extraction_utils import reduce_dup_key
from settings import N_BINS, OUT_DIR

from multiprocessing import Pool

def dedup_extracted_emails(bin_idx, indir, outdir):

	print(bin_idx)
	task_info = {'bin_idx': bin_idx,  'n_input_dup_keys': 0}
	bin_indir = os.path.join(indir, '%02d' % bin_idx)

	dup_key_to_emails = defaultdict(list)
	for file in os.listdir(bin_indir):
		print(bin_idx, file)
		with open(os.path.join(bin_indir, file)) as f:
			extracted_emails = json.load(f)
			task_info['n_input_dup_keys'] += len(extracted_emails)
			for dup_key, emails in extracted_emails.items():
				dup_key_to_emails[dup_key] += emails


	split_dup_keys = {}
	deduped_emails = {}
	parent_child_mappings = defaultdict(set)
	child_parent_mappings = {}

	for dup_key, emails in dup_key_to_emails.items():
		reduced_emails = reduce_dup_key(emails)
		if len(reduced_emails) > 1:
			split_dup_keys[dup_key] = {
				email['dup_key']: list(set(
						x.split('.txt')[0] for x in email['source_files']
					)) for email in reduced_emails
			}

		for email in reduced_emails:
			deduped_emails[email['dup_key']] = email
			parent_child_mappings[email.get('reply_key', 'ROOT')].add(email['dup_key'])
			child_parent_mappings[email['dup_key']] = email.get('reply_key', 'ROOT')

	task_info['n_output_dup_keys'] = len(deduped_emails)
	task_info['n_split_keys'] = len(split_dup_keys)

	pprint.pprint(task_info)

	with open(os.path.join(outdir, 'content', '%02d.json' % bin_idx), 'w') as f:
		json.dump(deduped_emails, f)
	with open(os.path.join(outdir, 'tree', '%02d.up.json' % bin_idx), 'w') as f:
		json.dump(child_parent_mappings, f)
	with open(os.path.join(outdir, 'tree', '%02d.down.json' % bin_idx), 'w') as f:
		json.dump({k: list(v) for k, v in parent_child_mappings.items()}, f)
	with open(os.path.join(outdir, 'split_keys', '%02d.json' % bin_idx), 'w') as f:
		json.dump(split_dup_keys, f)

	with open(os.path.join(outdir, 'info', '%02d.json' % bin_idx), 'w') as f:
		json.dump(task_info, f)

if __name__ == '__main__':
	root_indir = os.path.join(OUT_DIR, 'map_extract_emails')
	root_outdir = os.path.join(OUT_DIR, 'reduce_dedup_extracted')
	
	args = [(bin_idx, root_indir, root_outdir) for bin_idx in range(N_BINS)]
	pool = Pool(8)

	pool.starmap(dedup_extracted_emails, args)