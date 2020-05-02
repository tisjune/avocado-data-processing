import sys
import os
import json
import pprint
import zlib
from collections import defaultdict
from settings import N_BINS, N_THREAD_BINS, OUT_DIR
from multiprocessing import Pool

def get_bin(s, n_bins=N_THREAD_BINS):
	return zlib.adler32(s.encode('utf-8')) % n_bins 

def reshuffle_by_thread_bin(bin_idx, node_root_file, indir, outdir):
	print(bin_idx)
	with open(node_root_file) as f:
		node_to_root = json.load(f)
	
	bin_to_emails = defaultdict(dict)

	with open(os.path.join(indir, '%02d.json' % bin_idx)) as f:
		email_dict = json.load(f)
		for key, email in email_dict.items():
			if key not in node_to_root: continue
			email['thread_root_key'], email['thread_level'] = node_to_root[key]
			bin_to_emails[get_bin(email['thread_root_key'])][key] = email 

	for thread_bin_idx, email_dict in bin_to_emails.items():
		print(bin_idx, thread_bin_idx, len(email_dict))
		outfile = os.path.join(outdir, '%02d' % thread_bin_idx, '%02d.json' % bin_idx)
		with open(outfile, 'w') as f:
			json.dump(email_dict, f)

if __name__ == '__main__':
	root_indir = os.path.join(OUT_DIR, 'reduce_dedup_extracted/content')
	root_outdir = os.path.join(OUT_DIR, 'reduce_dedup_thread/content')
	for thread_bin_idx in range(N_THREAD_BINS):
		print(thread_bin_idx)
		try:
			os.mkdir(os.path.join(root_outdir, '%02d' % thread_bin_idx))
		except Exception as e:
			print(e)

	node_root_file = os.path.join(OUT_DIR, 'reduce_dedup_thread/node_to_root.json')
	
	pool = Pool(4)
	pool.starmap(reshuffle_by_thread_bin,
		[(bin_idx, node_root_file, root_indir, root_outdir) for bin_idx in range(N_BINS)])