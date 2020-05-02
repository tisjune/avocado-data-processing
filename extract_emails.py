'''
	NOTE/TODO: this "works" if run in pacific time (or whatever timezone most of the emails are generated). maybe it's possible to do it better and per-custodian timezones.
'''

import sys
import os
import json
import pprint
import zlib
from collections import defaultdict
from extraction_utils import parse_email_file
from settings import N_BINS, RAW_DIR, OUT_DIR
from multiprocessing import Pool



def get_bin(s, n_bins=N_BINS):
	return zlib.adler32(s.encode('utf-8')) % n_bins 

def extract_emails(chunk_id, custodians, indir, outdir, n_bins=N_BINS, verbose=5000):

	print(chunk_id)
	task_info = {'chunk_id': chunk_id, 'custodians': custodians,
		'n_files': 0, 'n_extracted': 0,
		'n_errors': 0, 'n_unique': 0}

	email_files = []
	for custodian in custodians:
		custodian_dir = os.path.join(indir, '%03d/%03d' % (custodian,custodian))

		email_files += [os.path.join(custodian_dir, x) 
			for x in os.listdir(custodian_dir) if x.endswith('EM.txt')]

	task_info['n_files'] = len(email_files)
	print(chunk_id, '\t', 'extracting %d files from: ' % len(email_files),
		 ' & '.join(str(x) for x in sorted(custodians)))

	parsed_emails = {bin_idx: defaultdict(list)
			for bin_idx in range(n_bins)
		} # bin --> dup key --> [emails]
	error_emails = [] # [emails]

	for f_idx, file in enumerate(email_files):
		if (f_idx > 0) and (f_idx % verbose == 0):
			print(chunk_id, f_idx, '/', len(email_files))
		try:
			curr_parsed_emails = parse_email_file(file)
			for email in curr_parsed_emails:
				if len(email['errors']) == 0:
					task_info['n_extracted'] += 1
					parsed_emails[get_bin(email['dup_key'], n_bins)][email['dup_key']].append(email)
				else:
					error_emails.append(email)
		except Exception as e:
			print(chunk_id, file, e)
			error_emails.append({'file': file, 'errors': [('ERR: ' + str(e))]})

	task_info['n_errors'] = len(error_emails)
	task_info['n_unique'] = sum(len(x) for x in parsed_emails.values())
	
	pprint.pprint(task_info)

	for bin_idx, bin_emails in parsed_emails.items():
		outfile = os.path.join(outdir, '%02d' % bin_idx, '%s.json' % chunk_id)
		with open(outfile, 'w') as f:
			json.dump(bin_emails, f)
	with open(os.path.join(outdir, 'log', '%s.errors.json' % chunk_id), 'w') as f:
		json.dump(error_emails, f)
	with open(os.path.join(outdir, 'log', '%s.info.json' % chunk_id), 'w') as f:
		json.dump(task_info, f)
	print(chunk_id, 'done')


if __name__ == '__main__':
	root_outdir = os.path.join(OUT_DIR, 'map_extract_emails')
	for bin_idx in range(N_BINS):
		print(bin_idx)
		try: 
			os.mkdir(os.path.join(root_outdir, '%02d' % bin_idx))
		except Exception as e:
			print(e)
	try:
		os.mkdir(os.path.join(root_outdir, 'log'))
	except Exception as e:
		print(e)

	chunks = {int(c_id): [int(c_id)] for c_id in os.listdir(RAW_DIR)}
	args = [(k, v, RAW_DIR, root_outdir) for k, v in chunks.items()]
	pool = Pool(4)
	pool.starmap(extract_emails, args)


