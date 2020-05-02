'''
	NOTE/TODO: this "works" if run in pacific time (or whatever timezone most of the emails are generated). maybe it's possible to do it better and per-custodian timezones.
'''

import sys
import os

import email as em
from email import parser as em_parser
import calendar
from datetime import datetime
import dateparser

import re
from collections import defaultdict

SUBJ_PREFIX_RE = re.compile(r'([\[\(] *)?\b(RE|AW|WG|FWD?) *([-:;)\]][ :;\])-]*|$)|\]+ *$',
								flags=re.I)
QUOTE_RE = re.compile(r'(^|\n)(\s*[>\*] *)+')
HEADER_RE = re.compile(r'^[A-Z][\w\-]+\:')

## Email parsing


def parse_subject(subject, subj_re=SUBJ_PREFIX_RE):
	# extracts prefixes from email subjects
	if subject is None:
		return '', None
	else:
		all_prefs = subj_re.findall(subject)
		if len(all_prefs) > 0:
			last_action = all_prefs[0][1][:2].lower()
			if last_action == 'aw': last_action = 're'
			if last_action == 'wg': last_action = 'fw'
			clean_subj = subj_re.sub('', subject)
		else:
			last_action = None
			clean_subj = subject
		return clean_subj.strip(), last_action

def get_dup_key(email_info, use_date=True):
	# outputs the information used to deduplicate emails in the first pass: subject line, sender, timestamp
	dup_key = []
	dup_key.append(email_info['raw_subject'])
	if email_info['from'][0] != '':
		dup_key.append(email_info['from'][0])
	else:
		dup_key.append(email_info['from'][1])
	if use_date:
		if email_info['date'] is not None:
			dup_key.append(email_info['date'])
		else:
			dup_key.append(email_info['raw_date'])
	dup_key.append('; '.join([x if x is not None else '' for x, _ in email_info['to']]))
	dup_key.append('; '.join([x if x is not None else '' for x, _ in email_info['cc']]))
	return '__'.join(x.replace('>','').strip() if x is not None else '' for x in dup_key)

def get_nearest_second(ts):
	return datetime.fromtimestamp(60*round(ts/60)).replace(second=0)

def parse_date(datestr, is_top):
	# processes a diversity of ways that time is listed in the emails
	if datestr is None: return None
	datestr = datestr.strip()
	try:
		if is_top:
			parsed = em.utils.parsedate_tz(datestr)
			ts = calendar.timegm(parsed) - parsed[9]
			tup = get_nearest_second(ts)
		else:

			if datestr[-1].lower() == 'm':           
				if datestr[0].isalpha():
					tup = datetime.strptime(datestr, '%A, %B %d, %Y %I:%M %p')
				else:
					
					tup = datetime.strptime(datestr, '%m/%d/%y %I:%M %p')
			else:
				tup = datetime.strptime(datestr, '%a %b %d %H:%M:%S %Y')
				ts = 60*round(tup.timestamp()/60)
				tup = get_nearest_second(tup.timestamp())
		return em.utils.format_datetime(tup)
	except Exception as e: 
		try:
			tup = dateparser.parse(datestr)
			tup = get_nearest_second(tup.timestamp())
			return em.utils.format_datetime(tup)
		except:
			return None

def get_content(email_parse):
	for part in email_parse.walk():
		if part.get_content_type() == 'text/plain':
			return part.get_payload().strip()
	return ''

def clean_name(name, email):
	# code to process a variety of formats in which senders and addressees are listed.
	if (email is None) and (('@' in name) or ('.' in name)):
		name, email = em.utils.parseaddr(name)
	name = name.replace('(E-mail)','').replace('\'','')
	if email is not None:
		email = email.replace('\'','')
	proc_email = email

	if '[ mailto' in name:
		name = name.replace('[ mailto', '[mailto')
	if '[mailto' in name:
		name, proc_email = name.split('[mailto:')
		name = name.strip()
		proc_email = proc_email.replace('mailto:', '').replace(']','').strip()
	elif '@' in name:
		name, proc_email = None, name.strip()
	else:
		name = name.split('-')[0].strip()
	if name is not None:
		if '(' in name: 
			name = name.split('(')[0]
		if '[' in name:
			name = name.split('[')[0]
	if (email == '') or (email is None):
		email = proc_email
	if name is None:
		name = ''
	if email is None:
		email = ''
	return name.title().strip().replace('"',''), email.lower().strip()

def prep_email_text(email_text):
	# removes extra quotes from email text, since '>' will precede lines in many replies.
	if ('\n>' in email_text) or ('\n*' in email_text) \
		or ('>>' in email_text) or ('\t>' in email_text) \
		or email_text.startswith('>'):
		after_header_block = False
		lines = email_text.split('\n')
		prepped_lines = ''
		for i, line in enumerate(lines):
			no_quotes = QUOTE_RE.sub('', line).strip()
			if (HEADER_RE.match(no_quotes) is not None) and (not after_header_block):
				prepped_lines += '\n' + no_quotes 
			elif no_quotes=='':
				after_header_block = True
				prepped_lines += '\n'
			else:
				if after_header_block:
					prepped_lines += '\n' + no_quotes
				else:
					prepped_lines += ' '  + no_quotes
		return prepped_lines.strip()
	else:
		return email_text.strip()

def parse_email(email_text, is_top): 
	# wrapper to parse email header and content
	email_info = {'is_top': is_top}
	
	email_text = prep_email_text(email_text)
	parsed_items = em_parser.Parser().parsestr(email_text)
	email_info['is_smtp'] = 'Received' in parsed_items
	if 'From' in parsed_items:
		if is_top:
		
			name, email = em.utils.parseaddr(parsed_items['From'])
			email_info['from'] = clean_name(name, email)
		
		else:

			email_info['from'] = clean_name(parsed_items['From'], None)
	else:
			email_info['from'] = ('', '')
	
	for field in ['To', 'Cc', 'Bcc']:
		field_key = field.lower()
		email_info[field_key] = []
		if field in parsed_items:
			if is_top:
				for name, email in em.utils.getaddresses([parsed_items[field]]):
					email_info[field_key].append((clean_name(name, email)))
			else:
				email_info[field_key] = [clean_name(name, None) 
							for name in parsed_items[field].split(';')]
	email_info['raw_subject'] = parsed_items['Subject']
	email_info['subject'], email_info['last_action'] = \
		parse_subject(parsed_items['Subject'], SUBJ_PREFIX_RE)
	if 'Date' in parsed_items:
		email_info['date'] = parse_date(parsed_items['Date'], is_top)
		email_info['raw_date'] = parsed_items['Date']
	else:
		email_info['date'] = parse_date(parsed_items['Sent'], is_top)
		email_info['raw_date'] = parsed_items['Sent']
	email_info['content'] = get_content(parsed_items)
	email_info['dup_key'] = get_dup_key(email_info)
	email_info['subj_dup_key'] = get_dup_key(email_info, use_date=False)
	return email_info

def get_parse_errors(entry):
	errors = []
	if entry['date'] is None: errors.append('date')
	if entry['from'] == ('', ''): errors.append('from')
	if (entry.get('content','').strip() == '') and (entry['subject'].strip() == ''): errors.append('content')
	return errors

def get_in_file_id(filename, idx):
	return '%s.%02d' % (filename, idx)

def parse_email_file(full_filename):
	# extracts individual emails from .txt files, separated by "original message" blocks.
	with open(full_filename, encoding='utf-8') as f:
		email_text = f.read()
	filename = os.path.basename(full_filename)
	is_smtp = False

	split_emails = email_text.split('-----Original Message-----')
	original_length = len(split_emails)
	split_emails = [x for x in split_emails if x.strip() != '']
	parsed_emails = []
	file_is_clean = len(split_emails) == original_length
	for idx, email in enumerate(split_emails):
		parsed = parse_email(email, idx==0)
		curr_errors = get_parse_errors(parsed)
		if len(curr_errors) > 0:
			file_is_clean = False
		parsed['errors'] = curr_errors
		if (idx == 0) and parsed['is_smtp']: 
			is_smtp = True
		parsed['is_smtp'] = is_smtp   
		parsed['file'] = filename
		parsed['in_file_idx'] = idx
		parsed['custodian'] = int(filename.split('-')[0])
		parsed_emails.append(parsed)
	for idx in range(len(parsed_emails)):
		if idx + 1 < len(parsed_emails):
			parsed_emails[idx]['reply_key'] = parsed_emails[idx+1]['dup_key']
		if not file_is_clean:
			parsed_emails[idx]['errors'].append('file')
		parsed_emails[idx]['root_key'] = parsed_emails[-1]['dup_key']
		parsed_emails[idx]['root_subject'] = parsed_emails[-1]['subject']
	return parsed_emails

def reduce_dup_key(dup_set):
	# coalesces a set of duplicated emails into a single email. the rough heuristic here is that i favoured keeping the copy of the email which was closest to the top of the .txt file; in particular, if an email was already at the top of a .txt file (and thus already had an XML entry of its own) then it was likely that the email would be formatted more consistently.
	reply_sets = defaultdict(list)
	for entry in dup_set:
		reply_sets[entry.get('reply_key',None)].append(entry)
	for k, v in reply_sets.items():
		reply_sets[k] = sorted(v, key=lambda x: 
			(x['in_file_idx'], x['is_smtp'], -len(x['content'])))
	if len(reply_sets) == 1:
		canonical_sets = list(reply_sets.values())
	else:
		reply_sets = {k: v for k, v in reply_sets.items() if k is not None}
		if len(reply_sets) == 1:
			canonical_sets = list(reply_sets.values())
		else:
			canonical_sets = []
			sorted_reply_sets = sorted(reply_sets.items(), key=lambda x: 
						(x[1][0]['in_file_idx'], -len(x[1]), x[1][0]['is_smtp']))
			for k, v in sorted_reply_sets:
				if v[0]['in_file_idx'] > 0: break
				canonical_sets.append(v)
			if len(canonical_sets) == 0:
				canonical_sets.append(sorted_reply_sets[0][1])
	canonical_emails = []
	for idx, email_set in enumerate(canonical_sets):
		# canon_email = {k:v for k, v in email_set[0].items()}
		canon_email = email_set[0]
		if len(canonical_sets) > 1:
			canon_email['dup_key'] = '%s.%d' % (canon_email['dup_key'], idx)
			canon_email['source_files'] = ['%s.%02d' % (entry['file'], entry['in_file_idx']) for entry in email_set]
		else:
			canon_email['source_files'] = ['%s.%02d' % (entry['file'], entry['in_file_idx']) for entry in dup_set]

		canonical_emails.append(canon_email)
	return canonical_emails

def dedup_thread(thread_set, email_dict, content_chars=100):
	# deduplicates emails which are rooted in the same parent. 
    threads_by_depth = defaultdict(list)
    for key, depth in thread_set:
        threads_by_depth[depth].append(key)
    threads_by_depth = sorted(threads_by_depth.items(),key=lambda x: x[0])
    key_to_dedup = {}
    for depth, keys in threads_by_depth:
        for key in keys:
            if 'reply_key' in email_dict[key]:
                email_dict[key]['dedup_reply_key'] = key_to_dedup.get(
                        email_dict[key]['reply_key'],
                        email_dict[key]['reply_key']
                    )
        if len(keys) == 1: continue
        dup_sets = defaultdict(list)
        for key in keys:
            email = email_dict[key]
            dup_sets[(email['dedup_reply_key'], email['subj_dup_key'].strip(), email['content'].strip()[:content_chars])].append(email)
        for (reply_key, subj, content), dup_set in dup_sets.items():
            if len(dup_set) == 1: continue
            canon_email = None
            sorted_by_time = sorted(dup_set, key=lambda x: em.utils.parsedate_tz(x['date']))
            prev_time = em.utils.parsedate_tz(email_dict[reply_key]['date'])
            for email in sorted_by_time:
                if em.utils.parsedate_tz(email['date']) > prev_time:
                    canon_email = email
                    break
            if canon_email is None:
                canon_email = sorted_by_time[0]
            for email in sorted_by_time:
                key_to_dedup[email['dup_key']] = canon_email['dup_key']
    thread_set = [(k, v) for k, v in thread_set if key_to_dedup.get(k,k) == k]
    return thread_set