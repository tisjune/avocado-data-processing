import os, json
import spacy
from settings import OUT_DIR, PROC_DIR
from multiprocessing import Pool

def load_spacy():
	return spacy.load('en_core_web_sm')

def process_token(token, offset, span):
    tok_info = {'tok_': token.text,
               'tag_': token.tag_,
                'pos': token.pos_,
               'dep': token.dep_}
    if tok_info['dep'] != 'ROOT':
        tok_info['up'] = next(token.ancestors).i - offset
    tok_info['dn'] = [x.i-offset for x in token.children]
    if token.tag_ == 'ADD':
        tok_info['tok'] = 'ADD'
        tok_info['tag'] = 'X'
    elif token.pos_ == 'SYM':
        tok_info['tok'] = '$'
        tok_info['tag'] = 'X'
    elif token.pos_ == 'SPACE':
        tok_info['tok'] = ' '
        tok_info['tag'] = 'SP'
    elif token.pos_ == 'NUM':
        if token.text.isalpha():
            tok_info['tok'] = token.text
            tok_info['tag'] = 'NUM'
        else:
            tok_info['tok'] = '#'
            tok_info['tag'] = 'CD'
    elif token.pos_ == 'PUNCT':
        if token.tag_ == 'HYPH':
            tok_info['tok'] = '-'
            tok_info['tag'] = 'PUNCT'
        elif token.tag_ in ("''",'""','``'):
            tok_info['tok'] = "'"
            tok_info['tag'] = 'PUNCT'
        elif token.tag_ == '-LRB-':
            tok_info['tok'] = '('
            tok_info['tag'] = 'PUNCT'
        elif token.tag_ == '-RRB-':
            tok_info['tok'] = ')'
            tok_info['tag'] = 'PUNCT'

        elif tok_info['tag_'] == 'NFP':
            tok_info['tok'] = '$'
            tok_info['tag'] = 'X'
        else:
            tok_info['tok'] = token.text
            tok_info['tag'] = 'PUNCT'
    elif tok_info['tag_'] in ('VBD', 'VBN', 'VB', 'VBG', 'VBP', 'VBZ'):
        tok_info['tok'] = token.text
        if tok_info['tag_'] in ('VBD', 'VBN'):
            tok_info['tag'] = 'VB0'
        else:
            dns = set(span[i].text.lower() for i in tok_info['dn'])
            if sum(x in dns for x in ('will', '\'ll', 'shall', 'wo')) > 0:
                tok_info['tag'] = 'VB2'
            elif sum(x in dns for x in ('are','is','am','are')) > 0: 
                tok_info['tag'] = 'VB1'
            elif sum(x in dns for x in ('was', 'were')) > 0:
                tok_info['tag'] = 'VB0'
            else: tok_info['tag'] = 'VB1'

    else:
        tok_info['tok'] = token.text
        tok_info['tag'] = token.tag_
    return tok_info

def is_usable_sent(toks):
		
	actual_text = sum(x['tag'] not in ['X','CD','SP','PUNCT', 'NNP', 'NNPS'] for x in toks)
	if (sum(x['tag'] in ['X','CD'] for x in toks) > 0) or (toks[0]['tag'].startswith('NNP')):
		return (actual_text >= 3) and (len(toks) - actual_text < actual_text)
	else:
		return actual_text > 0

def process_sentence(sent_obj, offset):
    tokens = [process_token(tok, offset, sent_obj) for tok in sent_obj]
    is_usable = is_usable_sent(tokens)
    ent_labels = [[x.label_, len(x)] for x in sent_obj.ents]
    return {'rt': sent_obj.root.i-offset, 'toks': tokens, 'use': is_usable, 'ents': ent_labels}

def process_text(text, nlp):
	obj = nlp(text[:nlp.max_length-1])
	sents = []
	offset = 0
	for sent in obj.sents:
		curr_sent = process_sentence(sent, offset)
		sents.append(curr_sent)
		offset += len(curr_sent['toks'])
	return sents

def get_clean_text(sents, use_all=False, sent_joiner=' <s> '):
	clean_sents = []
	for sent in sents:
		if use_all or sent['use']:
			clean_sents.append(' '.join(x['tok'] for x in sent['toks']
									   if x['tag'] not in ('SP')))
	return sent_joiner.join(clean_sents)

def is_usable(tok):
	text = tok['tok']
	tag = tok['tag']
	if tag.startswith('NNP'): return False
	return text.isalpha() or (text == '#') or ((len(text) > 1) and (text[1:].isalpha()))

def get_arcs_at_root(root, sent, use_start=True):
	arcs = set()
	root_tok = root['tok'].lower()
	if not is_usable(root): return arcs
	arcs.add( root['tok'].lower() + '_*')
	next_elems = []
	for kid_idx in root['dn']:
		kid = sent['toks'][kid_idx]
		if kid['dep'] in ['cc']: continue
		if is_usable(kid):
			if kid['dep'] != 'conj':
				arcs.add( root['tok'].lower() + '_' + kid['tok'].lower())
			next_elems.append(kid)
	if use_start:
		first_elem = sent['toks'][0]
		if is_usable(first_elem):
			if (1 not in first_elem['dn']) and (len(sent['toks']) == 2):
				second_elem = sent['toks'][1]
				if 0 not in second_elem['dn']:
					if is_usable(second_elem): arcs.add(first_elem['tok'].lower() + '>'
															  + second_elem['tok'].lower())
	for next_elem in next_elems:
		arcs.update(get_arcs_at_root(next_elem, sent, False))
	return arcs

def get_arcs_wrapper(sent):
	return ' '.join(get_arcs_at_root(sent['toks'][sent['rt']], sent))

def process_email(email, nlp):
	proc_entry = {'id': email['id'], 'date': email['date'], 'last_action': email['last_action']}
	subject = process_text(email['subject'], nlp)
	content = process_text(email['content'], nlp)
	proc_entry['subject'] = get_clean_text(subject, True, ' ')
	proc_entry['content'] =  get_clean_text(content)
	full_entry = {'id': email['id'], 'subject': subject, 'content': content}
	arcs = {'id': email['id'], 'subject': ' '.join(get_arcs_wrapper(x) for x in subject),
		   'content': ' <s> '.join(get_arcs_wrapper(x) for x in content if x['use'])}
	return proc_entry, full_entry, arcs

def process_email_file(idx):
	print(idx)
	with open(os.path.join(OUT_DIR, 'email_threads/content/%02d.json' % idx)) as f:
		email_dict = json.load(f)

	nlp = load_spacy()
	proc_text = []
	parses = []
	arcs = []
	for i, email in enumerate(email_dict.values()):
		if (i > 0) and (i % 500 == 0):
			print(idx, i, len(email_dict))
			# break
		curr_text, curr_parse, curr_arc = process_email(email, nlp)
		proc_text.append(curr_text)
		parses.append(curr_parse)
		arcs.append(curr_arc)
	with open(os.path.join(PROC_DIR, 'content/%02d.text.json'% idx), 'w') as f:
		json.dump(proc_text, f)
	with open(os.path.join(PROC_DIR, 'content/%02d.parse.json'% idx), 'w') as f:
		json.dump(parses, f)
	with open(os.path.join(PROC_DIR, 'content/%02d.arcs.json'% idx), 'w') as f:
		json.dump(arcs, f)

if __name__ == '__main__':
	# process_email_file(0)
	pool = Pool(4)
	pool.map(process_email_file, range(10))