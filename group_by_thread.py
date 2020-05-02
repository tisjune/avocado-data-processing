import sys
import os
import json
import pprint
from collections import defaultdict
from settings import N_BINS, N_THREAD_BINS, OUT_DIR



def group_by_thread(indir, outdir):
	
	child_parent_mappings = {}
	parent_child_mappings = defaultdict(set)
	for file in os.listdir(os.path.join(indir, 'tree')):
		print(file)
		with open(os.path.join(indir, 'tree', file)) as f:
			edge_dict = json.load(f)
		if file.endswith('up.json'):
			for k, v in edge_dict.items(): child_parent_mappings[k] = v
		elif file.endswith('down.json'):
			for k, v in edge_dict.items(): parent_child_mappings[k].update(v)
	
	thread_sets = defaultdict(set)
	node_to_root = {}
	to_traverse = set([])
	to_traverse.update(parent_child_mappings['ROOT'])
	while len(to_traverse) > 0:
		curr_node = to_traverse.pop()
		parent = child_parent_mappings[curr_node]
		if parent == 'ROOT':
			level = 0
			node_to_root[curr_node] = (curr_node, 0)
		else:
			root, level = node_to_root[parent]
			level += 1
			node_to_root[curr_node] = (root, level)
		thread_sets[node_to_root[curr_node][0]].add((curr_node, level))
		to_traverse.update(parent_child_mappings[curr_node])

	print(len(thread_sets), 'thread_sets;', len(child_parent_mappings), len(node_to_root), 'nodes')
	with open(os.path.join(outdir, 'node_to_root.json'), 'w') as f:
		json.dump(node_to_root, f)
	

if __name__ == '__main__':
	group_by_thread(
			os.path.join(OUT_DIR, 'reduce_dedup_extracted'),
			os.path.join(OUT_DIR, 'reduce_dedup_thread')
		)

