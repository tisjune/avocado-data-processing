import os
import zipfile
from multiprocessing import Pool

def unzip_files(source_file, dest_dir):

	zip_obj = zipfile.ZipFile(source_file, 'r')
	zip_obj.extractall(dest_dir)
	zip_obj.close()

def unzip_wrapper(cust_id):
	print(cust_id)
	source_file = os.path.join('F:/zipped_text/%03d.zip' % cust_id)
	dest_dir = os.path.join('F:/raw_text/%03d' % cust_id)
	unzip_files(source_file, dest_dir)

if __name__ == '__main__':
	pool = Pool(8)
	pool.map(unzip_wrapper, [int(x.replace('.zip','')) for x in os.listdir('F:/zipped_text')])