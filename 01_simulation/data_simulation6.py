import random
import json
import time

def set_n_researchers():
    global n_researchs
    n_researchers = int(10e6)
    return n_researchers
    
def random_string(length):
    alpha = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIGKLMNOPQRSTUVWXYZ'
    id = ''
    for i in range(0,length,2):
        id += random.choice(alpha)
    return id

def random_id(length):
    number = '0123456789'
    id = ''
    for i in range(0,length,2):
        id += random.choice(number)
    return id

def json_generator(record_id, title, reference_list, author, co_author,year, freekeywords=[],standardizedkeywords=[], citation_list=[]):
#    {"free_keywords": [], \
#     "standardized_keywords": [],\
#     "citations": [452267, 275304, 524107, 76215, 311629, 366639, 846024, 151315, 838517, 139190, 81651, 128250], \
#     "recid": 1, \
#     "title": "Isoclinic N planes in Euclidean 2N space, Clifford parallels in elliptic (2N-1) space, and the Hurwitz matrix equations", \
#     "references": [1256272, 51394], 
#     "abstract": "", \
#     "authors": ["Wong, Yung-Chow"], 
#     "creation_date": "1961", 
#     "co-authors": []\
#     }
        schema = {'free_keywords' : freekeywords, \
              'standardized_keywords': standardizedkeywords, \
              'citations': citation_list, \
              'recid': record_id, \
              'title': title, \
              'references': reference_list, \
              'abstract': '', \
              'authors': author, \
              'creation_date': year,\
              'co-authors': co_author\
             }
        json_str = json.dumps(schema)
    
        return json_str

def reference_generator(recid, n_ref):
    reference_list = []
    for j in range(n_ref):
        ref_temp = random.randint(1, recid-1)
        reference_list.append(ref_temp)
    reference_list = list(set(reference_list))
    return reference_list


def coauthor_generator(recid, n_coauthor, author):
    coauthor_list = []
    for j in range(n_coauthor):
        coauthor_temp = str(random.randint(1, int(10e6)))
        coauthor_list.append(coauthor_temp)
    coauthor_list = list(set(coauthor_list))
    if author in coauthor_list:
        coauthor_list = coauthor_list.remove(author)
    return coauthor_list

def year_generator():
    return str(random.randint(1961, 2015))

n_researchs = int(10e6)
del_n_rec = int(5e6)

for j in range(6, 10):
	n_cycle = j

	range_low = (n_cycle) * del_n_rec + 1
	range_high = (n_cycle + 1) * del_n_rec
	print "part %d: %d -- %d" %(j, range_low, range_high)

	if n_cycle == 1:
	    range_low = 141879

	start_time = time.time()
	print "start generating data, please wait ..."



	for i in range(range_low, range_high):
    		record_id = i + range_low
    		author = str(random.randint(1, int(10e6)))
    
    		title_length = random.randint(1, 100)
    		title = random_string(title_length)
    
    		n_reference = random.randint(1, 30)
    		reference_list = reference_generator(record_id, n_reference)
    
    		n_coauthor = random.randint(1, 50)
    		co_author_list = coauthor_generator(record_id, n_coauthor, author)
    
    		year = year_generator()

    		js_fake = json_generator(record_id, title, reference_list, author, co_author_list, year)
#    print reference_list, co_author_list
#    print js_fake
		file_out = 'fake_data_p' + str(j) + '.json'
    		with open(file_out,'a') as f:
			f.write(js_fake + '\n')

		if i in range(range_low, range_high, int(1e6)):
			print "%d records have been generated" %i
	
	end_time = time.time()
	delta = end_time - start_time
	print "part %d: %d have been generated, time used: %f second" %(j, i, delta)

