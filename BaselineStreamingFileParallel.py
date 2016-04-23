import csv
import gzip
import os
import operator
from collections import defaultdict
import multiprocessing
from multiprocessing import Manager
import numpy as np

def read_revesed_graph_in_shared_memory():
    print("Start to read in memory reversed graph")
    manager_list = [list() for _ in range(1, USERS_COUNT_REVERSED_GRAPH + 2)]

    with open(reversed_graph_path) as csvinput:
        csv_reader = csv.reader(csvinput, delimiter=',')
        for line in csv_reader:
            user = int(line[0])
            friends = np.array(map(int, line[1:]), dtype=np.int32)
            manager_list[user] = friends

    print("Finish to read in memory reversed graph")
    return manager_list

def count_common_friends_part(i):
    print('Working on part {0}'.format(i))

    if i > 9:
        csvinput = open(graph_path + '/part-v024-o000-r-000' + str(i))
    else:
        csvinput = open(graph_path + '/part-v024-o000-r-0000' + str(i))

    csv_reader = csv.reader(csvinput, delimiter='\t')

    with open(common_friends_count + "_" + str(i), "w") as csvoutput:
        csv_writer = csv.writer(csvoutput, delimiter=',')

        for line in csv_reader:
            user = int(line[0])
            friends_with_masks = line[1].replace("{(", "").replace(")}", "").split("),(")
            friends = np.array([int(friends_with_mask.split(",")[0]) for friends_with_mask in friends_with_masks], dtype=np.int32)
            user_friend_counter = defaultdict(int)

            for friend in friends:
                friend_friends = reversed_graph[friend]
                for friend_friend in friend_friends:
                    user_friend_counter[friend_friend] += 1

            csv_writer.writerow([user] + ['{0}:{1}'.format(friend, counter) for friend, counter in user_friend_counter.iteritems()])
            user_friend_counter.clear()

    csvinput.close()

if __name__ == "__main__":

    data_home = "/Users/n.pritykovskaya/AIST/Task3/"
    graph_path = os.path.join(data_home, "trainGraph")
    reversed_graph_path = os.path.join(data_home, "trainGraphReversed")
    common_friends_count = os.path.join(data_home, "common_friends_count")
    prediction_path = os.path.join(data_home, "prediction")

    USERS_COUNT_GRAPH = 9417921  # 107182
    USERS_COUNT_REVERSED_GRAPH = 9418031  # 8398377

    # print("Start to create shared list")

    manager = Manager()
    reversed_graph = manager.list()
    reversed_graph = read_revesed_graph_in_shared_memory()
    print("Finish to create shared list")

    print("Start to run parallel")
    pool = multiprocessing.Pool(processes=2)
    result = pool.map(count_common_friends_part, range(11))
    print("Stop to run parallel")
