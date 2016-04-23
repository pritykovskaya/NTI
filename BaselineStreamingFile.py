# coding=utf-8
import csv
import gzip
import os
import operator
from collections import defaultdict
import numpy as np

def read_revesed_graph_list():
    print("Start to read in memory reversed graph")
    users_with_common_friends = [list() for _ in range(1, USERS_COUNT_REVERSED_GRAPH + 2)]

    with open(reversed_graph_path) as csvinput:
        csv_reader = csv.reader(csvinput, delimiter=',')
        for line in csv_reader:
            user = int(line[0])
            friends = map(int, line[1:])
            users_with_common_friends[user] = friends

    print("Finish to read in memory reversed graph")
    return users_with_common_friends


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


def create_prediction(i):
    user_friends = read_common_friends_part(i)
    gzip_csv_output = gzip.open(prediction_path, "w")
    csv_writer = csv.writer(gzip_csv_output, delimiter=',')
    for user, friends_counts in user_friends.iteritems():
        sorted_friends_counts = sorted(friends_counts.items(), key=operator.itemgetter(1), reverse=True)
        csv_writer.writerow([user] + map(lambda x: x[0], sorted_friends_counts[:20]))
    gzip_csv_output.close()


def read_common_friends_part(i):
    user_friends = defaultdict(lambda: defaultdict(int))

    with open(common_friends_count + "_" + str(i)) as csvinput:
        csv_reader = csv.reader(csvinput, delimiter=',')
        for line in csv_reader:
            user = int(line[0])
            if user % 11 == 7:
                for pair in line[1:]:
                    friend = int(pair.split(":")[0])
                    count = int(pair.split(":")[1])
                    user_friends[user][friend] = count

    return user_friends

if __name__ == "__main__":

    data_home = "/Users/n.pritykovskaya/AIST/Task3/"
    graph_path = os.path.join(data_home, "trainGraph")
    reversed_graph_path = os.path.join(data_home, "trainGraphReversed")
    common_friends_count = os.path.join(data_home, "common_friends_count")
    prediction_path = os.path.join(data_home, "prediction")

    USERS_COUNT_GRAPH = 9417921  # 107182
    USERS_COUNT_REVERSED_GRAPH = 9418031  # 8398377

    reversed_graph = read_revesed_graph_list()
    count_common_friends_part(1)


