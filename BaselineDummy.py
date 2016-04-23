import csv
import gzip
import os
from collections import defaultdict
import sys


def read_graph():
    print("Start to read in memory graph")
    files_list = [f for f in os.listdir(graph_path) if f.startswith("part")]

    user_friends = {}
    for file in files_list:
        csvinput = gzip.open(os.path.join(graph_path, file))
        csv_reader = csv.reader(csvinput, delimiter='\t')
        for line in csv_reader:
            user = line[0]
            friends_with_masks = line[1].replace("{(", "").replace(")}", "").split("),(")
            user_friends[user] = [friends_with_mask.split(",")[0] for friends_with_mask in friends_with_masks]
        csvinput.close()

    print("Finish to read in memory graph")
    return user_friends


def create_reversed_graph():
    print("Start to create reversed graph")
    user_friends = read_graph()

    user_friends_reversed = defaultdict(list)
    for user, friends in user_friends.iteritems():
        for friend in friends:
            user_friends_reversed[friend].append(user)

    with open(reversed_graph_path, "w") as csvoutput:
        csv_writer = csv.writer(csvoutput, delimiter=',')
        for user, friends in user_friends_reversed.iteritems():
            csv_writer.writerow([user] + friends)

    print("Finish to create reversed graph")
    return len(user_friends_reversed.keys())


def read_revesed_graph():
    print("Start to read in memory reversed graph")
    users_with_common_friends = defaultdict(list)

    with open(reversed_graph_path) as csvinput:
        csv_reader = csv.reader(csvinput, delimiter=',')
        for line in csv_reader:
            user = line[0]
            friends = line[1:]
            users_with_common_friends[user] = friends

    print("Finish to read in memory reversed graph")
    return users_with_common_friends


def create_common_friends_counts():
    print("Start to create common friends counts")

    user_friends_reversed = read_revesed_graph()
    user_friend_counter = defaultdict(int)

    counter = 0
    for user, friends in user_friends_reversed.iteritems():
        counter += 1
        for user1 in friends:
            for user2 in friends:
                if user1 != user2:
                    user_friend_counter[(user1, user2)] += 1
        if counter % 10000 == 0:
            print counter
            print sys.getsizeof(user_friend_counter) / 2.0**30

    with open(common_friends_count_path, "w") as csvoutput:
        csv_writer = csv.writer(csvoutput, delimiter=',')
        for couple, counter in user_friend_counter.iteritems():
            csv_writer.writerow(list(couple) + [counter])


if __name__ == "__main__":

    data_home = "/Users/n.pritykovskaya/AIST/Task3"
    graph_path = os.path.join(data_home, "trainGraph")
    reversed_graph_path = os.path.join(data_home, "trainGraphReversed")
    common_friends_count_path = os.path.join(data_home, "commonFriendsCount")

    if not os.path.exists(reversed_graph_path):
        print create_reversed_graph()

    create_common_friends_counts()

