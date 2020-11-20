import hashlib
import mmh3
import math


def int_to_float(value: int) -> float:
    fifty_three_ones = 0xFFFFFFFFFFFFFFFF >> (64 - 53)
    fifty_three_zeros = float(1 << 53)
    return (value & fifty_three_ones) / fifty_three_zeros


class HWR:
    def __init__(self, nodes, seed):
        self.nodes = []
        self.seed = seed
        if nodes is not None:
            self.nodes = nodes
        # self.hash_function = lambda x: mmh3.hash(x, seed)

    def compute_weighted_score(self, key):
        hash_1, hash_2 = mmh3.hash64(str(key), 0xFFFFFFFF & self.seed)
        hash_f = int_to_float(hash_2)
        score = 1.0 / -math.log(hash_f)
        return score

    def add_node(self, node):
        if node not in self.nodes:
            self.nodes.append(node)

    def remove_node(self, node):
        if node in self.nodes:
            self.nodes.remove(node)
        else:
            raise ValueError("No such node %s to remove" % node)

    def find_leader_node(self, key):
        high_score = -1
        leader = None
        for node in self.nodes:
            score = self.compute_weighted_score(
                "%s-%s" % (str(node), str(key)))
            # print("%s-%s" % (str(node), str(key)))
            # print("Node, Score: %s, %s" % (node, score))
            if score > high_score:
                leader = node
                high_score = score
        # print("Leader: ", leader)
        return leader
