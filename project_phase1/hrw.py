import hashlib
import mmh3


class HWR:
    def __init__(self, nodes, seed):
        self.nodes = []
        self.seed = seed
        if nodes is not None:
            self.nodes = nodes
        self.hash_function = lambda x: mmh3.hash(x, seed)

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
            score = self.hash_function("%s-%s" % (str(node), str(key)))
            print("%s-%s" % (str(node), str(key)))
            print("Score: %s" % score)
            if score > high_score:
                leader = node
                high_score = score
        print("Leader: ", leader)
        return leader
