import hashlib


class ConsistentHashing:
    def __init__(self, nodes=None, replicas=1):
        self.replicas = replicas
        self.ring = dict()
        self.sorted_keys = []
        if nodes:
            for node in nodes:
                self.add_node(node)

    def my_hash(self, key):
        m = hashlib.md5(key.encode('utf-8'))
        return int(m.hexdigest(), 16) % (2**32)

    def add_node(self, node):
        # for i in range(0, self.replicas):
        key = self.my_hash(node)
        self.ring[key] = node
        self.sorted_keys.append(key)
        self.sorted_keys.sort()

    def remove_node(self, node):
        # for i in range(0, self.replicas):
        key = self.my_hash(node)
        del self.ring[key]
        self.sorted_keys.remove(key)

    # Get node and node position
    def get_node(self, str_key):
        h = self.my_hash(str_key)
        if h > self.sorted_keys[-1]:
            return self.ring[self.sorted_keys[0]], 0
        for i in range(len(self.sorted_keys)):
            if h <= self.sorted_keys[i]:
                return self.ring[self.sorted_keys[i]], i

    # Save for future implementation
    # def get_nodes(self, str_key):
    #     print("here1")
    #     if not self.ring:
    #         print("here2")
    #         yield None, None
    #     node, pos = self.get_node(str_key)
    #     print(node)
    #     print("node")
    #     for key in self.sorted_keys[pos:]:
    #         yield self.ring[key]
    #     while True:
    #         for key in self.sorted_keys:
    #             yield self.ring[key]
