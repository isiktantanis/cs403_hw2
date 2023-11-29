from MapReduce import MapReduce


class FindCitations(MapReduce):
    def Map(self, parts):
        partial_result = {}
        for line in parts:
            _, id = line.split()
            if id not in partial_result:
                partial_result[id] = 1
            else:
                partial_result[id] += 1
        return partial_result

    def Reduce(self, kvs):
        if kvs is None:
            return None
        result = {}
        print(kvs)
        for kv in kvs:
            for key in kv:
                if key not in result:
                    result[key] = kv[key]
                else:
                    result[key] += kv[key]
        return result
