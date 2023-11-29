from MapReduce import MapReduce


class FindCyclicReferences(MapReduce):
    @staticmethod
    def _get_key(line):
        a, b = line.split()
        if a < b:
            return f"({a}, {b})"
        return f"({b}, {a})"

    def Map(self, parts):
        print("parts", parts)
        partial_result = {}
        for part in parts:
            print(self._get_key(part))
            if (k := self._get_key(part)) in partial_result:
                partial_result[k] = 1
            else:
                partial_result[k] = 0
        print(partial_result)
        return partial_result

    def Reduce(self, kvs):
        if kvs is None:
            return None
        temp = {}
        result = {}
        for partial_result in kvs:
            for k in partial_result:
                if k in temp or partial_result[k] == 1:
                    result[k] = 1
                else:
                    temp[k] = 0
        return result
