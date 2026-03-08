from fs_arbitrage.variety_pair import VarietyPair


class PairContainer:
    def __init__(self):
        self._pairs:list[VarietyPair] = []
        self._pair_set= set()
        self._code_to_pair:dict[str,set[VarietyPair]] = {}

    def add(self,pair:VarietyPair ):
        if pair in self._pair_set:
            return
        self._pair_set.add(pair)
        self._pairs.append(pair)

        if pair.buy_leg.code not in self._code_to_pair:
            self._code_to_pair[pair.buy_leg.code] = set()
        self._code_to_pair[pair.buy_leg.code].add(pair)

        if pair.sell_leg.code not in self._code_to_pair:
            self._code_to_pair[pair.sell_leg.code] = set()
        self._code_to_pair[pair.sell_leg.code].add(pair)


    def get_pairs_by_code(self,code:str) -> set[VarietyPair]:
        return self._code_to_pair.get(code)

    def get_pairs_by_code_batch(self,codes:list[str]) -> set[VarietyPair]:
        r = set()
        for code in codes:
            r |= self.get_pairs_by_code(code)
        return r

