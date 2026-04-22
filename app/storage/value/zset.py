import random

MAX_LEVEL = 16
P = 0.5

class SkipListNode():
    """Represents a single node in a Redis skiplist."""
    def __init__(self, score: float, member: bytes, level: int):
        self.score = score
        self.member = member
        self.forward = [None] * level
        self.span = [0] * level

class SkipList():
    def __init__(self):
        self.level = 1
        self.length = 0
        self.head = SkipListNode(0, None, MAX_LEVEL)
    
    def random_level(self) -> int:
        level = 1
        while random.random() < P and level < MAX_LEVEL:
            level += 1
        return level
    
    def first_in_rank(self, rank: int):
        if rank < 0 or rank >= self.length:
            return None
        
        target_rank = rank + 1
        traversed = 0
        current = self.head

        for i in reversed(range(self.level)):
            while current.forward[i] and (traversed + current.span[i]) <= target_rank:
                traversed += current.span[i]
                current = current.forward[i]

        return current
    
    def insert(self, score: float, member: bytes):
        update = [None] * MAX_LEVEL
        rank = [0] * MAX_LEVEL
        current = self.head
        
        for i in reversed(range(self.level)):
            rank[i] = 0 if i == self.level - 1 else rank[i + 1]
            while(
                current.forward[i]
                and (current.forward[i].score, current.forward[i].member) < (score, member)
            ):
                rank[i] += current.span[i]
                current = current.forward[i]
                
            update[i] = current
        
        level = self.random_level()
        
        if level > self.level:
            for i in range(self.level, level):
                rank[i] = 0
                update[i] = self.head
                update[i].span[i] = self.length
            self.level = level
        
        node = SkipListNode(score, member, level)
        
        for i in range(level):
            offset = rank[0] - rank[i]
            node.forward[i] = update[i].forward[i]
            node.span[i] = update[i].span[i] - offset if update[i].span[i] else 0
            update[i].forward[i] = node
            update[i].span[i] = offset + 1

        for i in range(level, self.level):
            update[i].span[i] += 1

        self.length += 1
            
    def search(self, score: float) -> SkipListNode:
        current = self.head

        for i in reversed(range(self.level)):
            while(
                current.forward[i]
                and current.forward[i].score < score
            ):
                current = current.forward[i]
        
        current = current.forward[0]
        
        if current and current.score == score:
            return current

        return None

    def rank(self, score: float, member: bytes) -> int | None:
        current = self.head
        rank = 0

        for i in reversed(range(self.level)):
            while (
                current.forward[i]
                and (current.forward[i].score, current.forward[i].member) <= (score, member)
            ):
                rank += current.span[i]
                current = current.forward[i]

                if (current.score, current.member) == (score, member):
                    return rank - 1

        return None
    
    def delete(self, score: float, member: bytes):
        update = [None] * MAX_LEVEL
        current = self.head
        
        for i in reversed(range(self.level)):
            while(
                current.forward[i]
                and (current.forward[i].score, current.forward[i].member) < (score, member)
            ):
                current = current.forward[i]                

            update[i] = current

        current = current.forward[0]

        if current and (current.score, current.member) == (score, member):
            for i in range(self.level):
                if update[i].forward[i] == current:
                    update[i].span[i] += current.span[i] - 1
                    update[i].forward[i] = current.forward[i]
                else:
                    update[i].span[i] -= 1
            
            while self.level > 1 and self.head.forward[self.level-1] is None:
                self.level -= 1

            self.length -= 1
            return True

        return False

    def display(self):
        for i in reversed(range(self.level)):
            node = self.head.forward[i]
            print("Level", i, end=": ")

            while node:
                print(node.score, end=" ")
                node = node.forward[i]

            print()
            

class SortedSet():
    """Redis zset implementation."""

    def __init__(self):
        self.dict: dict[bytes, float] = {}
        self.skiplist = SkipList()

    def add(self, pairs: list[tuple[float, bytes]]) -> int:
        added = 0

        for score, member in pairs:
            previous = self.dict.get(member)
            if previous is None:
                added += 1
            else:
                self.skiplist.delete(previous, member)

            self.dict[member] = score
            self.skiplist.insert(score, member)

        return added

    def rank(self, member: bytes) -> int | None:
        score = self.dict.get(member)
        if score is None:
            return None

        return self.skiplist.rank(score, member)

    def card(self) -> int:
        return len(self.dict)

    def score(self, member: bytes) -> bytes | None:
        score = self.dict.get(member)
        if score is None:
            return None

        return str(score).encode()

    def remove(self, members: list[bytes]) -> int:
        removed = 0

        for member in members:
            score = self.dict.pop(member, None)
            if score is None:
                continue

            self.skiplist.delete(score, member)
            removed += 1

        return removed
        
    def range(self, start: int, stop: int) -> list[bytes]:
        n = self.skiplist.length
        if n == 0:
            return []
        
        if start < 0:
            start += n
        if stop < 0:
            stop += n

        if start < 0:
            start = 0
        if stop >= n:
            stop = n - 1
            
        if start >= n or start > stop:
            return []
        
        node = self.skiplist.first_in_rank(start)
        if node is None:
            return []
        
        result = []
        remaining = stop - start + 1
        while node is not None and remaining > 0:
            result.append(node.member)
            node = node.forward[0]
            remaining -= 1

        return result
