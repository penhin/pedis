from typing import Any

import random

MAX_LEVEL = 16
P = 0.5

class SkipListNode():
    """Represents a single node in a Redis skiplist."""
    def __init__(self, score: float, member: Any, level: int):
        self.score = score
        self.member = member
        self.forward = [None] * level

class SkipList():
    def __init__(self):
        self.level = 1
        self.head = SkipListNode(0, None, MAX_LEVEL)
    
    def random_level(self) -> int:
        level = 1
        while random.random() < P and level < MAX_LEVEL:
            level += 1
        return level
    
    def insert(self, score: float, member: Any):
        update = [None] * MAX_LEVEL
        current = self.head
        
        for i in reversed(range(self.level)):
            while(
                current.forward[i]
                and (current.forward[i].score, current.forward[i].member) < (score, member)
            ):
                current = current.forward[i]
                
            update[i] = current
        
        level = self.random_level()
        
        if level > self.level:
            for i in range(self.level, level):
                update[i] = self.head
            self.level = level
        
        node = SkipListNode(score, member, level)
        
        for i in range(level):
            node.forward[i] = update[i].forward[i]
            update[i].forward[i] = node
            
    def search(self, score: float) -> SkipListNode:
        current = self.head

        for i in reversed(range(self.level)):
            while(
                current.forward[i]
                and current.forward[i].score < score
            ):
                current = current.forward[i]
        
        current = current[0]
        
        if current and current.score == score:
            return current

        return None
    
    def delete(self, score: float, member: Any):
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
                if update[i].forward[i] != current:
                    break
                update[i].forward[i] = current.forward[i]
            
            while self.level > 1 and self.head.forward[self.level-1] is None:
                self.level -= 1


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

