from functools import total_ordering

@total_ordering
class TableColumnPair:
    def __init__(self, table:str, column:str):
        self.table = table.lower()
        self.column = column.lower()
        
    def __eq__(self, other):
        assert isinstance(other, TableColumnPair)
        return self.table == other.table and self.column == other.column

    def __lt__(self, other):
        assert isinstance(other, TableColumnPair)
        return (self.table, self.column) < (other.table, other.column)
    
    def __hash__(self):
        return hash((self.table, self.column))
    
    
    def __repr__(self):
        return f"{self.table}.{self.column}"
    
    def __str__(self):
        return f"{self.table}.{self.column}"
    
    
class DSU:
    def __init__(self, elements: list):
        assert len(elements) == len(set(elements)), "Elements should be unique."
        self.parent = {x:x for x in elements}
        self.size = {x:1 for x in elements}
        
        
    def find(self, x):
        if x == self.parent[x]:
            return x
        else:
            self.parent[x] = self.find(self.parent[x])
            return self.parent[x]
    
    def union(self, x, y):
        px, py = self.find(x), self.find(y)
        if px != py:
            self.parent[px] = py
            self.size[py] += self.size[px]
            
    def get_size(self, x) -> int:
        return self.size[self.find(x)]

    def same(self, x, y) -> bool:
        return self.find(x) == self.find(y)
