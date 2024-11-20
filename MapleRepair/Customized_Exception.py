class DispatchError(Exception):
    def __init__(self, message=''):
        self.message = message
        super().__init__(self.message)
        
class NoForeignKeyError(Exception):
    def __init__(self, message=''):
        self.message = message
        super().__init__(self.message)
        
class NoAliasError(Exception):
    def __init__(self, message=''):
        self.message = message
        super().__init__(self.message)
        
class NoSuchTableError(Exception):
    def __init__(self, message=''):
        self.message = message
        super().__init__(self.message)
        
class NoSuchColumnError(Exception):
    def __init__(self, message=''):
        self.message = message
        super().__init__(self.message)