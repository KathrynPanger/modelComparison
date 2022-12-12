
class Variable():
    def __init__(self, name: str):
        self.name = name
        self.entropy = None
    def __repr__(self):
        print(f"name: {self.name}, entropy: {self.entropy}")
