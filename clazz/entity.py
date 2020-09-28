# 实体类

# 股票相关信息
class Stock:
    code = ""
    name = ""

    def __init__(self, c: object, n: object) -> object:
        self.code = c
        self.name = n

    def to_json(self):
        return {
            "code": self.code,
            "name": self.name
        }
