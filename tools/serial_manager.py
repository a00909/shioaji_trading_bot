class SerialManager:
    MAX_SERIAL = 65536

    def __init__(self):
        self.map: dict[str, int] = {}

    def get(self, key):
        if key in self.map:
            ret = self.map[key]
            self.map[key] += 1
            if self.map[key] >= self.MAX_SERIAL:
                self.map[key] = 0
        else:
            self.map[key] = 0
            ret = 0
        return ret


serial_manager = SerialManager()
