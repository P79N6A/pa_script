#coding=utf-8

class BCP(object):

    __apps_path = defaultdict(list)

    @classmethod
    def get_paths(cls):
        return cls.__apps_path

    @classmethod
    def set_path(cls, cache_db, software_type):
        cls.__apps_path[str(software_type)].append([cache_db, software_type])