import copy
from kvfile import KVFile
from ..helpers.saver_loader import saver, loader


def pre_condition(rows, field, value):
    found = False
    for row in rows:
        if row[field] == value:
            found = True
        if not found:
            yield row

def post_condition(rows, field, value):
    found = False
    for row in rows:
        if row[field] == value:
            found = True
        if found:
            yield row

def split(source=None,
            field=None,
            value=None,
            post_target_name=None,
            post_target_path=None,
            batch_size=1000):
    def func(package):
        source_, post_target_name_, post_target_path_ = source, post_target_name, post_target_path
        if source_ is None:
            source_ = package.pkg.descriptor['resources'][0]['name']
        if post_target_name is None:
            post_target_name_ = source_ + '_post'
        if post_target_path is None:
            post_target_path_ = post_target_name_ + '.csv'

        def traverse_resources(resources):
            for res in resources:
                yield res
                if res['name'] == source_:
                    res = copy.deepcopy(res)
                    res['name'] = post_target_name_
                    res['path'] = post_target_path_
                    yield res

        descriptor = package.pkg.descriptor
        descriptor['resources'] = list(traverse_resources(descriptor['resources']))
        yield package.pkg

        for resource in package:
            if resource.res.name == source_:
                db = KVFile()
                yield pre_condition(saver(resource, db, batch_size), field, value)
                yield post_condition(loader(db), field, value)
            else:
                yield resource

    return func
