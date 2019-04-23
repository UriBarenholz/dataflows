def saver(resource, db, batch_size):
    gen = db.insert_generator(
        (("{:08x}".format(idx), row)
         for idx, row
         in enumerate(resource)),
        batch_size=batch_size
    )
    for _, row in gen:
        yield row

def loader(db):
    for _, value in db.items():
        yield value
