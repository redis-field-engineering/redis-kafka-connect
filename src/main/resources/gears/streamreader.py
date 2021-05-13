proc = GB('StreamReader')
proc.foreach(lambda x: execute('SADD', 'myset', x['body']))
proc.register('mystream')