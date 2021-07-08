# Capture each keyspace event and store it in a stream
GB('KeysReader') \
.foreach(lambda x:
            execute('XADD', '${stream}', '*', 'key', x['key'], 'value', x['value'], 'type', x['type'], 'event', x['event'])) \
.register(prefix='${prefix}',
             mode='sync',
             eventTypes=${eventTypes},
             readValue=True)
