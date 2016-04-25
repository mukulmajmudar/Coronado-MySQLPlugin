def buildInsertQuery(table, keys):
    '''
    Build an insert query with dictionary-type placeholders for the given keys.
    '''

    return 'INSERT INTO ' + table +' (' + ','.join(keys) + \
            ') VALUES(%(' + ')s,%('.join(keys) + ')s)'


def whereEquals(equalConditions, coordinator='AND'):
    '''
    Build a WHERE clause with dictionary-type placeholders where all
    conditions are "equals" and the coordinating conjunction is the same.
    '''
    coordinator = coordinator + ' '
    return coordinator.join(
            [key + ' = %(' + key + ')s ' for key in equalConditions])


def makePlaceholderList(sourceList):
    return ','.join(['%s'] * len(sourceList))
