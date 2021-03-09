import sys
import os
import inspect


def log_exceptions(**kwargs):
    from datetime import datetime as akdatetime
    import traceback

    directory = 'logs/'
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(directory + os.path.relpath(inspect.stack()[1][1]) + '_error.log', 'a+') as fp:
        nowtime = str(akdatetime.now())
        tb = traceback.format_exc()
        entry = ('===================================================================================================\n'
                 f'{nowtime}\n'
                 '---------------------------------------------------------------------------------------------------\n'
                 f'sys.args->{sys.argv}\n'
                 '---------------------------------------------------------------------------------------------------\n'
                 f'variables->{str(kwargs)}\n'
                 '---------------------------------------------------------------------------------------------------\n'
                 f'{tb}\n')
        fp.write(entry)

def log_data(**kwargs):
    from datetime import datetime as akdatetime

    directory = 'logs/'
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(directory + os.path.relpath(inspect.stack()[1][1]) + '_data.log', 'a+') as fp:
        nowtime = str(akdatetime.now())
        entry = ('===================================================================================================\n'
                 f'{nowtime}\n'
                 '---------------------------------------------------------------------------------------------------\n'
                 f'sys.args->{sys.argv}\n'
                 '---------------------------------------------------------------------------------------------------\n'
                 f'variables->{str(kwargs)}\n')
        fp.write(entry)

def custom_log_data(**kwargs):
    from datetime import datetime as akdatetime

    directory = 'logs/'
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(directory + kwargs['filename'] + '.log', 'a+') as fp:
        nowtime = str(akdatetime.now())
        entry = ('===================================================================================================\n'
                 f'{nowtime}\n'
                 '---------------------------------------------------------------------------------------------------\n'
                 f'variables->{str(kwargs)}\n')
        fp.write(entry)