from functools import partial
import os
import json
import logging
from contextlib import closing
import importlib
import tempfile
import time
import subprocess

import argh
from argh import CommandError
from Coronado.Plugin import AppPlugin as AppPluginBase, \
        CommandLinePlugin as CLPluginBase
import pymysql
from pymysql.cursors import DictCursor

logger = logging.getLogger(__name__)

config = \
{
    'mysqlSchemaPackage': None,
    'mysqlHost': 'localhost',
    'mysqlPort': 3306,
    'mysqlUser': None,
    'mysqlPassword': None,
    'mysqpDbName': None,
    'mySchemaFilePath': None
}

def getMysqlConnection(context):
    # Connect to MySQL
    database = pymysql.connect(host=context['mysqlHost'],
        user=context['mysqlUser'], passwd=context['mysqlPassword'],
        db=context['mysqlDbName'], use_unicode=True, charset='utf8',
        cursorclass=DictCursor)

    # Turn on autocommit
    database.autocommit(True)

    # Set wait_timeout to its largest value (365 days): connection will be
    # disconnected only if it is idle for 365 days.
    with closing(database.cursor()) as cursor:
        cursor.execute("SET wait_timeout=31536000")

    return database


class SchemaVersionMismatch(Exception):
    pass


class AppPlugin(AppPluginBase):
    context = None

    def getId(self):
        return 'mysqlPlugin'

    def start(self, context):
        self.context = context

        if 'database' not in context:
            context['database'] = getMysqlConnection(context)

        if 'getNewDbConnection' not in context:
            context['getNewDbConnection'] = \
                partial(getMysqlConnection, context)

        # Check Database schema version matches what is expected
        self.checkDbSchemaVersion()

        self.context['shortcutAttrs'] += ['database', 'getNewDbConnection']


    def getCurrDbSchemaVersion(self):
        '''
        Get currently installed database schema version.
        '''
        currentVersion = None
        with closing(self.context['database'].cursor()) as cursor:
            try:
                cursor.execute('SELECT * FROM metadata WHERE attribute = %s',
                        ('version',))
            except pymysql.ProgrammingError as e:
                # 1146 == table does not exist
                if e.args[0] == 1146:
                    # Version 1 tables don't exist either, so it is most
                    # likely that no schema is installed
                    return None
                else:
                    raise
            else:
                row = cursor.fetchone()
                if not row:
                    raise DatabaseError('Could not read current ' +
                        'database version')
                currentVersion = row['value']

        return currentVersion


    def checkDbSchemaVersion(self):
        currentVersion = self.getCurrDbSchemaVersion()

        # Get most recent version from context
        expectedVersion = self.context['mysqlSchemaPackage'].versions[-1]

        if currentVersion != expectedVersion:
            raise SchemaVersionMismatch(
                ('Installed database schema version {} does '
                'not match expected version {}').format(
                    currentVersion, expectedVersion))



class CommandLinePlugin(CLPluginBase):
    context = None

    def getConfig(self):
        return \
        {
            'name': 'mysql',
            'title': 'MySQL database operations',
            'commands': 
            [
                self.execute,
                self.installSchema,
                self.importData, 
                self.getSchemaVersion, 
                self.upgrade,
                self.overlay,
                self.trim
            ],
            'namespace': True
        }


    def setup(self, context):
        self.context = context


    def execute(self, sqlFilePath):
        '''
        Execute the SQL file at the given path.
        '''
        context = self.context

        logger.info('Executing SQL from file %s...', (sqlFilePath,))

        cmd = 'mysql --host=%s --user=%s --password="%s" %s < %s' \
                % (context['mysqlHost'], context['mysqlUser'],
                        context['mysqlPassword'], context['mysqlDbName'],
                        sqlFilePath)
        subprocess.check_call(cmd, shell=True)

        logger.info('Executed file ' + sqlFilePath + ' successfully.')


    @argh.arg('-l', '--logLevel', 
            help='one of "debug", "info", "warning", "error", and "critical"')
    @argh.arg('--logFormat', 
            help='Python-like log format (see Python docs for details)')
    def installSchema(self, logLevel='warning',
            logFormat='%(levelname)s:%(name)s (at %(asctime)s): %(message)s'):
        '''
        Install the application's database schema.

        Warning: this will wipe out any existing database!
        '''
        logging.basicConfig(level=getattr(logging, logLevel.upper(),
            logging.NOTSET), format=logFormat)
        context = self.context

        # Drop database if exists
        cmd = 'mysql --host=%s --user=%s --password="%s" \
                --execute="DROP DATABASE IF EXISTS \\`%s\\`"' \
                % (context['mysqlHost'], context['mysqlUser'],
                        context['mysqlPassword'], context['mysqlDbName'])
        subprocess.check_call(cmd, shell=True)

        logger.info('Creating database...')

        # Create database
        cmd = 'mysql --host=%s --user=%s --password="%s" \
                --execute="CREATE DATABASE \\`%s\\`"' \
                % (context['mysqlHost'], context['mysqlUser'], context['mysqlPassword'],
                        context['mysqlDbName'])
        subprocess.check_call(cmd, shell=True)

        return self.execute(context['mySchemaFilePath'])


    @argh.arg('-l', '--logLevel', 
            help='one of "debug", "info", "warning", "error", and "critical"')
    @argh.arg('--logFormat', 
            help='Python-like log format (see Python docs for details)')
    def importData(self, jsonDataFilePath, ignoreConflicts=False,
            logLevel='warning',
            logFormat='%(levelname)s:%(name)s (at %(asctime)s): %(message)s'):
        logging.basicConfig(level=getattr(logging, logLevel.upper(),
            logging.NOTSET), format=logFormat)

        # Get a database connection
        db = getMysqlConnection(self.context)

        # Load data file
        tables = json.load(open(jsonDataFilePath, encoding='utf-8'))

        # Import
        for table in tables:
            logger.info('Installing table %s', table['name'])
            for row in table['rows']:
                query = 'INSERT INTO ' + table['name'] + ' (' \
                        + ','.join(row.keys()) \
                        + ') VALUES (' + '%s' + ',%s' * (len(row) -1) + ')'
                with closing(database.cursor()) as cursor:
                    try:
                        cursor.execute(query, tuple(row.values()))
                    except pymysql.IntegrityError:
                        if ignoreConflicts:
                            logger.info('Ignoring conflict in table %s',
                                    table['name'])
                            continue
                        else:
                            raise


    def getSchemaVersion(self, metadataTableName='metadata'):
        '''
        Get currently installed database schema version.
        '''
        currentVersion = None
        with closing(getMysqlConnection(self.context)) as db:
            with closing(db.cursor()) as cursor:
                try:
                    cursor.execute('SELECT * FROM ' + metadataTableName + \
                            ' WHERE attribute = %s', ('version',))
                except pymysql.ProgrammingError as e:
                    # 1146 == table does not exist
                    if e.args[0] == 1146:
                        # Version 1 tables don't exist either, so it is most
                        # likely that no schema is installed
                        return None
                    else:
                        raise
                else:
                    row = cursor.fetchone()
                    if not row:
                        raise CommandError('Could not read current ' +
                            'database version')
                    currentVersion = row['value']

        return currentVersion


    @argh.arg('-t', '--targetVersion', 
            help='target version for upgrade '
                 '(default is latest version in Database package')
    @argh.arg('-l', '--logLevel', 
            help='one of "debug", "info", "warning", "error", and "critical"')
    @argh.arg('--logFormat', 
            help='Python-like log format (see Python docs for details)')
    def upgrade(self, targetVersion=None, logLevel='warning',
            logFormat='%(levelname)s:%(name)s (at %(asctime)s): %(message)s'):
        '''
        Perform a schema upgrade. An upgrade means a complete migration of
        data to the target version. It may be safer or more useful to 
        wield the overlay-trim duo (as long as database size doesn't become 
        an issue).

        This will call the application's target version's upgrade function. 
        '''
        logging.basicConfig(level=getattr(logging, logLevel.upper(),
            logging.NOTSET), format=logFormat)

        currentVersion = self.getSchemaVersion()

        if currentVersion is None:
            raise CommandError('It seems there is no schema ' +
                'currently installed. You can install a schema with ' +
                'the "install" command.')
        elif currentVersion == targetVersion:
            raise CommandError('Schema version ' + currentVersion 
                    + ' is already installed')

        logger.info('Current schema version = %s', currentVersion)

        # Default target version is the latest one available
        if targetVersion is None:
            targetVersion = self.context['mysqlSchemaPackage'].versions[-1]

        # Get module for target version
        targetVersMod = importlib.import_module(
                self.context['mysqlSchemaPackage'].__name__ + '.v' +
                str(targetVersion))

        # Make sure it has an upgrade function
        if not hasattr(targetVersMod, 'upgrade'):
            raise CommandError('Version ' + targetVersion +
                    ' does not support ' +
                    'the upgrade operation (hint: overlay and trim ' +
                    'may be supported).')

        # Delegate to appropriate upgrade function
        with closing(getMysqlConnection(self.context)) as db:
            context = self.context.copy()
            context['database'] = db
            targetVersMod.upgrade(context, fromVersion=currentVersion)


    @argh.arg('-t', '--targetVersion', 
            help='target version for upgrade '
                 '(default is latest version in Database package')
    @argh.arg('-l', '--logLevel', 
            help='one of "debug", "info", "warning", "error", and "critical"')
    @argh.arg('--logFormat', 
            help='Python-like log format (see Python docs for details)')
    def overlay(self, targetVersion=None,
            logLevel='warning', 
            logFormat='%(levelname)s:%(name)s (at %(asctime)s): %(message)s'):
        '''
        Overlay a schema on top of the currently installed one.

        Overlaying is a non-destructive, backwards-compatible first-step for
        schema migration. After overlaying, a superimposition of the current
        and target schemas will be installed. This is useful when both versions
        need to be simultaneously supported. When the previous version is no
        longer needed, perform a "trim" operation on the database.

        This will call the application's target version's overlay function. 
        '''
        logging.basicConfig(level=getattr(logging, logLevel.upper(),
            logging.NOTSET), format=logFormat)

        currentVersion = self.getSchemaVersion()

        if currentVersion is None:
            raise CommandError('It seems there is no schema ' +
                'currently installed. You can install a schema with ' +
                'the "install" command.')
        elif currentVersion == targetVersion:
            raise CommandError('Schema version ' + currentVersion 
                    + ' is already installed')

        logger.info('Current schema version = %s', currentVersion)

        # Default target version is the latest one available
        if targetVersion is None:
            targetVersion = self.context['mysqlSchemaPackage'].versions[-1]

        # Get module for target version
        targetVersMod = importlib.import_module(
                self.context['mysqlSchemaPackage'].__name__ + '.v' +
                str(targetVersion))

        # Make sure it has an overlay function
        if not hasattr(targetVersMod, 'overlay'):
            raise CommandError('Version ' + targetVersion +
                    ' does support the overlay operation.')

        # Delegate to appropriate upgrade function
        with closing(getMysqlConnection(self.context)) as db:
            context = self.context.copy()
            context['database'] = db
            targetVersMod.overlay(context, fromVersion=currentVersion)


    @argh.arg('-r', '--referenceVersion', 
            help='reference version for trim operation')
    @argh.arg('-t', '--trimVersion', 
            help='version that is now obsolete')
    @argh.arg('-l', '--logLevel', 
            help='one of "debug", "info", "warning", "error", and "critical"')
    @argh.arg('--logFormat', 
            help='Python-like log format (see Python docs for details)')
    def trim(self, referenceVersion=None, trimVersion=None,
            logLevel='warning', 
            logFormat='%(levelname)s:%(name)s (at %(asctime)s): %(message)s'):
        '''
        Trim the database to remove any data from a previous schema version 
        that is now irrelevant to the reference schema version and thus
        obsolete.  This operation should be performed once the previous schema
        version no longer needs to be supported.

        This will call the application's reference version's trim function. If
        no reference version is specified, this will trim with reference to the
        currently installed schema version.
        '''
        logging.basicConfig(level=getattr(logging, logLevel.upper(),
            logging.NOTSET), format=logFormat)

        if referenceVersion is None:
            referenceVersion = self.getSchemaVersion()

        if referenceVersion is None:
            raise CommandError('It seems there is no schema ' +
                'currently installed.')

        # Get module for target version
        refVersMod = importlib.import_module(
                self.context['mysqlSchemaPackage'].__name__ + '.v' + \
                        str(referenceVersion))

        # Make sure it has a trim function
        if not hasattr(refVersMod, 'trim'):
            raise CommandError('Version ' + referenceVersion + ' does not ' +
                'support the trim operation.')

        # Set default trim version
        if trimVersion is None:
            trimVersion = refVersMod.previousVersion

        # Confirm with user
        response = askYesOrNoQuestion('Trim is a destructive and ' +
            'irreversible operation. Are you sure you want to proceed?')

        if response == 'y':
            # Delegate to appropriate upgrade function
            with closing(getConnection()) as db:
                context = self.context.copy()
                context['database'] = db
                refVersMod.trim(context, trimVersion)
        else:
            logger.info('Trim not performed (whew!)')



def askYesOrNoQuestion(question):
    cfm = raw_input(question + ' (y/n): ')
    while cfm != 'y' and cfm != 'n':
        cfm = raw_input('Please type y or n: ')
    return cfm
