from functools import partial
import os
import json
import logging
from contextlib import closing
import importlib
import tempfile

import argh
from argh import CommandError
from Coronado.Config import Config as ConfigBase
from Coronado.Plugin import AppPlugin as AppPluginBase, CommandLinePlugin as CLPluginBase
import Coronado.Testing
import MySQLdb
from MySQLdb.cursors import DictCursor

logger = logging.getLogger(__name__)

class Config(ConfigBase):

    def __init__(self, keys=None): 
        if keys is None:
            keys = []
        super(Config, self).__init__(
        [
            'databasePkg',
            'mysql'
        ] + keys)


    def _getDatabasePkg(self):
        raise NotImplementedError()


    def _getMysql(self):
        return \
        {
            'host': self._getMysqlHost(),
            'port': self._getMysqlPort(),
            'user': self._getMysqlUser(),
            'password': self._getMysqlPassword(),
            'dbName': self._getMysqlDbName(),
            'schemaFilePath': self._getMySchemaFilePath()
        }

    def _getMysqlHost(self):
        return 'localhost'


    def _getMysqlPort(self):
        return 3306


    def _getMysqlUser(self):
        raise NotImplementedError()


    def _getMysqlPassword(self):
        raise NotImplementedError()


    def _getMysqlDbName(self):
        raise NotImplementedError()


    def _getMySchemaFilePath(self):
        raise NotImplementedError()


def getMysqlConnection(context):
    # Connect to MySQL
    mysqlArgs = context['mysql']
    database = MySQLdb.connect(host=mysqlArgs['host'],
            user=mysqlArgs['user'], passwd=mysqlArgs['password'],
            db=mysqlArgs['dbName'], use_unicode=True, charset='utf8',
            cursorclass=DictCursor)

    # Turn on autocommit
    database.autocommit(True)

    # Set wait_timeout to its largest value (365 days): connection will be
    # disconnected only if it is idle for 365 days.
    with closing(database.cursor()) as cursor:
        cursor.execute("SET wait_timeout=31536000")

    return database


class AppPlugin(AppPluginBase):
    pluginId = 'mysqlPlugin'
    context = None

    def setup(self, application, context):
        self.context = context

        if 'database' not in context:
            context['database'] = getMysqlConnection(context)

        if 'getNewDbConnection' not in context:
            context['getNewDbConnection'] = \
                partial(getMysqlConnection, context)

        # Check Database schema version matches what is expected
        self.checkDbSchemaVersion()

        application.addToContextFlatten(
        {
            'public': ['database']
        })


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
        expectedVersion = self.context['databasePkg'].versions[-1]

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
                self.installFixture, 
                self.mergeFixture, 
                self.getCurrentVersion, 
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
        mysql = self.context['mysql']

        logger.info('Executing SQL from file %s...', (sqlFilePath,))

        cmd = 'mysql --host=%s --user=%s --password="%s" %s < %s' \
                % (mysql['host'], mysql['user'], mysql['password'], 
                        mysql['dbName'], sqlFilePath)
        rc = os.system(cmd)
        if rc != 0:
            raise CommandError('Failed to execute SQL from file')

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
        Coronado.configureLogging(level=logLevel, format=logFormat)
        mysql = self.context['mysql']

        # Drop database if exists
        cmd = 'mysql --host=%s --user=%s --password="%s" \
                --execute="DROP DATABASE IF EXISTS \\`%s\\`"' \
                % (mysql['host'], mysql['user'], mysql['password'],
                        mysql['dbName'])
        rc = os.system(cmd)
        if rc != 0:
            raise CommandError('Failed to drop previous database')

        logger.info('Creating database...')

        # Create database
        cmd = 'mysql --host=%s --user=%s --password="%s" \
                --execute="CREATE DATABASE \\`%s\\`"' \
                % (mysql['host'], mysql['user'], mysql['password'],
                        mysql['dbName'])
        rc = os.system(cmd)
        if rc != 0:
            raise CommandError('Failed to create database')

        return self.execute(mysql['schemaFilePath'])


    def installFixture(self, schemaFilePath, fixtureFilePath):
        # Re-install schema
        logger.info('Reinstalling schema...')
        installSchema()
        logger.info('Installing fixture...')

        # Get a database connection
        db = getMysqlConnection(self.context)

        # Load fixture file
        fixture = json.load(open(fixtureFilePath))

        # Install the fixture
        installAFixture(db, fixture)


    @argh.arg('-l', '--logLevel', 
            help='one of "debug", "info", "warning", "error", and "critical"')
    @argh.arg('--logFormat', 
            help='Python-like log format (see Python docs for details)')
    def mergeFixture(self, fixtureFilePath, ignoreConflicts=False, logLevel='warning',
            logFormat='%(levelname)s:%(name)s (at %(asctime)s): %(message)s'):
        Coronado.configureLogging(level=logLevel, format=logFormat)

        # Get a database connection
        db = getMysqlConnection(self.context)

        # Load fixture file
        fixture = json.load(open(fixtureFilePath))

        # Install the fixture
        installAFixture(db, fixture, ignoreConflicts)


    def getCurrentVersion(self):
        '''
        Get currently installed database schema version.
        '''
        currentVersion = None
        with closing(getMysqlConnection(self.context)) as db:
            with closing(db.cursor()) as cursor:
                try:
                    cursor.execute('SELECT * FROM metadata WHERE attribute = %s',
                            ('version',))
                except MySQLdb.ProgrammingError as e:
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
        data to the target version. It is usually safer and more useful to 
        wield the overlay-trim duo (as long as database size doesn't become 
        an issue).

        This will call the application's target version's upgrade function. 
        '''
        Coronado.configureLogging(level=logLevel, format=logFormat)

        currentVersion = getCurrentVersion()

        if currentVersion is None:
            raise CommandError('It seems there is no schema currently installed. ' +
                    'You can install a schema with the "install" command.')
        elif currentVersion == targetVersion:
            raise CommandError('Schema version ' + currentVersion 
                    + ' is already installed')

        logger.info('Current schema version = %s', currentVersion)

        # Default target version is the latest one available
        if targetVersion is None:
            targetVersion = self.context['databasePkg'].versions[-1]

        # Get module for target version
        targetVersMod = importlib.import_module(
                self.context['databasePkg'].__name__ + '.v' + str(targetVersion))

        # Make sure it has an upgrade function
        if not hasattr(targetVersMod, 'upgrade'):
            raise CommandError('Version ' + targetVersion + ' does not support ' +
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

        Overlaying is a non-destructive, backwards-compatible first-step for schema
        migration. After overlaying, a superimposition of the current and target
        schemas will be installed. This is useful when both versions need to
        be simultaneously supported. When the previous version is no longer needed,
        perform a "trim" operation on the database.

        This will call the application's target version's overlay function. 
        '''
        Coronado.configureLogging(level=logLevel, format=logFormat)

        currentVersion = getCurrentVersion()

        if currentVersion is None:
            raise CommandError('It seems there is no schema currently installed. ' +
                    'You can install a schema with the "install" command.')
        elif currentVersion == targetVersion:
            raise CommandError('Schema version ' + currentVersion 
                    + ' is already installed')

        logger.info('Current schema version = %s', currentVersion)

        # Default target version is the latest one available
        if targetVersion is None:
            targetVersion = self.context['databasePkg'].versions[-1]

        # Get module for target version
        targetVersMod = importlib.import_module(
                self.context['databasePkg'].__name__ + '.v' + str(targetVersion))

        # Make sure it has an overlay function
        if not hasattr(targetVersMod, 'overlay'):
            raise CommandError('Version ' + targetVersion + ' does support the ' +
                    'overlay operation.')

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
        that is now irrelevant to the reference schema version and thus obsolete.
        This operation should be performed once the previous schema version no 
        longer needs to be supported.

        This will call the application's reference version's trim function. If no
        reference version is specified, this will trim with reference to the 
        currently installed schema version.
        '''
        Coronado.configureLogging(level=logLevel, format=logFormat)

        if referenceVersion is None:
            referenceVersion = getCurrentVersion()

        if referenceVersion is None:
            raise CommandError('It seems there is no schema currently installed.')

        # Get module for target version
        refVersMod = importlib.import_module(
                self.context['databasePkg'].__name__ + '.v' + \
                        str(referenceVersion))

        # Make sure it has a trim function
        if not hasattr(refVersMod, 'trim'):
            raise CommandError('Version ' + referenceVersion + ' does not ' +
                'support the trim operation.')

        # Set default trim version
        if trimVersion is None:
            trimVersion = refVersMod.previousVersion

        # Confirm with user
        response = askYesOrNoQuestion('Trim is a destructive and irreversible ' +
                'operation. Are you sure you want to proceed?')

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



def _installFixture(database, fixture, ignoreConflicts):
    if 'tableOrder' not in fixture:
        return

    # Install fixture into database
    for tableName in fixture['tableOrder']:
        logger.info('Installing table %s', tableName)
        for row in fixture[tableName]:
            query = 'INSERT INTO ' + tableName + ' (' \
                    + ','.join(row.keys()) \
                    + ') VALUES (' + '%s' + ',%s' * (len(row) -1) + ')'
            with closing(database.cursor()) as cursor:
                try:
                    cursor.execute(query, tuple(row.values()))
                except MySQLdb.IntegrityError:
                    if ignoreConflicts:
                        logger.info('Ignoring conflict in table %s', tableName)
                        continue
                    else:
                        raise


def installAFixture(database, fixture, ignoreConflicts=False):
    # If there is no "self" key in self.fixture, that means the
    # entire fixture dict is the self fixture
    if 'self' not in fixture:
        _installFixture(database, fixture, ignoreConflicts)
    else:
        # Fixtures for multiple apps are given
        for appName, fix in fixture.items():
            if appName == 'self':
                _installFixture(database, fix, ignoreConflicts)
            else:
                # Output fixture into a JSON file and wait for confirmation
                # from user that it has been loaded into the correct app
                f = tempfile.NamedTemporaryFile(
                        prefix=appName + '-', suffix='.json',
                        delete=False)
                json.dump(fix, f)
                f.flush()
                raw_input(('Please load the file "%s" into a test ' +
                    'instance of "%s". Press ENTER to continue.')
                    % (f.name, appName))


class FixtureMixin(Coronado.Testing.TestRoot):
    '''
    Database fixture mixin for TestCase.

    Implement _getFixture() to return either a dictionary or a
    JSON file path containing a fixture. The fixture should be
    a dictionary mapping table names to rows.
    '''

    def __init__(self, *args, **kwargs):
        '''
        A "context" keyword argument is required. It should be a dictionary
        containing at least the following mappings:

        database => MySQL database connection
        mysql => dictionary of MySQL connection arguments: must contain at least
                 user, password, dbName
        '''
        context = kwargs['context']
        self._database = context['database']
        self._mysqlArgs = context['mysql']

        # Call parent constructor
        super(FixtureMixin, self).__init__(*args, **kwargs)


    def setUp(self):
        # Call parent version
        super(FixtureMixin, self).setUp()

        # Get the fixture
        fixture = self._getFixture()

        # If fixture is a file path, load it as JSON
        if isinstance(fixture, str):
            fixture = json.load(open(fixture))

        # Fixture must be a dictionary
        if not isinstance(fixture, dict):
            raise IllegalArgument('fixture must be a dictionary')

        # Install the fixture
        installAFixture(self._database, fixture)


    def tearDown(self):
        '''
        Truncate all tables.
        '''
        # Call parent version
        super(FixtureMixin, self).tearDown()

        if self._mysqlArgs is None:
            return

        # Credit: http://stackoverflow.com/a/8912749/1196816
        cmd = ("mysql -u %s -p'%s' -Nse 'show tables' %s " \
                + "| while read table; do mysql -u %s -p'%s' " \
                + "-e \"truncate table $table\" " \
                + "%s; done") % (
                        self._mysqlArgs['user'],
                        self._mysqlArgs['password'],
                        self._mysqlArgs['dbName'],
                        self._mysqlArgs['user'],
                        self._mysqlArgs['password'],
                        self._mysqlArgs['dbName'])

        rc = os.system(cmd)
        if rc != 0:
            raise TestEnvironmentError('Failed to reset database')


    def _getFixture(self):
        return {}


    _database = None
    _mysqlArgs = None
