# etl-wrapper

[![GitHub license](https://img.shields.io/badge/license-GPLv3-blue.svg)](https://raw.githubusercontent.com/NeoOrigin/etl-wrapper/master/LICENSE)
[![GitHub release](https://img.shields.io/github/release/NeoOrigin/etl-wrapper.svg)](https://github.com/NeoOrigin/etl-wrapper/releases/latest)

Provides a common generic interface to execute ETL jobs. 

ETL jobs regardless of the technology tend to share some common characteristics in their implementation. 

1 Predominantly they are batch orientated and run at set intervals. 

2 Frequently they are structured into a hierarchical project structure.

3 They will run on a host remote to the scheduling host

4 Jobs are created in the 1000's. To avoid migration issues the scheduler should be abstracted from implementation specifics

5 An ETL environment will often contain multiple tools / methods, either for cost or performance reasons. This significantly increases the learning and complexity of the environment.

## Examples

The following provide examples how to configure the etl wrapper application at runtime.

1 The following will override the ETL_DATE to be passed dynamically on the command line and BATCH_ID passed directly to the underlying plan as it is an unrecognised parameter definition

```shell
etlw --ETL_DATE "20101018" --BATCH_ID "4"
```

2 If ETL_DATE is a valid program argument you can pass the it directly to the underlying program using the following form (NOTE the wrapper will therefore not see a command line override but will receive its value from the next best alternative according to the overrides precedence.  This may later lead to confusion so it is advised that parameters are uniquely named.  ETL_BRANCH however, as it is a known parameter and because it occurs before the double dash will be processed by the wrapper

```shell
etlw --ETL_BRANCH "main" -- --ETL_DATE "20101018" --BATCH_ID "4"
```

3 The following runs the program with its default settings however the user has overridden the command to run as the etl_prod user, if this is not the current user the process will attempt to sudo before starting

```shell
etlw --ETL_USER "etl_prod"
```

4 The following runs the program with its default settings however the user has overridden the command to run as the etl_prod user on the etlx001 server, if this is not the current server then the process will ssh to the target (key based authentication) before executing

```shell
etlw --ETL_USER "etl_prod" --ETL_HOST "etlx001"
```

5 The following overrides the ETL_PROGRAM from its current definition to point at a hotfix program. This allows quick temporary changes to the underlying code via configuration without the requirement for involving third party teams in scheduling changes, waiting for code promotion and/or release cycle delays.  It can also be used in testing environments for overriding default inputs or implementing very simple bug fixes, ensuring testing progresses without having to formally wait a few hours or more for developers to fix and unit/regression test the code, regenerate sample data etc.

```shell
etlw --ETL_PROGRAM "fixes/pos_ftp_files.mp"
```

6 The following example dynamically determines information from a configuration file, note how this file is significantly simpler than implementing a full wrapper script per process and reduces chances for errors and improves code re-use.  Config files also have the potential benefit of being version controlled alongside your code, rather than in a bespoke scheduler database schema etc.  
        
Although passing parameters via ETL_PARAMS in a production environment is unusual it is however useful in development or test environments where access to IDE tools or locking of code for simple parameter changes during testing is discouraged.   

```shell     
etlw -f ./pos_ftp_files.params
```

```shell
# contents of ./pos_ftp_files.params
ETL_BASE_DIR="${HOME}/etl"
ETL_BRANCH="test_branch"
ETL_REL_PATH="MyCompany/POS"
ETL_PROJECT="pos_staging"
ETL_PROGRAM="mp/pos_ftp_files.mp"
ETL_PARAMS="-OUTPUT_DIRECTORY '/data/\$LOGICAL_RUN_DATE/my_file.dat'"
```

Because these files are no different than simple bash scripts care should be taken to escape any values that are set to ensure they are evaluated by the subsystem, not by the wrappers shell.
