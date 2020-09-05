## CHANGELOG

### 0.22.7 - 2020-09-05

 - Fix sorted set repository

### 0.22.6 - 2020-09-01

 - Improve Service.get_many performance

### 0.22.5 - 2020-09-01

 - Fix Service.get_many_cached

### 0.22.4 - 2020-08-28

 - Fix Service

### 0.22.3 - 2020-08-28

 - Fix repository timeout error catch

### 0.22.2 - 2020-08-28

 - Fix repository timeout error catch

### 0.22.1 - 2020-08-28

 - Fix query make_key method

### 0.22.0 - 2020-08-28

 - Fix service get_many ordering

 - Fix service cache when has composed ids

### 0.21.2 - 2020-08-27

 - Fix Repository.already_got_not_found timeout

### 0.21.1 - 2020-08-27

 - Add timeout for repository when getting data from data sources

### 0.21.0 - 2020-08-27

 - Refactor get_many to use async generator

### 0.20.0 - 2020-08-23

 - Improve SortedSet

### 0.19.1 - 2020-08-13

 - Improve georadius support

### 0.19.0 - 2020-08-12

 - Add fallback circuit breaker for service

### 0.18.0 - 2020-07-23

 - Add support for redis geospatial data type

 - Fix Hash services conditional imports

 - Fix Service class to be generic

### 0.17.2 - 2020-07-21

 - Fix service

### 0.17.1 - 2020-07-21

 - Fix service

### 0.16.0 - 2020-07-21

 - Improve Services

### 0.15.1 - 2020-07-09

 - Improve type hints

### 0.15.0 - 2020-06-25

 - Create exists method service and repository

### 0.14.0 - 2020-06-18

 - Create mongodb fallback data source

### 0.13.2 - 2020-06-16

 - Fix base service

### 0.13.1 - 2020-06-16

 - Fix service

### 0.13.0 - 2020-06-14

 - Improve delete method of base repository

### 0.12.0 - 2020-06-14

 - Improve service cache

### 0.11.0 - 2020-06-12

 - Improve boolean repository

### 0.10.1 - 2020-05-29

 - Improve DaoraCache

### 0.10.0 - 2020-05-27

 - Create BooleanReposity and service

### 0.9.6 - 2020-05-26

 - Remove experimental sync repository

### 0.9.5 - 2020-05-22

 - Adding sync repository. Experimental feature (WIP)

### 0.9.4 - 2020-05-15

 - Remove redis transaction from hash repository

 - Fix hash service add method

### 0.9.3 - 2020-05-15

 - Improve ttldaora cache

### 0.9.2 - 2020-05-13

 - Improve service add method

### 0.9.1 - 2020-05-12

 - Add ttl daora cache to service builder

### 0.9.0 - 2020-05-12

 - Create simple ttl cache class

### 0.8.0 - 2020-05-08

 - Improve add entity on repositories

### 0.7.1 - 2020-05-08

 - Fix requires

### 0.7.0 - 2020-05-07

 - Improve hash service cache

### 0.6.2 - 2020-05-05

 - Fix sorted set fallback key

 - Fix hash service cache

### 0.6.1 - 2020-05-04

 - Fix repository task_done_callback

### 0.6.0 - 2020-05-02

 - Simplify get_many

### 0.5.2 - 2020-04-30

 - Improve hash fallback data

### 0.5.1 - 2020-04-28

 - Fix get_many from fallback

### 0.5.0 - 2020-04-26

 - Improve repositories interface

 - Create tests for datastore repositories

### 0.4.1 - 2020-04-25

 - Improve datastore repositories

### 0.4.0 - 2020-04-25

 - Improve repositories interface

 - Add support for TypedDict

 - Improve sorted set entity

 - Improve coverage

### 0.3.0 - 2020-04-25

 - Create new tests and fix some bugs

 - Add subclass validation for MemoryRepository

### 0.2.1 - 2020-04-25

 - Fix DatastoreDataSource.make_key method

### 0.2.0 - 2020-04-24

 - Create dbdaora package
