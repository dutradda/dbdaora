# dbdaora

<p align="center" style="margin: 3em">
  <a href="https://github.com/dutradda/dbdaora">
    <img src="https://dutradda.github.io/dbdaora/dbdaora-white.svg" alt="dbdaora" width="300"/>
  </a>
</p>

<p align="center">
    <em>Communicates with <b>NoSQL</b> (and <b>SQL</b> for future) databases using repository and service patterns and python dataclasses</em>
</p>

---

**Documentation**: <a href="https://dutradda.github.io/dbdaora/" target="_blank">https://dutradda.github.io/dbdaora/</a>

**Source Code**: <a href="https://github.com/dutradda/dbdaora" target="_blank">https://github.com/dutradda/dbdaora</a>

---


## Key Features

- **Creates an optional service layer with cache and circuit breaker**

- **Supports for redis data types:**
    + Hash
    + Sorted Set
    + *(Others data types are planned)*

- **Backup redis data into other databases:**
    + Google Datastore
    + Mongodb *(soon)*
    + SQL databases with SQLAlchemy *(soon)*
    + *(Others data bases are planned)*

- *Support for other databases are in development.*


## Requirements

 - Python 3.8+
 - [jsondaora](https://github.com/dutradda/jsondaora) for data validation/parsing
 - circuitbreaker
 - cachetools

 - Optionals:
    + aioredis
    + google-cloud-datastore


## Instalation

```
$ pip install dbdaora
```


## Simple redis hash example

```python
{!./src/index/simple_hash.py!}
```


## Simple redis sorted set example

```python
{!./src/index/simple_sorted_set.py!}
```


## Using the service layer

The service layer uses the backup dataset when redis is offline, opening a circuit breaker.

It has an optional cache system too.


```python
{!./src/index/simple_service.py!}
```


## Simple Domain Model Example


```python
{!./src/index/simple_domain_model.py!}
```
